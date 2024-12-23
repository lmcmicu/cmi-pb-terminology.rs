//! Low-level validation functions

use crate::{
    ast::Expression,
    toolkit::{
        cast_sql_param_from_text, get_column_value, get_column_value_as_string,
        get_datatype_ancestors, get_query_param, get_sql_type_from_global_config,
        get_table_options_from_config, get_value_type, is_sql_type_error, local_sql_syntax,
        ColumnRule, CompiledCondition, DbKind, QueryAsIf, QueryAsIfKind, QueryParam, ValueType,
    },
    valve::{
        ValveCell, ValveCellMessage, ValveConfig, ValveRow, ValveRuleConfig, ValveTreeConstraint,
    },
    DT_CACHE_SIZE, SQL_PARAM,
};
use anyhow::Result;
use indexmap::IndexMap;
use lfu_cache::LfuCache;
use serde_json::{json, Value as SerdeValue};
use sqlx::{any::AnyPool, query as sqlx_query, Acquire, Row, Transaction, ValueRef};
use std::collections::HashMap;

/// Given a config struct, maps of compiled datatype and rule conditions, a database connection
/// pool, a table name, a row to validate represented as a [ValveRow], and a row number in the case
/// where the row already exists, perform both intra- and inter-row validation and return the
/// validated row. Optionally, if a transaction is given, use that instead of the pool for database
/// access. Optionally, if query_as_if is given, validate the row counterfactually according to that
/// parameter. Note that this function is idempotent.
pub async fn validate_row_tx(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &str,
    row: &ValveRow,
    query_as_if: Option<&QueryAsIf>,
) -> Result<ValveRow> {
    // Fallback to a default transaction if it is not given. Since we do not commit before it falls
    // out of scope the transaction will be rolled back at the end of this function. And since this
    // function is read-only the rollback is trivial and therefore inconsequential.
    let default_tx = &mut pool.begin().await?;
    let tx = match tx {
        Some(tx) => tx,
        None => default_tx,
    };

    // Store the row number in a separate local variable for convenience:
    let row_number = row.row_number;

    // Initialize the result row with the values from the given row:
    let mut valve_row = row.clone();

    // We check all the cells for nulltype first, since the rules validation requires that we
    // have this information for all cells.
    for (column_name, cell) in valve_row.contents.iter_mut() {
        validate_cell_nulltype(
            config,
            datatype_conditions,
            &table_name.to_string(),
            column_name,
            cell,
        );
    }

    let context = valve_row.clone();
    for (column_name, cell) in valve_row.contents.iter_mut() {
        validate_cell_rules(
            config,
            rule_conditions,
            &table_name.to_string(),
            column_name,
            &context,
            cell,
        );

        if cell.nulltype == None {
            validate_cell_datatype(
                config,
                datatype_conditions,
                &table_name.to_string(),
                column_name,
                cell,
            );

            // We don't do any further validation on cells that have SQL type violations because
            // they can result in database errors when, for instance, we compare a numeric with a
            // non-numeric type.
            let sql_type = get_sql_type_from_global_config(
                &config,
                table_name,
                &column_name,
                &DbKind::from_pool(pool)?,
            );
            if !is_sql_type_error(&sql_type, &cell.strvalue()) {
                validate_cell_foreign_constraints(
                    config,
                    pool,
                    Some(tx),
                    datatype_conditions,
                    &table_name.to_string(),
                    column_name,
                    cell,
                    query_as_if,
                    &None,
                )
                .await?;
                validate_cell_unique_constraints(
                    config,
                    pool,
                    Some(tx),
                    &table_name.to_string(),
                    column_name,
                    cell,
                    &vec![],
                    row_number,
                    &None,
                )
                .await?;
            }
        }
    }

    // TODO: Possibly propagate `query_as_if` down into this function:
    let mut violations = validate_tree_foreign_keys(
        config,
        pool,
        Some(tx),
        &table_name.to_string(),
        Some(&context.clone()),
    )
    .await?;
    for violation in violations.iter_mut() {
        let vrow_number = violation.get("row_number").unwrap().as_i64().unwrap() as u32;
        if Some(vrow_number) == row_number || (row_number == None && Some(vrow_number) == Some(0)) {
            let column = violation.get("column").and_then(|s| s.as_str()).unwrap();
            let level = violation.get("level").and_then(|s| s.as_str()).unwrap();
            let rule = violation.get("rule").and_then(|s| s.as_str()).unwrap();
            let message = violation.get("message").and_then(|s| s.as_str()).unwrap();
            let result_cell = &mut valve_row.contents.get_mut(column).unwrap();
            result_cell.messages.push(ValveCellMessage {
                level: level.to_string(),
                rule: rule.to_string(),
                message: message.to_string(),
            });
            if result_cell.valid {
                result_cell.valid = false;
            }
        }
    }

    remove_duplicate_messages(&mut valve_row)?;
    Ok(valve_row)
}

/// Given a config map, a db connection pool, and a table name, validate whether there is a
/// 'foreign key' violation for any of the table's trees; i.e., for a given tree: tree(child) which
/// has a given parent column, validate that all of the values in the parent column are in the child
/// column. Optionally, if a transaction is given, use that instead of the pool for database access.
pub async fn validate_tree_foreign_keys(
    config: &ValveConfig,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &String,
    extra_row: Option<&ValveRow>,
) -> Result<Vec<SerdeValue>> {
    let tkeys = config
        .constraint
        .tree
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name));

    let table_options = get_table_options_from_config(config, table_name)?;
    let query_table = {
        if !table_options.contains("conflict") {
            table_name.to_string()
        } else {
            format!("{}_view", table_name)
        }
    };
    let mut results = vec![];
    for tkey in tkeys {
        let child_col = &tkey.child;
        let parent_col = &tkey.parent;
        let parent_sql_type = get_sql_type_from_global_config(
            &config,
            &table_name,
            &parent_col,
            &DbKind::from_pool(pool)?,
        );
        let with_clause;
        let params;
        if let Some(ref extra_row) = extra_row {
            (with_clause, params) = select_with_extra_row(
                &config,
                extra_row,
                table_name,
                &query_table,
                &DbKind::from_pool(pool)?,
            );
        } else {
            with_clause = String::new();
            params = vec![];
        }

        let effective_table_name;
        if !with_clause.is_empty() {
            effective_table_name = format!("{}_ext", query_table);
        } else {
            effective_table_name = query_table.clone();
        }

        let sql = local_sql_syntax(
            &DbKind::from_pool(pool)?,
            &format!(
                r#"{with_clause}
                   SELECT
                     t1."row_number", t1."{parent_col}"
                   FROM "{effective_table_name}" t1
                   WHERE NOT EXISTS (
                     SELECT 1
                     FROM "{effective_table_name}" t2
                     WHERE t2."{child_col}" = t1."{parent_col}"
                   )"#,
                with_clause = with_clause,
                parent_col = parent_col,
                effective_table_name = effective_table_name,
                child_col = child_col,
            ),
        );

        let mut query = sqlx_query(&sql);
        for param in &params {
            query = query.bind(param);
        }
        let rows = {
            if let None = tx {
                query.fetch_all(pool).await?
            } else {
                query
                    .fetch_all(tx.as_mut().unwrap().acquire().await?)
                    .await?
            }
        };
        for row in rows {
            let raw_row_number = row.try_get_raw("row_number").unwrap();
            let row_number: i64;
            if raw_row_number.is_null() {
                row_number = 0;
            } else {
                row_number = row.get("row_number");
            }
            let raw_parent_val = row
                .try_get_raw(format!(r#"{}"#, parent_col).as_str())
                .unwrap();
            if !raw_parent_val.is_null() {
                let parent_val = get_column_value_as_string(&row, &parent_col, &parent_sql_type);
                let message = json!({
                    "row_number": row_number as u32,
                    "column": parent_col,
                    "value": parent_val,
                    "level": "error",
                    "rule": "tree:foreign",
                    "message": format!("Value '{}' of column {} is not in column {}",
                                       parent_val, parent_col, child_col).as_str(),
                });
                results.push(message);
            }
        }
    }

    Ok(results)
}

/// Given a config map, a database connection pool, a hashmap describing datatype conditions, a
/// table name, and a number of rows to validate, validate foreign and unique constraints, where
/// the latter include unique and primary constraints and modify the given rows with the validation
/// results.
pub async fn validate_rows_constraints(
    config: &ValveConfig,
    pool: &AnyPool,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    table: &String,
    rows: &mut Vec<ValveRow>,
) -> Result<()> {
    // Given a config map, a pool, and a table and column name, then if the column is not
    // constrained by a foreign constraint, return None. Otherwise if there is a constraint on,
    // say, the column "fcolumn" of the table "ftable", then return two vectors of SerdeValues,
    // where the first contains the contents of ftable.fcolumn, and the second contains the contents
    // of ftable_conflict.fcolumn, or is empty if no conflict table exists.
    async fn get_allowed_values(
        config: &ValveConfig,
        pool: &AnyPool,
        table: &str,
        column: &str,
        received_values: &HashMap<&str, Vec<SerdeValue>>,
    ) -> Result<Option<(Vec<SerdeValue>, Vec<SerdeValue>)>> {
        let fconstraint = {
            let mut fconstraints = config
                .constraint
                .foreign
                .get(table)
                .expect(&format!(
                    "Could not retrieve foreign constraints for table '{}'",
                    table
                ))
                .iter()
                .filter(|fkey| fkey.column == column)
                .collect::<Vec<_>>();
            if fconstraints.len() > 1 {
                log::warn!(
                    "There is more than one foreign constraint defined for '{}.{}'.",
                    table,
                    column
                );
            }
            // Take the last one:
            fconstraints.pop()
        };
        match fconstraint {
            None => Ok(None),
            Some(fkey) => {
                let sql_type = get_sql_type_from_global_config(
                    config,
                    &fkey.ftable,
                    &fkey.fcolumn,
                    &DbKind::from_pool(pool)?,
                )
                .to_lowercase();
                let values = received_values
                    .get(&*fkey.column)
                    .unwrap()
                    .iter()
                    .filter(|value| {
                        !is_sql_type_error(
                            &sql_type,
                            value
                                .as_str()
                                .expect(&format!("'{}' is not a string", value)),
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let lookup_sql = values
                    .iter()
                    .map(|_| SQL_PARAM.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let param_values = values
                    .iter()
                    .map(|value| get_query_param(value, &sql_type))
                    .collect::<Vec<_>>();

                // Foreign keys always correspond to columns with unique constraints so we do not
                // need to use the keyword 'DISTINCT' when querying the normal version of the table:
                let sql = local_sql_syntax(
                    &DbKind::from_pool(pool)?,
                    &format!(
                        r#"SELECT "{}" FROM "{}" WHERE "{}" IN ({})"#,
                        fkey.fcolumn, fkey.ftable, fkey.fcolumn, lookup_sql
                    ),
                );
                let mut query = sqlx_query(&sql);
                for param_value in &param_values {
                    match param_value {
                        QueryParam::Integer(p) => query = query.bind(p),
                        QueryParam::Numeric(p) => query = query.bind(p),
                        QueryParam::Real(p) => query = query.bind(p),
                        QueryParam::String(p) => query = query.bind(p),
                    }
                }

                let allowed_values = query
                    .fetch_all(pool)
                    .await?
                    .iter()
                    .map(|row| get_column_value(row, &fkey.fcolumn, &sql_type))
                    .collect::<Vec<_>>();

                let allowed_values_conflict = {
                    let foptions = &config
                        .table
                        .get(&fkey.ftable)
                        .expect(&format!("No config for table: '{}'", fkey.ftable))
                        .options;
                    if foptions.contains("conflict") {
                        // The conflict table has no keys other than on row_number so in principle
                        // it could have duplicate values of the foreign constraint, therefore we
                        // add the DISTINCT keyword here:
                        let sql = local_sql_syntax(
                            &DbKind::from_pool(pool)?,
                            &format!(
                                r#"SELECT DISTINCT "{}" FROM "{}_conflict" WHERE "{}" IN ({})"#,
                                fkey.fcolumn, fkey.ftable, fkey.fcolumn, lookup_sql
                            ),
                        );
                        let mut query = sqlx_query(&sql);
                        for param_value in &param_values {
                            match param_value {
                                QueryParam::Integer(p) => query = query.bind(p),
                                QueryParam::Numeric(p) => query = query.bind(p),
                                QueryParam::Real(p) => query = query.bind(p),
                                QueryParam::String(p) => query = query.bind(p),
                            }
                        }
                        query
                            .fetch_all(pool)
                            .await?
                            .iter()
                            .map(|row| get_column_value(row, &fkey.fcolumn, &sql_type))
                            .collect::<Vec<_>>()
                    } else {
                        // If there is no conflict table just return an empty vector.
                        vec![]
                    }
                };

                Ok(Some((allowed_values, allowed_values_conflict)))
            }
        }
    }

    // Given a config map, a pool, and a table and column name, then if the column is not
    // constrained by a unique or primary constraint, return None. Otherwise
    // return a vector of SerdeValues containing the (distinct) values of the column in question,
    // regardless of their validity (except when the invalidity would result in a SQL error).
    async fn get_forbidden_values(
        config: &ValveConfig,
        pool: &AnyPool,
        table: &str,
        column: &str,
        received_values: &HashMap<&str, Vec<SerdeValue>>,
    ) -> Result<Option<Vec<SerdeValue>>> {
        let primaries = &config
            .constraint
            .primary
            .get(table)
            .expect(&format!("Undefined table '{}'", table));
        let uniques = &config
            .constraint
            .unique
            .get(table)
            .expect(&format!("Undefined table '{}'", table));

        if primaries.iter().any(|c| c == column) || uniques.iter().any(|c| c == column) {
            let options = &config
                .table
                .get(table)
                .expect(&format!("No config for table: '{}'", table))
                .options;

            let (query_table, query_modifier) = {
                if !options.contains("conflict") {
                    (table.to_string(), String::new())
                } else {
                    (format!("{}_view", table), "DISTINCT".to_string())
                }
            };

            let sql_type =
                get_sql_type_from_global_config(config, &table, &column, &DbKind::from_pool(pool)?)
                    .to_lowercase();
            let values = received_values
                .get(&*column)
                .unwrap()
                .iter()
                .filter(|value| {
                    !is_sql_type_error(
                        &sql_type,
                        value
                            .as_str()
                            .expect(&format!("'{}' is not a string", value)),
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            let lookup_sql = values
                .iter()
                .map(|_| SQL_PARAM.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let param_values = values
                .iter()
                .map(|value| get_query_param(value, &sql_type))
                .collect::<Vec<_>>();

            let sql = local_sql_syntax(
                &DbKind::from_pool(pool)?,
                &format!(
                    r#"SELECT {} "{}" FROM "{}" WHERE "{}" IN ({})"#,
                    query_modifier, column, query_table, column, lookup_sql
                ),
            );
            let mut query = sqlx_query(&sql);
            for param_value in &param_values {
                match param_value {
                    QueryParam::Integer(p) => query = query.bind(p),
                    QueryParam::Numeric(p) => query = query.bind(p),
                    QueryParam::Real(p) => query = query.bind(p),
                    QueryParam::String(p) => query = query.bind(p),
                }
            }

            let forbidden_values = query
                .fetch_all(pool)
                .await?
                .iter()
                .map(|row| get_column_value(row, &column, &sql_type))
                .collect::<Vec<_>>();
            Ok(Some(forbidden_values))
        } else {
            Ok(None)
        }
    }

    // Begin by getting the table config for this table:
    let table_config = &config
        .table
        .get(table)
        .expect(&format!("Undefined table '{}'", table));

    // Declare a closure for constructing a HashMap from column names (looked up in table_config) to
    // vectors of the values of those columns in the row. The closure accepts an argument, split,
    // which, if set, indicates that the value of a column which has the special list() datatype
    // is to be interpreted, not as itself a value, but as a list of individual values.
    let mut get_values_by_column_from_rows = |split: bool| -> HashMap<&str, Vec<SerdeValue>> {
        let mut received_values = table_config
            .column_order
            .iter()
            .map(|column| (column.as_str(), vec![]))
            .collect::<HashMap<_, _>>();
        for row in rows.iter_mut() {
            for (column, values) in &mut received_values {
                let cell = row
                    .contents
                    .get(&column.to_string())
                    .expect(&format!("No column '{}' in row", column));
                if !split {
                    values.push(cell.value.clone());
                } else {
                    let value_list = {
                        let value_type = get_value_type(config, datatype_conditions, table, column);
                        match &value_type {
                            ValueType::Single => vec![cell.value.clone()],
                            ValueType::List(_, separator) => cell
                                .strvalue()
                                .split(separator)
                                .map(|s| json!(s))
                                .collect::<Vec<_>>(),
                        }
                    };
                    for value in &value_list {
                        values.push(value.clone());
                    }
                }
            }
        }
        received_values
    };

    let received_values_split = get_values_by_column_from_rows(true);
    let received_values_unsplit = get_values_by_column_from_rows(false);
    // Prefetch the values that are allowed and/or forbidden for this particular
    // column given the current state of the given table:
    let allowed_values = {
        let mut allowed_values = HashMap::new();
        for column in &table_config.column_order {
            allowed_values.insert(
                column.to_string(),
                get_allowed_values(config, pool, table, column, &received_values_split).await?,
            );
        }
        allowed_values
    };
    let forbidden_values = {
        let mut forbidden_values = HashMap::new();
        for column in &table_config.column_order {
            forbidden_values.insert(
                column.to_string(),
                get_forbidden_values(config, pool, table, column, &received_values_unsplit).await?,
            );
        }
        forbidden_values
    };

    let mut valve_rows = vec![];
    for row in rows.iter_mut() {
        let mut valve_row = ValveRow {
            row_number: None,
            contents: IndexMap::new(),
        };
        for column in &table_config.column_order {
            let cell = row.contents.get_mut(column).unwrap();
            // We don't do any further validation on cells that are legitimately empty, or on cells
            // that have SQL type violations. We exclude the latter because they can result in
            // database errors when, for instance, we compare a numeric with a non-numeric type.
            let sql_type =
                get_sql_type_from_global_config(config, table, &column, &DbKind::from_pool(pool)?);
            if cell.nulltype == None && !is_sql_type_error(&sql_type, &cell.strvalue()) {
                let (allowed_values, forbidden_values) = {
                    let error_msg = format!("Could not retrieve values for column '{}'", column);
                    let allowed_values = allowed_values.get(column).expect(&error_msg);
                    let forbidden_values = forbidden_values.get(column).expect(&error_msg);
                    (allowed_values, forbidden_values)
                };
                validate_cell_foreign_constraints(
                    config,
                    pool,
                    None,
                    datatype_conditions,
                    table,
                    &column,
                    cell,
                    None,
                    &allowed_values,
                )
                .await
                .expect(&format!(
                    "Unable to validate foreign constraints for row number: {:?}, column '{}.{}'",
                    row.row_number, table, column
                ));
                validate_cell_unique_constraints(
                    config,
                    pool,
                    None,
                    table,
                    &column,
                    cell,
                    &valve_rows,
                    None,
                    &forbidden_values,
                )
                .await
                .expect(&format!(
                    "Unable to validate unique constraints for row number: {:?}, column '{}.{}'",
                    row.row_number, table, column
                ));
            }
            valve_row.contents.insert(column.to_string(), cell.clone());
        }
        // Note that in this implementation, the result rows are never actually returned, but we
        // still need them because the validate_cell_unique_constraints() function needs a list of
        // previous results, and this then requires that we generate the result rows to play that
        // role. The call to cell.clone() above is required to make rust's borrow checker happy.
        valve_rows.push(valve_row);
    }

    Ok(())
}

/// Given a config map, compiled datatype and rule conditions, a table name, the headers for the
/// table, and a number of rows to validate, run intra-row validatation on all of the rows and
/// return the validated versions.
pub fn validate_rows_intra(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    headers: &Vec<String>,
    rows: &Vec<Result<csv::StringRecord, csv::Error>>,
    only_nulltype: bool,
) -> Vec<ValveRow> {
    let mut dt_cache = match DT_CACHE_SIZE {
        0 => None,
        _ => Some(HashMap::new()),
    };
    let mut valve_rows = vec![];
    for row in rows {
        match row {
            Err(err) => log::error!(
                "While processing row for '{}', got error '{}'",
                table_name,
                err
            ),
            Ok(row) => {
                let mut valve_row = ValveRow {
                    row_number: None,
                    contents: IndexMap::new(),
                };
                for (i, value) in row.iter().enumerate() {
                    let result_cell = ValveCell {
                        nulltype: None,
                        value: json!(value),
                        valid: true,
                        messages: vec![],
                    };
                    let column = headers.get(i).unwrap();
                    valve_row.contents.insert(column.to_string(), result_cell);
                }

                let column_names = &config
                    .table
                    .get(table_name)
                    .expect(&format!("Undefined table '{}'", table_name))
                    .column_order;

                // If a table has been configured with more columns than are actually in its
                // associated .tsv file, then when we try to get a value for one of the extra
                // columns from the row, it will not be found. Instead of panicking we will
                // use unwrap_or() with an empty ValveCell as the default:
                let mut default_cell = ValveCell {
                    ..Default::default()
                };
                // We begin by determining the nulltype of all of the cells, since the rules
                // validation step requires that all cells have this information.
                for column_name in column_names {
                    let cell = valve_row
                        .contents
                        .get_mut(column_name)
                        .unwrap_or(&mut default_cell);
                    validate_cell_nulltype(
                        config,
                        datatype_conditions,
                        table_name,
                        &column_name,
                        cell,
                    );
                }

                if !only_nulltype {
                    for column_name in column_names {
                        let context = valve_row.clone();
                        let cell = valve_row
                            .contents
                            .get_mut(column_name)
                            .unwrap_or(&mut default_cell);
                        validate_cell_rules(
                            config,
                            rule_conditions,
                            table_name,
                            &column_name,
                            &context,
                            cell,
                        );

                        if cell.nulltype != None {
                            continue;
                        }

                        if let Some(dt_cache) = &mut dt_cache {
                            // Add a new map for the column to the dt_cache if one doesn't exist:
                            if !dt_cache.contains_key(column_name) {
                                dt_cache.insert(
                                    column_name.to_string(),
                                    LfuCache::with_capacity(DT_CACHE_SIZE),
                                );
                            }
                            let dt_col_cache = dt_cache.get_mut(column_name).unwrap();
                            let string_value = cell.value.to_string();

                            // We do not want to add cells to the datatype validation cache if they
                            // already contain other types of violations, since that will make it
                            // more difficult to construct a unique mapping from values to result
                            // cells; i.e., the same value could in principle map to either a valid
                            // or an invalid cell and this will need to be considered. Ignoring
                            // initially invalid cells (which will be comparitively fewer in number)
                            // thus simplifies the process.
                            if cell.valid {
                                let cached_cell = dt_col_cache.get_mut(&string_value);
                                match cached_cell {
                                    None => {
                                        validate_cell_datatype(
                                            config,
                                            datatype_conditions,
                                            table_name,
                                            &column_name,
                                            cell,
                                        );
                                        dt_col_cache.insert(string_value, cell.clone());
                                    }
                                    Some(cached_cell) => {
                                        cell.nulltype = cached_cell.nulltype.clone();
                                        cell.value = cached_cell.value.clone();
                                        cell.valid = cached_cell.valid;
                                        cell.messages = cached_cell.messages.clone();
                                    }
                                };
                            } else {
                                validate_cell_datatype(
                                    config,
                                    datatype_conditions,
                                    table_name,
                                    &column_name,
                                    cell,
                                );
                            }
                        } else {
                            validate_cell_datatype(
                                config,
                                datatype_conditions,
                                table_name,
                                &column_name,
                                cell,
                            );
                        }
                    }
                }
                valve_rows.push(valve_row);
            }
        };
    }

    // Finally return the result rows:
    valve_rows
}

/// Given a row represented as a [ValveRow], remove any duplicate messages from the row's cells, so
/// that no cell has messages with the same level, rule, and message text.
fn remove_duplicate_messages(row: &mut ValveRow) -> Result<()> {
    for (_column_name, cell) in row.contents.iter_mut() {
        let mut messages = cell.messages.clone();
        messages.sort_by(|a, b| {
            let a = format!("{}{}{}", a.level, a.rule, a.message);
            let b = format!("{}{}{}", b.level, b.rule, b.message,);
            a.partial_cmp(&b).unwrap()
        });
        messages.dedup_by(|a, b| a.level == b.level && a.rule == b.rule && a.message == b.message);
        cell.messages = messages;
    }
    Ok(())
}

/// Generate a SQL Select clause that is a union of: (a) the literal values of the given extra row,
/// and (b) a Select statement over `table_name` of all the fields in the extra row.
pub fn select_with_extra_row(
    config: &ValveConfig,
    extra_row: &ValveRow,
    table: &str,
    effective_table: &str,
    kind: &DbKind,
) -> (String, Vec<String>) {
    let extra_row_len = extra_row.contents.keys().len();
    let mut params = vec![];
    let mut first_select;
    match extra_row.row_number {
        Some(rn) => first_select = format!(r#"SELECT {} AS "row_number", "#, rn),
        _ => first_select = String::from(r#"SELECT NULL AS "row_number", "#),
    };

    let mut second_select = String::from(r#"SELECT "row_number", "#);
    for (i, (key, content)) in extra_row.contents.iter().enumerate() {
        let sql_type = get_sql_type_from_global_config(config, &table, &key, kind);
        let sql_param = cast_sql_param_from_text(&sql_type);
        // enumerate() begins from 0 but we need to begin at 1:
        let i = i + 1;
        first_select.push_str(format!(r#"{} AS "{}""#, sql_param, key).as_str());
        params.push(content.strvalue());
        second_select.push_str(format!(r#""{}""#, key).as_str());
        if i < extra_row_len {
            first_select.push_str(", ");
            second_select.push_str(", ");
        } else {
            second_select.push_str(format!(r#" FROM "{}""#, effective_table).as_str());
        }
    }

    if let Some(rn) = extra_row.row_number {
        second_select.push_str(format!(r#" WHERE "row_number" <> {}"#, rn).as_str());
    }

    (
        format!(
            r#"WITH "{}_ext" AS ({} UNION ALL {})"#,
            effective_table, first_select, second_select
        ),
        params,
    )
}

/// Given a map representing a tree constraint, a table name, and an extra SQL clause, generate the
/// SQL for a WITH clause representing tree.
pub fn with_tree_sql(
    tree: &ValveTreeConstraint,
    effective_table_name: &str,
    extra_clause: Option<&String>,
) -> String {
    let empty_string = String::new();
    let extra_clause = extra_clause.unwrap_or_else(|| &empty_string);
    let child_col = &tree.child;
    let parent_col = &tree.parent;
    format!(
        r#"WITH RECURSIVE "tree" AS (
           {extra_clause}
               SELECT "{child_col}", "{parent_col}" 
                   FROM "{effective_table_name}" 
                   UNION ALL 
               SELECT "t1"."{child_col}", "t1"."{parent_col}" 
                   FROM "{effective_table_name}" AS "t1" 
                   JOIN "tree" AS "t2" ON "t2"."{parent_col}" = "t1"."{child_col}"
           )"#,
        extra_clause = extra_clause,
        child_col = child_col,
        parent_col = parent_col,
        effective_table_name = effective_table_name,
    )
}

/// Given a config map, compiled datatype conditions, a table name, a column name, and a cell to
/// validate, validate the cell's nulltype condition. If the cell's value is one of the allowable
/// nulltype values for this column, then fill in the cell's nulltype value before returning the
/// cell.
pub fn validate_cell_nulltype(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &String,
    column_name: &String,
    cell: &mut ValveCell,
) {
    let column = config
        .table
        .get(table_name)
        .and_then(|t| t.column.get(column_name))
        .expect(&format!(
            "Undefined column '{}.{}'",
            table_name, column_name
        ));

    if column.nulltype != "" {
        let nt_name = &column.nulltype;
        let nt_condition = &datatype_conditions.get(nt_name).unwrap().compiled;
        let value = &cell.strvalue();
        if nt_condition(&value) {
            cell.nulltype = Some(nt_name.to_string());
        }
    }
}

/// Given a config map, compiled datatype conditions, a table name, a column name, and a cell to
/// validate, validate the cell's datatype and return the validated cell.
pub fn validate_cell_datatype(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &String,
    column_name: &String,
    cell: &mut ValveCell,
) {
    fn construct_message(
        value: &str,
        condition: &CompiledCondition,
        column: &str,
        dt_name: &str,
        dt_description: &str,
    ) -> String {
        match &condition.value_type {
            ValueType::List(_, _) if dt_description == "" => {
                let concrete_dt = match &condition.parsed {
                    Expression::Function(name, args) if name == "list" => match &*args[0] {
                        Expression::Label(datatype) => datatype,
                        _ => unreachable!(
                            "ValueType::List for a function with an invalid datatype argument"
                        ),
                    },
                    _ => unreachable!("ValueType::List for a non-function condition"),
                };
                format!(
                    "Value '{}' of column {} should be one of a list of tokens whose datatype is {}",
                    value, column, concrete_dt
                )
            }
            ValueType::List(_, _) => {
                format!(
                    "Value '{}' of column {} should be one of {}",
                    value, column, dt_description
                )
            }
            ValueType::Single if dt_description == "" => {
                format!("{} should be of datatype {}", column, dt_name)
            }
            ValueType::Single => {
                format!("{} should be {}", column, dt_description)
            }
        }
    }

    let column = config
        .table
        .get(table_name)
        .and_then(|t| t.column.get(column_name))
        .expect(&format!(
            "Undefined column '{}.{}'",
            table_name, column_name
        ));
    let primary_dt_name = &column.datatype;
    let primary_dt = &config
        .datatype
        .get(primary_dt_name)
        .expect(&format!("Undefined datatype '{}'", primary_dt_name));
    let primary_dt_desc = &primary_dt.description;
    if let Some(primary_dt_cond) = datatype_conditions.get(primary_dt_name) {
        let strvalue = cell.strvalue();
        let values = match &primary_dt_cond.value_type {
            ValueType::Single => vec![strvalue.as_str()],
            ValueType::List(_, separator) => strvalue.split(separator).collect::<Vec<_>>(),
        };
        for value in &values {
            if (primary_dt_cond.compiled)(&value) {
                continue;
            }
            cell.valid = false;
            let mut datatypes_to_check =
                get_datatype_ancestors(config, datatype_conditions, primary_dt_name, true);
            // If this datatype has any parents, check them beginning from the most general to
            // the most specific. We use while and pop instead of a for loop so as to check the
            // conditions in LIFO order.
            while !datatypes_to_check.is_empty() {
                let datatype = datatypes_to_check.pop().unwrap();
                let dt_name = &datatype.datatype;
                let dt_description = &datatype.description;
                let dt_condition = &datatype_conditions.get(dt_name).unwrap().compiled;
                if !dt_condition(&value) {
                    let message = construct_message(
                        &value,
                        primary_dt_cond,
                        column_name,
                        dt_name,
                        dt_description,
                    );
                    let message_info = ValveCellMessage {
                        rule: format!("datatype:{}", dt_name),
                        level: "error".to_string(),
                        message: message,
                    };
                    cell.messages.push(message_info);
                }
            }

            let message = construct_message(
                &value,
                primary_dt_cond,
                column_name,
                primary_dt_name,
                primary_dt_desc,
            );
            let message_info = ValveCellMessage {
                rule: format!("datatype:{}", primary_dt_name),
                level: "error".to_string(),
                message: message,
            };
            cell.messages.push(message_info);
        }
    }
}

/// Given a config map, compiled rule conditions, a table name, a column name, the row context,
/// and the cell to validate, look in the rule table (if it exists) and validate the cell according
/// to any applicable rules.
pub fn validate_cell_rules(
    config: &ValveConfig,
    rules: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    column_name: &String,
    context: &ValveRow,
    cell: &mut ValveCell,
) {
    fn check_condition(
        condition_type: &str,
        cell: &ValveCell,
        rule: &ValveRuleConfig,
        table_name: &String,
        column_name: &String,
        rules: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    ) -> bool {
        let condition = {
            if condition_type == "when" {
                rule.when_condition.as_str()
            } else if condition_type == "then" {
                rule.then_condition.as_str()
            } else {
                panic!("Invalid condition type: {}", condition_type);
            }
        };

        if vec!["null", "not null"].contains(&condition) {
            return (condition == "null" && cell.nulltype != None)
                || (condition == "not null" && cell.nulltype == None);
        } else {
            let compiled_condition = rules
                .get(table_name)
                .and_then(|t| t.get(column_name))
                .and_then(|v| {
                    v.iter().find(|c| {
                        if condition_type == "when" {
                            c.when.original == condition
                        } else {
                            c.then.original == condition
                        }
                    })
                })
                .and_then(|c| {
                    if condition_type == "when" {
                        Some(c.when.compiled.clone())
                    } else {
                        Some(c.then.compiled.clone())
                    }
                })
                .unwrap();
            return compiled_condition(&cell.strvalue());
        }
    }

    let applicable_rules = match config.rule.get(table_name).and_then(|t| t.get(column_name)) {
        Some(rules) => rules,
        _ => return,
    };

    for (rule_number, rule) in applicable_rules.iter().enumerate() {
        // enumerate() begins at 0 by default but we need to begin with 1:
        let rule_number = rule_number + 1;
        // Check the then condition only if the when condition is satisfied:
        if check_condition("when", cell, rule, table_name, column_name, rules) {
            let then_cell = context.contents.get(&rule.then_column).unwrap();
            if !check_condition("then", then_cell, rule, table_name, column_name, rules) {
                cell.valid = false;
                cell.messages.push(ValveCellMessage {
                    rule: format!("rule:{}-{}", column_name, rule_number),
                    level: rule.level.to_string(),
                    message: rule.description.to_string(),
                });
            }
        }
    }
}

/// Generates an SQL fragment representing the "as if" portion of a query that will be used for
/// counterfactual validation.
pub fn as_if_to_sql(
    config: &ValveConfig,
    kind: &DbKind,
    as_if: &QueryAsIf,
    conflict_table: bool,
) -> String {
    let sql = {
        let suffix = {
            if conflict_table {
                "_conflict"
            } else {
                ""
            }
        };

        match as_if.kind {
            QueryAsIfKind::Remove => {
                format!(
                    r#""{table_alias}{suffix}" AS (
                       SELECT * FROM "{table_name}{suffix}" WHERE "row_number" <> {row_number}
                    )"#,
                    table_alias = as_if.alias,
                    suffix = suffix,
                    table_name = as_if.table,
                    row_number = as_if.row_number,
                )
            }
            QueryAsIfKind::Add | QueryAsIfKind::Replace => {
                let row = as_if.row.as_ref().unwrap();
                let columns = row.contents.keys().cloned().collect::<Vec<_>>();
                let values = {
                    let mut values = vec![];
                    for column in &columns {
                        let valid = row
                            .contents
                            .get(column)
                            .and_then(|c| Some(c.valid))
                            .unwrap();

                        let value = {
                            if valid == true {
                                let value = row
                                    .contents
                                    .get(column)
                                    .and_then(|c| Some(c.strvalue()))
                                    .unwrap();
                                if value == "" {
                                    "NULL".to_string()
                                } else {
                                    value
                                }
                            } else {
                                "NULL".to_string()
                            }
                        };

                        let sql_type =
                            get_sql_type_from_global_config(&config, &as_if.table, &column, kind);

                        if sql_type.to_lowercase() == "text"
                            || sql_type.to_lowercase().starts_with("varchar(")
                        {
                            values.push(format!("'{}'", value));
                        } else {
                            values.push(value.to_string());
                        }
                    }
                    values.join(", ")
                };
                let columns = columns
                    .iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let where_clause = {
                    if as_if.kind == QueryAsIfKind::Replace {
                        format!(r#"WHERE "row_number" != {}"#, as_if.row_number)
                    } else {
                        "".to_string()
                    }
                };
                format!(
                    r#""{table_alias}{suffix}" AS (
                   SELECT "row_number", {columns}
                   FROM "{table_name}{suffix}"
                   {where_clause}
                   UNION ALL
                   SELECT {row_number}, {values}
                )"#,
                    columns = columns,
                    table_alias = as_if.alias,
                    suffix = suffix,
                    table_name = as_if.table,
                    row_number = as_if.row_number,
                    values = values,
                    where_clause = where_clause,
                )
            }
        }
    };

    sql
}

/// Given a config map, a db connection pool, a table name, a column name, and a cell to validate,
/// check the cell value against any foreign keys that have been defined for the column. If there is
/// a violation, indicate it with an error message attached to the cell. Optionally, if a
/// transaction is given, use that instead of the pool for database access. Optionally, if
/// query_as_if is given, use it to query the table counterfactually. Optionally, if a cache is
/// given, use the cache to determine foreign key violations instead of querying the database. Note
/// that caching is normally used only in the context of batch processing.
pub async fn validate_cell_foreign_constraints(
    config: &ValveConfig,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &String,
    column_name: &String,
    cell: &mut ValveCell,
    query_as_if: Option<&QueryAsIf>,
    cache: &Option<(Vec<SerdeValue>, Vec<SerdeValue>)>,
) -> Result<()> {
    let fkeys = config
        .constraint
        .foreign
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .iter()
        .filter(|t| t.column == *column_name)
        .collect::<Vec<_>>();

    // If there are no foreign keys, then just return:
    if fkeys.is_empty() {
        return Ok(());
    }

    let as_if_clause = match query_as_if {
        Some(query_as_if) => {
            format!(
                "WITH {} ",
                as_if_to_sql(config, &DbKind::from_pool(pool)?, &query_as_if, false)
            )
        }
        None => "".to_string(),
    };
    let as_if_clause_for_conflict = match query_as_if {
        Some(query_as_if) => {
            format!(
                "WITH {} ",
                as_if_to_sql(config, &DbKind::from_pool(pool)?, &query_as_if, true)
            )
        }
        None => "".to_string(),
    };

    // Given a db pool, optionally a transaction, and an as_if clause, checks whether the given
    // value is in the given column, which has the given sql_type, of the given table. If the
    // optional cache is provided, use it to look up the values of the column instead of accessing
    // the database. This cache has the form of a tuple of vectors of SerdeValues, such that the
    // vector in the first position is used if the table name does not end in '_conflict', and the
    // vector in the second position is used if it does.
    async fn fkey_in_db(
        pool: &AnyPool,
        tx: &mut Option<&mut Transaction<'_, sqlx::Any>>,
        as_if_clause: &str,
        table: &str,
        column: &str,
        sql_type: &str,
        value: &str,
        cache: &Option<(Vec<SerdeValue>, Vec<SerdeValue>)>,
    ) -> Result<bool> {
        match cache {
            Some((values, conflict_values)) => {
                let values = {
                    if table.ends_with("_conflict") {
                        conflict_values
                    } else {
                        values
                    }
                };
                Ok(values.iter().any(|cached_value| match cached_value {
                    SerdeValue::String(s) => s == value,
                    _ => cached_value.to_string() == value,
                }))
            }
            None => {
                let fsql = local_sql_syntax(
                    &DbKind::from_pool(pool)?,
                    &format!(
                        r#"{}SELECT 1 FROM "{}" WHERE "{}" = {} LIMIT 1"#,
                        as_if_clause, table, column, SQL_PARAM,
                    ),
                );
                let frows = {
                    let mut query = sqlx_query(&fsql);
                    match get_query_param(&json!(value), &sql_type) {
                        QueryParam::Integer(p) => query = query.bind(p),
                        QueryParam::Numeric(p) => query = query.bind(p),
                        QueryParam::Real(p) => query = query.bind(p),
                        QueryParam::String(p) => query = query.bind(p),
                    }
                    if let None = tx {
                        query.fetch_all(pool).await?
                    } else {
                        query
                            .fetch_all(tx.as_mut().unwrap().acquire().await?)
                            .await?
                    }
                };
                Ok(!frows.is_empty())
            }
        }
    }

    // Check if the column has the list() datatype. If so parse the values in the list and
    // iterate over them.
    let strvalue = cell.strvalue();
    let values = {
        let value_type = get_value_type(config, datatype_conditions, table_name, column_name);
        match &value_type {
            ValueType::Single => vec![strvalue.as_str()],
            ValueType::List(_, separator) => strvalue.split(separator).collect::<Vec<_>>(),
        }
    };
    for value in &values {
        for fkey in &fkeys {
            let ftable = &fkey.ftable;
            let (as_if_clause, ftable_alias) = match query_as_if {
                Some(query_as_if) if *ftable == query_as_if.table => {
                    (as_if_clause.to_string(), query_as_if.alias.to_string())
                }
                _ => ("".to_string(), ftable.to_string()),
            };
            let fcolumn = &fkey.fcolumn;
            let sql_type = get_sql_type_from_global_config(
                &config,
                ftable,
                fcolumn,
                &DbKind::from_pool(pool)?,
            )
            .to_lowercase();
            if !fkey_in_db(
                pool,
                &mut tx,
                &as_if_clause,
                &ftable_alias,
                fcolumn,
                &sql_type,
                value,
                cache,
            )
            .await?
            {
                cell.valid = false;
                let mut message = ValveCellMessage {
                    rule: "key:foreign".to_string(),
                    level: "error".to_string(),
                    message: format!(
                        "Value '{}' of column {} is not in {}.{}",
                        value, column_name, ftable, fcolumn
                    ),
                };
                let foptions = &config
                    .table
                    .get(ftable)
                    .expect(&format!(
                        "Foreign table: '{}' is not in table config",
                        ftable
                    ))
                    .options;

                if foptions.contains("conflict") {
                    let (as_if_clause_for_conflict, ftable_alias) = match query_as_if {
                        Some(query_as_if) if *ftable == query_as_if.table => (
                            as_if_clause_for_conflict.to_string(),
                            query_as_if.alias.to_string(),
                        ),
                        _ => ("".to_string(), ftable.to_string()),
                    };
                    if fkey_in_db(
                        pool,
                        &mut tx,
                        &as_if_clause_for_conflict,
                        &format!("{}_conflict", ftable_alias),
                        fcolumn,
                        &sql_type,
                        value,
                        cache,
                    )
                    .await?
                    {
                        message.message = format!(
                            "Value '{}' of column {} exists only in {}_conflict.{}",
                            value, column_name, ftable, fcolumn
                        );
                    }
                }
                cell.messages.push(message);
            }
        }
    }

    Ok(())
}

/// Given a config map, a db connection pool, a table name, a column name, a cell to validate,
/// and a list of previously validated rows, check the cell value against any unique-type keys that
/// have been defined for the column. If there is a violation, indicate it with an error message
/// attached to the cell. If `row_number` is set to None, then no row corresponding to the given
/// cell is assumed to exist in the table. Optionally, if a transaction is given, use that
/// instead of the pool for database access. Optionally, if a cache is given, use that to determine
/// the validity of the cell rather than accessing the database.
pub async fn validate_cell_unique_constraints(
    config: &ValveConfig,
    pool: &AnyPool,
    tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &String,
    column_name: &String,
    cell: &mut ValveCell,
    prev_results: &Vec<ValveRow>,
    row_number: Option<u32>,
    cache: &Option<Vec<SerdeValue>>,
) -> Result<()> {
    // If the column has a primary or unique key constraint, or if it is the child associated with
    // a tree, then if the value of the cell is a duplicate either of one of the previously
    // validated rows in the batch, or a duplicate of a validated row that has already been inserted
    // into the table, mark it with the corresponding error:
    let primaries = config
        .constraint
        .primary
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name));
    let uniques = config
        .constraint
        .unique
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name));

    let is_primary = primaries.contains(column_name);
    let is_unique = !is_primary && uniques.contains(column_name);

    fn make_error(rule: &str, column_name: &String) -> ValveCellMessage {
        ValveCellMessage {
            rule: rule.to_string(),
            level: "error".to_string(),
            message: format!("Values of {} must be unique", column_name.to_string()),
        }
    }

    async fn in_db(
        config: &ValveConfig,
        pool: &AnyPool,
        mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
        table_name: &String,
        column_name: &String,
        cell: &mut ValveCell,
        row_number: Option<u32>,
        cache: &Option<Vec<SerdeValue>>,
    ) -> Result<bool> {
        // If a cache is given, just check it, otherwise access the database.
        match cache {
            Some(values) => Ok(values.iter().any(|cached_value| match cached_value {
                SerdeValue::String(s) => *s == cell.value,
                _ => cached_value.to_string() == cell.value,
            })),
            None => {
                let table_options = get_table_options_from_config(config, table_name)?;
                // If the table does not have a conflict table then there is no view to check, so
                // we check the table itself.
                let mut query_table = {
                    if !table_options.contains("conflict") {
                        table_name.to_string()
                    } else {
                        format!("{}_view", table_name)
                    }
                };
                let mut with_sql = String::new();
                let except_table = format!("{}_exc", query_table);
                if let Some(row_number) = row_number {
                    with_sql = format!(
                        r#"WITH "{}" AS (
                               SELECT * FROM "{}"
                               WHERE "row_number" != {}
                           ) "#,
                        except_table, query_table, row_number
                    );
                }

                if !with_sql.is_empty() {
                    query_table = except_table;
                }

                let sql_type = get_sql_type_from_global_config(
                    &config,
                    &table_name,
                    &column_name,
                    &DbKind::from_pool(pool)?,
                );
                let sql_param = cast_sql_param_from_text(&sql_type);
                let sql = local_sql_syntax(
                    &DbKind::from_pool(pool)?,
                    &format!(
                        r#"{} SELECT 1 FROM "{}" WHERE "{}" = {} LIMIT 1"#,
                        with_sql, query_table, column_name, sql_param
                    ),
                );
                let strvalue = cell.strvalue();
                let query = sqlx_query(&sql).bind(&strvalue);
                if let None = tx {
                    Ok(!query.fetch_all(pool).await?.is_empty())
                } else {
                    Ok(!query
                        .fetch_all(tx.as_mut().unwrap().acquire().await?)
                        .await?
                        .is_empty())
                }
            }
        }
    }

    if is_primary || is_unique {
        let in_prev_results = !prev_results
            .iter()
            .filter(|p| {
                p.contents.get(column_name).unwrap().value == cell.value
                    && p.contents.get(column_name).unwrap().valid
            })
            .collect::<Vec<_>>()
            .is_empty();

        if in_prev_results
            || in_db(
                config,
                pool,
                tx,
                table_name,
                column_name,
                cell,
                row_number,
                cache,
            )
            .await?
        {
            cell.valid = false;
            if is_primary || is_unique {
                let error_message;
                if is_primary {
                    error_message = make_error("key:primary", column_name);
                } else {
                    error_message = make_error("key:unique", column_name);
                }
                cell.messages.push(error_message);
            }
        }
    }
    Ok(())
}
