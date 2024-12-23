//! The Valve Toolkit - low-level functions useful for working with Valve.

use crate::{
    ast::Expression,
    internal::generate_internal_table_config,
    validate::{validate_row_tx, validate_rows_constraints, validate_rows_intra},
    valve::{
        JsonRow, Valve, ValveCell, ValveCellMessage, ValveColumnConfig, ValveConfig,
        ValveConstraintConfig, ValveDatatypeConfig, ValveError, ValveForeignConstraint,
        ValveMessage, ValveRow, ValveRuleConfig, ValveSpecialConfig, ValveTableConfig,
        ValveTreeConstraint,
    },
    valve_grammar::StartParser,
    ALLOWED_OPTIONS, CHUNK_SIZE, INTERNAL_TABLES, MAX_DB_CONNECTIONS, MOVE_INTERVAL,
    MULTI_THREADED, REQUIRED_COLUMN_COLUMNS, REQUIRED_DATATYPE_COLUMNS, REQUIRED_RULE_COLUMNS,
    REQUIRED_TABLE_COLUMNS, SQL_PARAM,
};
use anyhow::Result;
use async_recursion::async_recursion;
use crossbeam;
use csv::{ReaderBuilder, StringRecordsIter};
use indexmap::IndexMap;
use indoc::indoc;
use is_executable::IsExecutable;
use itertools::{IntoChunks, Itertools};
use petgraph::{
    algo::{all_simple_paths, toposort},
    graphmap::DiGraphMap,
    Direction,
};
use regex::Regex;
use serde_json::{json, Value as SerdeValue};
use sqlx::{
    any::{AnyConnectOptions, AnyKind, AnyPool, AnyPoolOptions, AnyRow},
    query as sqlx_query, Acquire, Column, Row, Transaction, ValueRef,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    iter::FromIterator,
    path::Path,
    str::FromStr,
    sync::Arc,
};

/// Represents the kind of database being managed
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbKind {
    Postgres,
    Sqlite,
}

impl DbKind {
    /// Given a database connection pool, return the corresponding DbKind.
    pub fn from_pool(pool: &AnyPool) -> Result<DbKind> {
        let any_kind = pool.any_kind();
        if any_kind == AnyKind::Postgres {
            Ok(Self::Postgres)
        } else if any_kind == AnyKind::Sqlite {
            Ok(Self::Sqlite)
        } else {
            Err(ValveError::InputError(format!("Unsupported database type: {:?}", any_kind)).into())
        }
    }
}

/// Represents a structure such as those found in the `structure` column of the `column` table in
/// both its parsed format (i.e., as an [Expression](ast/enum.Expression.html)) as well as in its
/// original format (i.e., as a plain [String]).
#[derive(Clone)]
pub struct ParsedStructure {
    pub original: String,
    pub parsed: Expression,
}

// We use Debug here instead of Display because we have only implemented Debug for Expressions.
// See the comment about this in ast.rs.
impl std::fmt::Debug for ParsedStructure {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{\"parsed_structure\": {{\"original\": \"{}\", \"parsed\": {:?}}}}}",
            &self.original, &self.parsed
        )
    }
}

/// The type of value the condition should be applied to.
#[derive(Clone, Debug, PartialEq)]
pub enum ValueType {
    /// A single value
    Single,
    /// List(DATATYPE, SEP): A list of values of a given datatype, separated by a given separator.
    List(String, String),
}

/// Represents a condition in three different ways: (i) in String format, (ii) as a parsed
/// [Expression](ast/enum.Expression.html), and (iii) as a pre-compiled regular expression.
#[derive(Clone)]
pub struct CompiledCondition {
    pub value_type: ValueType,
    pub original: String,
    pub parsed: Expression,
    pub compiled: Arc<dyn Fn(&str) -> bool + Sync + Send>,
}

// We use Debug here instead of Display because we have only implemented Debug for Expressions.
// See the comment about this in ast.rs.
impl std::fmt::Debug for CompiledCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{\"compiled_condition\": {{\"original\": \"{}\", \"parsed\": {:?}}}}}",
            &self.original, &self.parsed
        )
    }
}

/// Represents a 'when-then' condition, as found in the `rule` table, as two
/// [CompiledCondition](struct.CompiledCondition.html) structs corresponding to the when and then
/// parts of the given rule.
#[derive(Clone)]
pub struct ColumnRule {
    pub when: CompiledCondition,
    pub then: CompiledCondition,
}

// We use Debug here instead of Display because we have only implemented Debug for Expressions.
// See the comment about this in ast.rs.
impl std::fmt::Debug for ColumnRule {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{\"column_rule\": {{\"when\": {:?}, \"then\": {:?}}}}}",
            &self.when, &self.then
        )
    }
}

/// Used for counterfactual queries. Use this struct to specify that a query should be run as if
/// the row specified in the instance were (depending on `kind`) either added, removed or replaced.
#[derive(Clone, Debug)]
pub struct QueryAsIf {
    pub kind: QueryAsIfKind,
    pub table: String,
    // Although PostgreSQL allows it, SQLite does not allow a CTE named 'foo' to refer to a table
    // named 'foo' so we need to use an alias:
    pub alias: String,
    pub row_number: u32,
    pub row: Option<ValveRow>,
}

/// The sense in which a [QueryAsIf] struct should be interpreted.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryAsIfKind {
    Add,
    Remove,
    Replace,
}

/// Used to represent a generic query parameter for binding to a SQLx query.
pub enum QueryParam {
    Numeric(f64),
    Real(f64),
    Integer(i32),
    String(String),
}

/// Given the path to a table table (either a table.tsv file or a database containing a
/// table named "table"), load and check the 'table', 'column', and 'datatype' tables, and return
/// the following items:
/// - Special table configuration information
/// - Table configuration information for all managed tables
/// - Table configuration information for all managed datatypes
/// - Rule configuration information for every column of every managed table
/// - Constraint configuration information
/// - The list of managed tables in dependency order
/// - A map from table names to the tables that depend on a given table
/// - A map from table names to the tables that a given table depends on
pub async fn read_config_files(
    path: &str,
    parser: &StartParser,
    pool: &AnyPool,
) -> Result<(
    ValveSpecialConfig,
    HashMap<String, ValveTableConfig>,
    Vec<String>,
    HashMap<String, ValveDatatypeConfig>,
    HashMap<String, HashMap<String, Vec<ValveRuleConfig>>>,
    ValveConstraintConfig,
    Vec<String>,
    HashMap<String, Vec<String>>,
    HashMap<String, Vec<String>>,
    IndexMap<u32, Vec<ValveMessage>>,
)> {
    // Given a list of columns that are required for some table, and a subset of those columns
    // that are required to have values, check if both sets of requirements are met by the given
    // row. Returns a ValveError if both sets of requirements are not met, or if values_are_required
    // is not a subset of columns_are_required.
    fn check_table_requirements(
        columns_are_required: &Vec<&str>,
        values_are_required: &Vec<&str>,
        row: &JsonRow,
    ) -> Result<()> {
        let columns_are_required: HashSet<&str> =
            HashSet::from_iter(columns_are_required.iter().cloned());
        let values_are_required: HashSet<&str> =
            HashSet::from_iter(values_are_required.iter().cloned());
        if !values_are_required.is_subset(&columns_are_required) {
            return Err(ValveError::InputError(format!(
                "{:?} is not a subset of {:?}",
                values_are_required, columns_are_required
            ))
            .into());
        }

        for &column in columns_are_required.iter() {
            match row.get(column).and_then(|c| c.as_str()) {
                None => {
                    return Err(ValveError::ConfigError(format!(
                        "Missing required column '{}'",
                        column
                    ))
                    .into());
                }
                Some(value) if value == "" && values_are_required.contains(&column) => {
                    return Err(ValveError::ConfigError(format!(
                        "Missing required value for '{}'",
                        column
                    ))
                    .into());
                }
                _ => (),
            }
        }
        Ok(())
    }

    // 1. Load the table config for the 'table' table from the given path, and determine the
    // table names to use for the other special config types: 'column', 'datatype', and 'rule', then
    // save those in specials_config. Also begin filling out the more general table configuration
    // information related to each of those tables, to which further info will be added later.
    let mut specials_config = ValveSpecialConfig::default();
    let mut tables_config = HashMap::new();
    let mut table_order: Vec<String> = vec![];
    let rows = {
        // Read in the table table from either a file or the database table called "table".
        if path.to_lowercase().ends_with(".tsv") {
            read_tsv_into_vector(path)?
        } else {
            read_db_table_into_vector(pool, "table").await?
        }
    };

    let mut startup_table_messages = IndexMap::new();
    for (row_number, row) in rows.iter().enumerate() {
        // enumerate() begins at 0 but we want to count rows from 1:
        let row_number = row_number as u32;
        let row_number = row_number + 1;
        if let Err(e) = check_table_requirements(&REQUIRED_TABLE_COLUMNS, &vec!["table"], &row) {
            return Err(ValveError::ConfigError(format!(
                "Error while reading '{}': {:?}",
                path, e
            ))
            .into());
        }

        let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
        table_order.push(row_table.into());
        let row_path = row.get("path").and_then(|t| t.as_str()).unwrap();
        let row_path_info = Path::new(&row_path);
        let row_type = row.get("type").and_then(|t| t.as_str()).unwrap();
        let row_type = row_type.to_lowercase();
        let row_type = row_type.as_str();
        let row_options = match row.get("options") {
            Some(o) => o.as_str().unwrap(),
            None => "",
        };
        let row_options = row_options.to_lowercase();
        let row_options = row_options.as_str().split(" ").collect::<Vec<_>>();
        let (row_options, messages) = match normalize_options(&row_options, row_number) {
            Err(e) => {
                return Err(ValveError::ConfigError(format!(
                    "Error while reading options for '{}' from table table: {}",
                    row_table, e,
                ))
                .into());
            }
            Ok((o, m)) => (o, m),
        };
        startup_table_messages.insert(row_number, messages);
        let row_desc = row.get("description").and_then(|t| t.as_str()).unwrap();

        // Here is a summary of the allowed table configurations for the various table modes:
        // - Views are allowed to have an empty path. If the path is non-empty then it must either
        //   end (case insensitively) in '.sql' or be an executable file. It must not end (case
        //   insensitively) in '.tsv'.
        // - A table is allowed to have an empty path unless it is editable. If the path is
        //   non-empty then it can be a file ending (case insensitively) with '.tsv', a file ending
        //   (case insensitively) with '.sql', or an executable file.
        let is_view = row_options.contains("db_view");
        let is_readonly = !row_options.contains("edit");
        let is_internal = row_options.contains("internal");
        if is_view || is_readonly {
            if is_view && row_path.ends_with(".tsv") {
                return Err(ValveError::ConfigError(format!(
                    "Invalid path '{}' for view '{}'. '.tsv' files are not supported for views.",
                    row_path, row_table,
                ))
                .into());
            }
            if row_path != "" && !row_path.ends_with(".tsv") && !row_path.ends_with(".sql") {
                if !row_path_info.is_executable() {
                    return Err(ValveError::ConfigError(format!(
                        "The generic program '{}' associated with the view or readonly table '{}' \
                         is not executable (assuming that it even exists at all)",
                        row_path, row_table
                    ))
                    .into());
                }
            }
        } else if !is_internal && !row_path.to_lowercase().ends_with(".tsv") {
            return Err(ValveError::ConfigError(format!(
                "Illegal path for table '{}'. Editable tables require a path that \
                 ends in '.tsv'",
                row_table
            ))
            .into());
        }

        // Check that the table table path is the same as the path that was input as an argument to
        // this function:
        if row_type == "table" {
            if row_table != "table" {
                return Err(ValveError::ConfigError(format!(
                    "Invalid table table name: '{}'. The table table must be named 'table'",
                    row_table,
                ))
                .into());
            }
            if path.to_lowercase().ends_with(".tsv") && row_path != path {
                return Err(ValveError::ConfigError(format!(
                    "The \"table\" table path '{}' is not the expected '{}'",
                    row_path, path
                ))
                .into());
            }
        }

        let duplicate_err_msg = format!(
            "Multiple tables with type '{}' declared in '{}'",
            row_type, path
        );
        match row_type {
            "" => (), // Tables with no type are ignored.
            "column" => {
                if specials_config.column != "" {
                    return Err(ValveError::ConfigError(duplicate_err_msg).into());
                }
                specials_config.column = row_table.to_string();
            }
            "datatype" => {
                if specials_config.datatype != "" {
                    return Err(ValveError::ConfigError(duplicate_err_msg).into());
                }
                specials_config.datatype = row_table.to_string();
            }
            "rule" => {
                if specials_config.rule != "" {
                    return Err(ValveError::ConfigError(duplicate_err_msg).into());
                }
                specials_config.rule = row_table.to_string();
            }
            "table" => {
                if specials_config.table != "" {
                    return Err(ValveError::ConfigError(duplicate_err_msg).into());
                }
                specials_config.table = row_table.to_string();
            }
            _ => {
                return Err(ValveError::ConfigError(format!(
                    "Unrecognized table type '{}' in '{}'",
                    row_type, path
                ))
                .into())
            }
        };

        tables_config.insert(
            row_table.to_string(),
            ValveTableConfig {
                table: row_table.to_string(),
                table_type: row_type.to_string(),
                options: row_options,
                description: row_desc.to_string(),
                path: row_path.to_string(),
                ..Default::default()
            },
        );
    }

    // Check that all the required special tables are present
    if specials_config.column == "" {
        return Err(ValveError::ConfigError(format!(
            "Missing required 'column' table in '{}'",
            path
        ))
        .into());
    }
    if specials_config.datatype == "" {
        return Err(ValveError::ConfigError(format!(
            "Missing required 'datatype' table in '{}'",
            path
        ))
        .into());
    }
    if specials_config.table == "" {
        return Err(ValveError::ConfigError(format!(
            "Missing required 'table' table in '{}'",
            path
        ))
        .into());
    }

    // Helper function for extracting special configuration (other than the main 'table'
    // configuration) from either a file or a table in the database, depending on the value of
    // `path`. When `path` ends in '.tsv', the path of the config table corresponding to
    // `table_type` is looked up, the TSV is read, and the rows are returned. When `path` does not
    // end in '.tsv', the table name corresponding to `table_type` is looked up in the database
    // indicated by `path`, the table is read, and the rows are returned.
    async fn get_special_config(
        table_type: &str,
        specials_config: &ValveSpecialConfig,
        tables_config: &HashMap<String, ValveTableConfig>,
        table_table: &str,
        pool: &AnyPool,
    ) -> Result<Vec<JsonRow>> {
        if table_table.to_lowercase().ends_with(".tsv") {
            let table_name = match table_type {
                "column" => &specials_config.column,
                "datatype" => &specials_config.datatype,
                "rule" => {
                    let rule_table = &specials_config.rule;
                    if rule_table == "" {
                        return Err(ValveError::ConfigError(format!(
                            "Tried to get special config for rule table but it is undefined"
                        ))
                        .into());
                    }
                    rule_table
                }
                _ => {
                    return Err(ValveError::InputError(format!(
                        "In get_special_config(): Table type '{}' not supported for this function.",
                        table_type
                    ))
                    .into())
                }
            };
            let path = String::from(
                tables_config
                    .get(table_name)
                    .and_then(|t| Some(t.path.to_string()))
                    .ok_or(ValveError::ConfigError(format!(
                        "Table '{}', supposedly of type '{}', not found in tables config",
                        table_name, table_type
                    )))?,
            );
            read_tsv_into_vector(&path)
        } else {
            let mut db_table = None;
            for (table_name, table_config) in tables_config {
                let this_type = table_config.table_type.as_str();
                if this_type == table_type {
                    db_table = Some(table_name);
                    break;
                }
            }
            let db_table = match db_table {
                None => {
                    return Err(ValveError::ConfigError(format!(
                        "Could not determine special table name for type '{}'.",
                        table_type
                    ))
                    .into())
                }
                Some(table) => table,
            };
            read_db_table_into_vector(pool, db_table).await
        }
    }

    // 2. Load the datatype table.
    let mut datatypes_config = HashMap::new();
    let rows = get_special_config("datatype", &specials_config, &tables_config, path, pool).await?;
    for row in rows {
        if let Err(e) =
            check_table_requirements(&REQUIRED_DATATYPE_COLUMNS, &vec!["datatype"], &row)
        {
            return Err(ValveError::ConfigError(format!(
                "Error while reading from datatype table: {:?}",
                e
            ))
            .into());
        }

        let dt_name = row.get("datatype").and_then(|d| d.as_str()).unwrap();
        let sql_type = row.get("sql_type").and_then(|s| s.as_str()).unwrap();
        let condition = row.get("condition").and_then(|s| s.as_str()).unwrap();
        let description = row.get("description").and_then(|s| s.as_str()).unwrap();
        let parent = row.get("parent").and_then(|s| s.as_str()).unwrap();
        datatypes_config.insert(
            dt_name.to_string(),
            ValveDatatypeConfig {
                sql_type: sql_type.to_string(),
                condition: condition.to_string(),
                datatype: dt_name.to_string(),
                description: description.to_string(),
                parent: parent.to_string(),
            },
        );
    }

    // Check that all the essential datatypes have been configured:
    for dt in vec!["text", "empty", "line", "trimmed_line", "nonspace", "word"] {
        if !datatypes_config.contains_key(dt) {
            return Err(
                ValveError::ConfigError(format!("Missing required datatype: '{}'", dt)).into(),
            );
        }
    }

    // 3. Load the column table.
    let rows = get_special_config("column", &specials_config, &tables_config, path, pool).await?;
    let special_tables = vec![
        specials_config.table.to_string(),
        specials_config.column.to_string(),
        specials_config.datatype.to_string(),
        specials_config.rule.to_string(),
    ];
    // The defined_column_orderings map, which contains the columns of a given table in the order in
    // which they have been defined in the column table, is used as a default in the determination
    // of the [ValveTableConfig::column_order] field, in the case where there no .TSV file
    // representing the table has been configured in valve.
    let mut defined_column_orderings = IndexMap::new();
    for row in rows {
        if let Err(e) = check_table_requirements(
            &REQUIRED_COLUMN_COLUMNS,
            &vec!["table", "column", "datatype"],
            &row,
        ) {
            return Err(ValveError::ConfigError(format!(
                "Error while reading from column table: {:?}",
                e
            ))
            .into());
        }

        let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
        if !tables_config.contains_key(row_table) {
            return Err(ValveError::ConfigError(format!("Undefined table '{}'", row_table)).into());
        }
        let nulltype = row.get("nulltype").and_then(|t| t.as_str()).unwrap();
        if nulltype != "" && !datatypes_config.contains_key(nulltype) {
            return Err(
                ValveError::ConfigError(format!("Undefined nulltype '{}'", nulltype)).into(),
            );
        }
        let datatype = row.get("datatype").and_then(|d| d.as_str()).unwrap();
        if !datatypes_config.contains_key(datatype) {
            return Err(
                ValveError::ConfigError(format!("Undefined datatype '{}'", datatype)).into(),
            );
        }
        let column_name = row.get("column").and_then(|c| c.as_str()).unwrap();
        let description = row.get("description").and_then(|c| c.as_str()).unwrap();
        let mut label = row
            .get("label")
            .and_then(|c| c.as_str())
            .unwrap()
            .to_string();
        if !label.is_empty() && special_tables.contains(&row_table.to_string()) {
            log::warn!(
                "Label '{}' for column '{}' of special table '{}' will be ignored.",
                label,
                column_name,
                row_table
            );
            label = String::from("");
        }
        let structure = row.get("structure").and_then(|c| c.as_str()).unwrap();

        let default = match row.get("default") {
            None => SerdeValue::String("".to_string()),
            Some(default) => default.clone(),
        };

        // If an entry in the defined_column_orderings map for this table doesn't already exist,
        // create one:
        if defined_column_orderings
            .keys()
            .filter(|k| *k == row_table)
            .collect::<Vec<_>>()
            .is_empty()
        {
            defined_column_orderings.insert(row_table.to_string(), vec![]);
        }

        // Add the column name to the ordered list of columns for this table:
        match defined_column_orderings
            .get_mut(row_table)
            .and_then(|t| Some(t.push(column_name.to_string())))
        {
            None => {
                return Err(ValveError::DataError(format!(
                    "Could not insert column '{}'",
                    column_name
                ))
                .into());
            }
            _ => (),
        };

        tables_config.get_mut(row_table).and_then(|t| {
            Some(t.column.insert(
                column_name.to_string(),
                ValveColumnConfig {
                    table: row_table.to_string(),
                    column: column_name.to_string(),
                    datatype: datatype.to_string(),
                    description: description.to_string(),
                    label: label,
                    structure: structure.to_string(),
                    nulltype: nulltype.to_string(),
                    default: default,
                },
            ))
        });
    }

    // 4. Load rule table if it exists
    let mut rules_config = HashMap::new();
    if specials_config.rule != "" {
        let rows = get_special_config("rule", &specials_config, &tables_config, path, pool).await?;
        for row in rows {
            if let Err(e) = check_table_requirements(
                &REQUIRED_RULE_COLUMNS,
                &vec![
                    "table",
                    "when column",
                    "when condition",
                    "then column",
                    "then condition",
                    "level",
                    "description",
                ],
                &row,
            ) {
                return Err(ValveError::ConfigError(format!(
                    "Error while reading from rule table: {:?}",
                    e
                ))
                .into());
            }

            let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
            if !tables_config.contains_key(row_table) {
                return Err(ValveError::ConfigError(format!(
                    "Undefined table '{}' while reading rule configuration",
                    row_table
                ))
                .into());
            }

            // Add the rule specified in the given row to the list of rules associated with the
            // value of the when column:
            if !rules_config.contains_key(row_table) {
                rules_config.insert(String::from(row_table), HashMap::new());
            }
            let table_rule_config = rules_config.get_mut(row_table).unwrap();

            let when_col = row.get("when column").and_then(|c| c.as_str()).unwrap();
            if !table_rule_config.contains_key(when_col) {
                table_rule_config.insert(String::from(when_col), vec![]);
            }

            let column_rule_config = table_rule_config.get_mut(&when_col.to_string()).unwrap();
            let desc = row.get("description").and_then(|c| c.as_str()).unwrap();
            let level = row.get("level").and_then(|c| c.as_str()).unwrap();
            let when_con = row.get("when condition").and_then(|c| c.as_str()).unwrap();
            let then_col = row.get("then column").and_then(|c| c.as_str()).unwrap();
            let then_con = row.get("then condition").and_then(|c| c.as_str()).unwrap();
            column_rule_config.push(ValveRuleConfig {
                description: desc.to_string(),
                level: level.to_string(),
                table: row_table.to_string(),
                then_column: then_col.to_string(),
                then_condition: then_con.to_string(),
                when_column: when_col.to_string(),
                when_condition: when_con.to_string(),
            });
        }
    }

    // 5. Initialize the constraints config:
    let mut constraints_config = ValveConstraintConfig::default();
    for table_name in &table_order {
        let table_name = table_name.to_string();
        let this_table = tables_config
            .get(&table_name)
            .ok_or(ValveError::ConfigError(format!(
                "Table '{}' not found in tables config",
                table_name
            )))?;

        let mut path = None;
        if !Path::new(&this_table.path).is_file() {
            log::warn!(
                "Path '{}' of table '{}' does not exist",
                this_table.path,
                table_name
            );
        } else if Path::new(&this_table.path).canonicalize().is_err() {
            log::warn!(
                "Path '{}' of table '{}' could not be made canonical",
                this_table.path,
                table_name
            );
        } else {
            path = Some(this_table.path.to_string())
        }

        // Constraints on internal tables do not need to be configured explicitly:
        if this_table.options.contains("internal") {
            continue;
        }

        let this_column_config = &this_table.column;
        let defined_labels = this_column_config
            .iter()
            .map(|(k, v)| {
                if v.label != "" {
                    v.label.to_string()
                } else {
                    k.to_string()
                }
            })
            .collect::<Vec<_>>();

        // We use column_order to explicitly indicate the order in which the columns should appear
        // in the table, for later reference. The default is to preserve the order from the actual
        // table file. If that does not exist, we use the ordering in defined_labels.
        let mut column_order = vec![];
        match path {
            Some(path) if path.to_lowercase().ends_with(".tsv") => {
                // Get the actual columns from the data itself. Note that we set has_headers to
                // false (even though the files have header rows) in order to explicitly read the
                // header row.
                let mut rdr = ReaderBuilder::new()
                    .has_headers(false)
                    .delimiter(b'\t')
                    .from_reader(File::open(path.clone()).map_err(|err| {
                        ValveError::ConfigError(format!(
                            "Unable to open '{}': {}",
                            path.clone(),
                            err
                        ))
                    })?);
                let mut iter = rdr.records();
                if let Some(result) = iter.next() {
                    let actual_labels = result
                        .map_err(|e| {
                            ValveError::ConfigError(format!(
                                "Unable to read row from '{}': {}",
                                path, e
                            ))
                        })?
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>();
                    // Make sure that the actual columns found in the table file are all defined
                    // in the column configuration:
                    for label_name in &actual_labels {
                        if !defined_labels.contains(&&label_name.to_string()) {
                            return Err(ValveError::ConfigError(format!(
                                "Label '{}' of table '{}' not in column config",
                                label_name, table_name
                            ))
                            .into());
                        }
                    }
                    // The defined_labels are what will be used to create the table's columns. In
                    // the case where a defined_label is not also an actual_label.
                    for label_name in &actual_labels {
                        let column_name =
                            get_column_for_label(&this_column_config, label_name, &table_name)?;
                        column_order.push(column_name);
                    }
                } else {
                    return Err(ValveError::ConfigError(format!("'{}' is empty", path)).into());
                }
            }
            _ => (),
        };

        // If for some reason we were unable to determine the column order, then use the order
        // defined in the column table:
        if column_order.is_empty() {
            match defined_column_orderings.get(&table_name) {
                Some(order) => column_order = order.to_vec(),
                _ => {
                    return Err(ValveError::DataError(format!(
                        "Table {} not found in {:?}",
                        table_name, defined_column_orderings
                    ))
                    .into());
                }
            };
        }
        tables_config
            .get_mut(&table_name)
            .and_then(|t| Some(t.column_order = column_order));

        // Populate the table constraints for this table:
        let (primaries, uniques, foreigns, trees) = get_table_constraints(
            &tables_config,
            &datatypes_config,
            parser,
            &table_name,
            &DbKind::from_pool(pool)?,
        );

        constraints_config
            .primary
            .insert(table_name.to_string(), primaries);
        constraints_config
            .unique
            .insert(table_name.to_string(), uniques);
        constraints_config
            .foreign
            .insert(table_name.to_string(), foreigns);
        constraints_config
            .tree
            .insert(table_name.to_string(), trees);
    }

    // 6. Add implicit unique constraints for trees and foreign keys:
    for (table, _) in &tables_config {
        let table_trees = constraints_config
            .tree
            .get(table)
            .expect(&format!("No tree constraints found for table '{}'", table));
        let table_uniques = constraints_config.unique.get_mut(table).expect(&format!(
            "No unique constraints found for table '{}'",
            table
        ));
        let table_primaries = constraints_config.primary.get(table).expect(&format!(
            "No primary constraints found for table '{}'",
            table
        ));
        for tree in table_trees {
            if !table_uniques.contains(&tree.child) && !table_primaries.contains(&tree.child) {
                log::warn!(
                    "Table '{}' has a tree defined on column '{}' which therefore requires \
                     a UNIQUE constraint. It will be implicitly created.",
                    table,
                    tree.child
                );
                table_uniques.push(tree.child.to_string());
            }
        }

        let table_foreigns = constraints_config.foreign.get(table).expect(&format!(
            "No foreign constraints found for table '{}'",
            table
        ));
        for foreign in table_foreigns {
            let ftable = &foreign.ftable;
            let funiques = constraints_config.unique.get_mut(ftable).expect(&format!(
                "No unique constraints found for table '{ftable}' (or '{ftable}' does not exist)",
            ));
            let fprimaries = constraints_config.primary.get(ftable).expect(&format!(
                "No primary constraints found for table '{}'",
                ftable
            ));
            let fcolumn = &foreign.fcolumn;
            if !funiques.contains(fcolumn) && !fprimaries.contains(fcolumn) {
                log::warn!(
                    "Column '{}.{}' is a foreign key for table '{}' and therefore requires \
                     a UNIQUE constraint. It will be implicitly created.",
                    ftable,
                    fcolumn,
                    table,
                );
                funiques.push(fcolumn.to_string());
            }
        }
    }

    // 7. Add internal table configuration to the table config:
    for table in INTERNAL_TABLES.iter() {
        tables_config.insert(table.to_string(), generate_internal_table_config(table));
        table_order.push(table.to_string());
    }

    // 8. Sort the tables (other than internal tables) according to their foreign key
    // dependencies so that tables are always loaded after the tables they depend on.
    let (sorted_tables, table_dependencies_in, table_dependencies_out) = verify_table_deps_and_sort(
        &table_order
            .iter()
            .cloned()
            // Internal tables will be taken account of within verify_table_deps_and_sort() and
            // manually added to the sorted table list that is returned there.
            .filter(|m| !INTERNAL_TABLES.contains(&m.to_string().as_str()))
            .collect::<Vec<_>>(),
        &constraints_config,
    );

    // 9. Finally, return all the configs:
    Ok((
        specials_config,
        tables_config,
        table_order,
        datatypes_config,
        rules_config,
        constraints_config,
        sorted_tables,
        table_dependencies_in,
        table_dependencies_out,
        startup_table_messages,
    ))
}

/// Given a string representing the location of a database, return a database connection pool.
pub async fn get_pool_from_connection_string(database: &str) -> Result<AnyPool> {
    let connection_options;
    if database.starts_with("postgresql://") {
        connection_options = AnyConnectOptions::from_str(database)?;
    } else {
        let connection_string;
        if !database.starts_with("sqlite://") {
            connection_string = format!("sqlite://{}?mode=rwc", database);
        } else {
            connection_string = database.to_string();
        }
        connection_options = AnyConnectOptions::from_str(connection_string.as_str())?;
    }

    let pool = AnyPoolOptions::new()
        .max_connections(MAX_DB_CONNECTIONS)
        .connect_with(connection_options)
        .await?;
    Ok(pool)
}

/// Given a path, read a TSV file and return a vector of rows represented as [JsonRows](JsonRow).
/// Note: Use this function to read "small" TSVs only. In particular, use this for the special
/// configuration tables.
pub fn read_tsv_into_vector(path: &str) -> Result<Vec<JsonRow>> {
    let mut rdr =
        ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(File::open(path).map_err(|err| {
                ValveError::ConfigError(format!("Unable to open '{}': {}", path, err))
            })?);

    let mut rows: Vec<JsonRow> = vec![];
    for result in rdr.deserialize() {
        match result {
            Err(e) => {
                return Err(ValveError::InputError(format!("Error reading {}: {}", path, e)).into())
            }
            Ok(row) => rows.push(row),
        }
    }

    if rows.len() < 1 {
        return Err(ValveError::DataError(format!("No rows in {}", path)).into());
    }

    for (i, row) in rows.iter().enumerate() {
        // enumerate() begins at 0 but we want to count rows from 1:
        let i = i + 1;
        for (col, val) in row {
            let val = match val {
                SerdeValue::String(s) => s.to_string(),
                _ => val.to_string(),
            };
            let trimmed_val = val.trim();
            if trimmed_val != val {
                return Err(ValveError::DataError(format!(
                    "Value '{}' of column '{}' in row {} of table '{}' {}",
                    val, col, i, path, "has leading and/or trailing whitespace."
                ))
                .into());
            }
        }
    }

    Ok(rows)
}

/// Given a database connection pool and a table name, read the entire table contents into
/// a vector of [JsonRow]s and return it. Note that this function should only be used for "small"
/// tables, e.g., configuration tables.
pub async fn read_db_table_into_vector(
    pool: &AnyPool,
    small_table_name: &str,
) -> Result<Vec<JsonRow>> {
    let mut tx = pool.begin().await?;
    let rows = read_db_table_into_vector_tx(&mut tx, small_table_name, false).await?;
    tx.commit().await?;
    Ok(rows)
}

/// Given a database transaction and a table name, read the entire table contents into
/// a vector of [JsonRow]s and return it. If `keep_rn` is set, include the row_number field
/// with each row that is returned. Note that this function should only be used for "small"
/// tables, e.g., configuration tables.
pub async fn read_db_table_into_vector_tx(
    tx: &mut Transaction<'_, sqlx::Any>,
    small_table_name: &str,
    keep_rn: bool,
) -> Result<Vec<JsonRow>> {
    let sql = format!("SELECT * FROM \"{}\"", small_table_name);
    let rows = sqlx_query(&sql)
        .fetch_all(tx.acquire().await?)
        .await
        .map_err(|e| {
            ValveError::InputError(format!(
                "Error while reading table '{}' from database: {}",
                small_table_name, e
            ))
        })?;
    let mut table_rows = vec![];
    for row in rows {
        let mut table_row = JsonRow::new();
        for column in row.columns() {
            let cname = column.name();
            if cname != "row_number" && cname != "row_order" {
                let raw_value = row.try_get_raw(format!(r#"{}"#, cname).as_str()).unwrap();
                if !raw_value.is_null() {
                    let value = get_column_value_as_string(&row, &cname, "text");
                    table_row.insert(cname.to_string(), json!(value));
                } else {
                    table_row.insert(cname.to_string(), json!(""));
                }
            } else if keep_rn && (cname == "row_number" || cname == "row_order") {
                let value = row.get::<i64, _>(cname) as u32;
                table_row.insert(cname.to_string(), json!(value));
            }
        }
        table_rows.push(table_row);
    }
    Ok(table_rows)
}

/// Given a condition on a datatype, if the condition is a Function, then parse it using
/// StartParser, create a corresponding CompiledCondition, and return it. If the condition is a
/// Label, then look for the CompiledCondition corresponding to it in datatype_conditions
/// and return it.
pub fn compile_condition(
    condition: &str,
    parser: &StartParser,
    datatype_conditions: &HashMap<String, CompiledCondition>,
) -> Result<CompiledCondition> {
    if condition == "null" || condition == "not null" {
        // The case of a "null" or "not null" condition will be treated specially later during the
        // validation phase in a way that does not utilise the associated closure. Since we still
        // have to assign some closure in these cases, we use a constant closure that always
        // returns true:
        return Ok(CompiledCondition {
            value_type: ValueType::Single,
            original: String::from(""),
            parsed: Expression::None,
            compiled: Arc::new(|_| true),
        });
    }

    let unquoted_re = Regex::new(r#"^['"](?P<unquoted>.*)['"]$"#)?;
    let parsed_condition = match parser.parse(condition) {
        Err(_) => {
            return Err(
                ValveError::InputError(format!("Could not parse condition: {}", condition)).into(),
            )
        }
        Ok(parsed_condition) => parsed_condition,
    };
    if parsed_condition.len() != 1 {
        return Err(ValveError::InputError(format!(
            "Invalid condition: '{}'. Only one condition per column is allowed.",
            condition
        ))
        .into());
    }
    let parsed_condition = &parsed_condition[0];
    match &**parsed_condition {
        Expression::Function(name, args) if name == "equals" => match &*args[0] {
            Expression::Label(label) => {
                let label = String::from(unquoted_re.replace(label, "$unquoted"));
                Ok(CompiledCondition {
                    value_type: ValueType::Single,
                    original: condition.to_string(),
                    parsed: *parsed_condition.clone(),
                    compiled: Arc::new(move |x| x == label),
                })
            }
            _ => Err(
                ValveError::InputError(format!("ERROR: Invalid condition: {}", condition)).into(),
            ),
        },
        Expression::Function(name, args)
            if vec!["exclude", "match", "search"].contains(&name.as_str()) =>
        {
            if let Expression::RegexMatch(pattern, flags) = &*args[0] {
                let mut pattern = String::from(unquoted_re.replace(pattern, "$unquoted"));
                let mut flags = String::from(flags);
                if flags != "" {
                    flags = format!("(?{})", flags.as_str());
                }
                match name.as_str() {
                    "exclude" => {
                        pattern = format!("{}{}", flags, pattern);
                        let re = Regex::new(pattern.as_str())?;
                        Ok(CompiledCondition {
                            value_type: ValueType::Single,
                            original: condition.to_string(),
                            parsed: *parsed_condition.clone(),
                            compiled: Arc::new(move |x| !re.is_match(x)),
                        })
                    }
                    "match" => {
                        pattern = format!("^({}{})$", flags, pattern);
                        let re = Regex::new(pattern.as_str())?;
                        Ok(CompiledCondition {
                            value_type: ValueType::Single,
                            original: condition.to_string(),
                            parsed: *parsed_condition.clone(),
                            compiled: Arc::new(move |x| re.is_match(x)),
                        })
                    }
                    "search" => {
                        pattern = format!("{}{}", flags, pattern);
                        let re = Regex::new(pattern.as_str())?;
                        Ok(CompiledCondition {
                            value_type: ValueType::Single,
                            original: condition.to_string(),
                            parsed: *parsed_condition.clone(),
                            compiled: Arc::new(move |x| re.is_match(x)),
                        })
                    }
                    _ => Err(ValveError::InputError(format!(
                        "Unrecognized function name: {}",
                        name
                    ))
                    .into()),
                }
            } else {
                Err(ValveError::InputError(format!(
                    "Argument to condition: {} is not a regular expression",
                    condition
                ))
                .into())
            }
        }
        Expression::Function(name, args) if name == "in" => {
            let mut alternatives: Vec<String> = vec![];
            for arg in args {
                if let Expression::Label(value) = &**arg {
                    let value = unquoted_re.replace(value, "$unquoted");
                    alternatives.push(value.to_string());
                } else {
                    return Err(ValveError::InputError(format!(
                        "Argument: {:?} to function 'in' is not a label",
                        arg
                    ))
                    .into());
                }
            }
            Ok(CompiledCondition {
                value_type: ValueType::Single,
                original: condition.to_string(),
                parsed: *parsed_condition.clone(),
                compiled: Arc::new(move |x| alternatives.contains(&x.to_string())),
            })
        }
        Expression::Function(name, args) if name == "list" => {
            let syntax_error =
                ValveError::InputError(format!("Invalid arguments for 'list': {:?}", args));
            match &*args[0] {
                Expression::Label(datatype) => match &*args[1] {
                    Expression::Label(separator) => {
                        let datatype_not_found_error = ValveError::InputError(format!(
                            "Datatype not found: '{}' in arguments for 'list': {:?}",
                            datatype, args
                        ));
                        let compiled = match datatype_conditions.get(datatype) {
                            Some(condition) => condition.compiled.clone(),
                            _ => return Err(datatype_not_found_error.into()),
                        };
                        let separator = String::from(unquoted_re.replace(separator, "$unquoted"));
                        Ok(CompiledCondition {
                            value_type: ValueType::List(
                                datatype.to_string(),
                                separator.to_string(),
                            ),
                            original: condition.to_string(),
                            parsed: *parsed_condition.clone(),
                            compiled: compiled,
                        })
                    }
                    _ => Err(syntax_error.into()),
                },
                _ => Err(syntax_error.into()),
            }
        }
        Expression::Label(value) if datatype_conditions.contains_key(&value.to_string()) => {
            let condition = datatype_conditions.get(&value.to_string()).unwrap();
            Ok(CompiledCondition {
                value_type: ValueType::Single,
                original: value.to_string(),
                parsed: condition.parsed.clone(),
                compiled: condition.compiled.clone(),
            })
        }
        _ => Err(ValveError::InputError(format!("Unrecognized condition: {}", condition)).into()),
    }
}

/// Given a vector of string slices, return a set containing the distinct options contained therein.
/// If there are logical conflicts between options, e.g., if both 'db_view' and 'load' are
/// specified (views cannot be loaded), then resolve them in favour of the last specified option by
/// removing the earlier conflicting option from the option set, and writing a warning to the log.
/// Note that if there are unrecognized options in the list these will generate warning messages but
/// will otherwise be ignored.
pub fn normalize_options(
    input_options: &Vec<&str>,
    row_number: u32,
) -> Result<(HashSet<String>, Vec<ValveMessage>)> {
    fn warn_and_get_message(
        row_number: u32,
        message: &str,
        violation: &str,
        level: &str,
        value: &str,
    ) -> ValveMessage {
        log::warn!(
            "{}: '{}' in row {} of 'table' table",
            message,
            value,
            row_number
        );
        ValveMessage {
            column: "options".to_string(),
            value: value.to_string(),
            rule: format!("option:{}", violation),
            level: level.to_string(),
            message: message.to_string(),
        }
    }

    // Collect all input options into a set, resolving any logical conflicts in favour of the last
    // read input option if necessary.
    let mut explicit_options = HashSet::new();
    let mut messages = vec![];
    for input_option in input_options {
        let input_option = input_option.trim().to_string();
        if input_option == "" {
            continue;
        }
        if !ALLOWED_OPTIONS.contains(&input_option.as_str()) {
            messages.push(warn_and_get_message(
                row_number,
                "unrecognized option",
                "unrecognized",
                "error",
                &input_option.as_str(),
            ));
            continue;
        }
        if explicit_options.contains(&input_option) {
            messages.push(warn_and_get_message(
                row_number,
                "redundant option",
                "redundant",
                "warning",
                &input_option.as_str(),
            ));
            continue;
        }

        match input_option.as_str() {
            "internal" => {
                messages.push(warn_and_get_message(
                    row_number,
                    "reserved for internal use",
                    "reserved",
                    "error",
                    &input_option.as_str(),
                ));
            }
            "db_table" => {
                if explicit_options.contains("db_view") {
                    messages.push(warn_and_get_message(
                        row_number,
                        "overrides db_view",
                        "overrides",
                        "warning",
                        &input_option.as_str(),
                    ));
                    explicit_options.remove("db_view");
                }
            }
            "db_view" => {
                for conflicting_option in [
                    "db_table",
                    "truncate",
                    "load",
                    "conflict",
                    "save",
                    "edit",
                    "validate_on_load",
                ] {
                    if explicit_options.contains(conflicting_option) {
                        messages.push(warn_and_get_message(
                            row_number,
                            &format!("overrides {}", conflicting_option),
                            "overrides",
                            "warning",
                            &input_option.as_str(),
                        ));
                        explicit_options.remove(conflicting_option);
                    }
                }
            }
            "truncate" => {
                if explicit_options.contains("db_view") {
                    messages.push(warn_and_get_message(
                        row_number,
                        "overrides db_view",
                        "overrides",
                        "warning",
                        &input_option.as_str(),
                    ));
                    explicit_options.remove("db_view");
                }
            }
            "load" => {
                if explicit_options.contains("db_view") {
                    messages.push(warn_and_get_message(
                        row_number,
                        "overrides db_view",
                        "overrides",
                        "warning",
                        &input_option.as_str(),
                    ));
                    explicit_options.remove("db_view");
                }
            }
            "conflict" => {
                for conflicting_option in ["db_view", "no-conflict"] {
                    if explicit_options.contains(conflicting_option) {
                        messages.push(warn_and_get_message(
                            row_number,
                            &format!("overrides {}", conflicting_option),
                            "overrides",
                            "warning",
                            &input_option.as_str(),
                        ));
                        explicit_options.remove(conflicting_option);
                    }
                }
            }
            "no-conflict" => {
                if explicit_options.contains("conflict") {
                    messages.push(warn_and_get_message(
                        row_number,
                        "overrides conflict",
                        "overrides",
                        "warning",
                        &input_option.as_str(),
                    ));
                    explicit_options.remove("conflict");
                }
            }
            "save" => {
                for conflicting_option in ["db_view", "no-save"] {
                    if explicit_options.contains(conflicting_option) {
                        messages.push(warn_and_get_message(
                            row_number,
                            &format!("overrides {}", conflicting_option),
                            "overrides",
                            "warning",
                            &input_option.as_str(),
                        ));
                        explicit_options.remove(conflicting_option);
                    }
                }
            }
            "no-save" => {
                if explicit_options.contains("save") {
                    messages.push(warn_and_get_message(
                        row_number,
                        "overrides save",
                        "overrides",
                        "warning",
                        &input_option.as_str(),
                    ));
                    explicit_options.remove("save");
                }
            }
            "edit" => {
                for conflicting_option in ["db_view", "no-edit"] {
                    if explicit_options.contains(conflicting_option) {
                        messages.push(warn_and_get_message(
                            row_number,
                            &format!("overrides {}", conflicting_option),
                            "overrides",
                            "warning",
                            &input_option.as_str(),
                        ));
                        explicit_options.remove(conflicting_option);
                    }
                }
            }
            "no-edit" => {
                if explicit_options.contains("edit") {
                    messages.push(warn_and_get_message(
                        row_number,
                        "overrides edit",
                        "overrides",
                        "warning",
                        &input_option.as_str(),
                    ));
                    explicit_options.remove("edit");
                }
            }
            "validate_on_load" => {
                for conflicting_option in ["db_view", "no-validate_on_load"] {
                    if explicit_options.contains(conflicting_option) {
                        messages.push(warn_and_get_message(
                            row_number,
                            &format!("overrides {}", conflicting_option),
                            "overrides",
                            "warning",
                            &input_option.as_str(),
                        ));
                        explicit_options.remove(conflicting_option);
                    }
                }
            }
            "no-validate_on_load" => {
                if explicit_options.contains("validate_on_load") {
                    messages.push(warn_and_get_message(
                        row_number,
                        "overrides validate_on_load",
                        "overrides",
                        "warning",
                        &input_option.as_str(),
                    ));
                    explicit_options.remove("validate_on_load");
                }
            }
            _ => (),
        };

        explicit_options.insert(input_option);
    }

    // Construct the base options for this view or table
    let mut base_options = HashSet::new();
    if explicit_options.contains("db_view") {
        base_options.insert("db_view".to_string());
    } else {
        base_options.insert("db_table".to_string());
        base_options.insert("truncate".to_string());
        base_options.insert("load".to_string());

        // If no options were specified, the user wants the default, which is a db_table with all
        // "extra" options enabled:
        if input_options.is_empty() || *input_options == vec![""] {
            explicit_options.insert("save".to_string());
            explicit_options.insert("edit".to_string());
            explicit_options.insert("validate_on_load".to_string());
            explicit_options.insert("conflict".to_string());
        } else {
            if !explicit_options.contains("no-save") {
                explicit_options.insert("save".to_string());
            } else if !explicit_options.contains("no-edit") {
                explicit_options.insert("edit".to_string());
            } else if !explicit_options.contains("no-validate_on_load") {
                explicit_options.insert("validate_on_load".to_string());
            } else if !explicit_options.contains("no-conflict") {
                explicit_options.insert("conflict".to_string());
            }
        }
    }
    // Remove any negative options here. They are no longer needed.
    explicit_options.remove("no-save");
    explicit_options.remove("no-edit");
    explicit_options.remove("no-validate_on_load");
    explicit_options.remove("no-conflict");
    // If the user specified the 'internal' option we also remove this here (a warning will have
    // been generated above);
    explicit_options.remove("internal");

    // Construct the union of the base and explicit sets of options and return it:
    let normalized_options = base_options.union(&explicit_options).cloned().collect();
    Ok((normalized_options, messages))
}

/// Given a global configuration struct and a table name, returns the options for the table.
pub fn get_table_options_from_config(config: &ValveConfig, table: &str) -> Result<HashSet<String>> {
    Ok(config
        .table
        .get(table)
        .ok_or::<ValveError>(
            ValveError::InputError(format!("'{}' is not in table config", table)).into(),
        )?
        .options
        .clone())
}

/// Given maps representing the table and datatype configurations, a parser, a table name, and a
/// database kind representing the type of database being used, return lists of: primary keys,
/// unique constraints, foreign keys, and trees.
pub fn get_table_constraints(
    tables_config: &HashMap<String, ValveTableConfig>,
    datatypes_config: &HashMap<String, ValveDatatypeConfig>,
    parser: &StartParser,
    table_name: &str,
    kind: &DbKind,
) -> (
    Vec<String>,
    Vec<String>,
    Vec<ValveForeignConstraint>,
    Vec<ValveTreeConstraint>,
) {
    let mut primaries = vec![];
    let mut uniques = vec![];
    let mut foreigns = vec![];
    let mut trees = vec![];

    let columns = tables_config
        .get(table_name)
        .and_then(|t| Some(t.column.clone()))
        .expect(&format!("Undefined table '{}'", table_name));
    let mut colvals = vec![];
    for (_, column) in columns.iter() {
        colvals.push(column.clone());
    }

    for row in colvals {
        let datatype = &row.datatype;
        let sql_type = get_sql_type(datatypes_config, datatype, kind);
        let column_name = &row.column;
        let structure = &row.structure;
        if structure != "" {
            let parsed_structure = parser
                .parse(&structure)
                .expect(&format!("Could not parse structure '{}'", structure));
            for expression in parsed_structure {
                match *expression {
                    Expression::Label(value) if value == "primary" => {
                        primaries.push(column_name.to_string());
                    }
                    Expression::Label(value) if value == "unique" => {
                        uniques.push(column_name.to_string());
                    }
                    Expression::Function(name, args) if name == "from" => {
                        if args.len() != 1 {
                            panic!("Invalid foreign key: {} for: {}", structure, table_name);
                        }
                        match &*args[0] {
                            Expression::Field(ftable, fcolumn) => {
                                foreigns.push(ValveForeignConstraint {
                                    original: structure.to_string(),
                                    table: table_name.to_string(),
                                    column: column_name.to_string(),
                                    ftable: ftable.to_string(),
                                    fcolumn: fcolumn.to_string(),
                                });
                            }
                            _ => {
                                panic!("Invalid foreign key: {} for: {}", structure, table_name)
                            }
                        };
                    }
                    Expression::Function(name, args) if name == "tree" => {
                        if args.len() != 1 {
                            panic!(
                                "Invalid 'tree' constraint: {} for: {}",
                                structure, table_name
                            );
                        }
                        match &*args[0] {
                            Expression::Label(child) => {
                                let child_datatype = columns
                                    .get(child)
                                    .and_then(|c| Some(c.datatype.to_string()));
                                if let None = child_datatype {
                                    panic!(
                                        "Could not determine datatype for {} of tree({})",
                                        child, child
                                    );
                                }
                                let child_datatype = child_datatype.unwrap();
                                let parent = column_name;
                                let child_sql_type = get_sql_type(
                                    datatypes_config,
                                    &child_datatype.to_string(),
                                    kind,
                                );
                                if sql_type != child_sql_type {
                                    panic!(
                                        "SQL type '{}' of '{}' in 'tree({})' for table \
                                         '{}' doe snot match SQL type: '{}' of parent: '{}'.",
                                        child_sql_type, child, child, table_name, sql_type, parent
                                    );
                                }
                                trees.push(ValveTreeConstraint {
                                    original: structure.to_string(),
                                    table: table_name.to_string(),
                                    child: child.to_string(),
                                    parent: column_name.to_string(),
                                });
                            }
                            _ => {
                                panic!(
                                    "Invalid 'tree' constraint: {} for: {}",
                                    structure, table_name
                                );
                            }
                        };
                    }
                    _ => panic!(
                        "Unrecognized structure: {} for {}.{}",
                        structure, table_name, column_name
                    ),
                };
            }
        }
    }

    return (primaries, uniques, foreigns, trees);
}

/// Given the global configuration struct and a parser, compile all of the datatype conditions,
/// add them to a hash map whose keys are the text versions of the conditions and whose values
/// are the compiled conditions, and then finally return the hash map.
pub fn generate_datatype_conditions(
    config: &ValveConfig,
    parser: &StartParser,
) -> Result<HashMap<String, CompiledCondition>> {
    // We go through the datatypes one by one, compiling any non-list types first and saving the
    // list types, which are higher level types that refer to non-list types, for later.
    let mut saved_for_last = HashMap::new();
    let mut datatype_conditions = HashMap::new();
    for (dt_name, dt_config) in config.datatype.iter() {
        let condition = &dt_config.condition;
        if condition != "" {
            if condition.starts_with("list(") {
                saved_for_last.insert(dt_name, dt_config);
            } else {
                let compiled_condition =
                    compile_condition(condition, parser, &datatype_conditions)?;
                datatype_conditions.insert(dt_name.to_string(), compiled_condition);
            }
        }
    }
    for (dt_name, dt_config) in saved_for_last.iter() {
        let condition = &dt_config.condition;
        let compiled_condition = compile_condition(&condition, parser, &datatype_conditions)?;
        datatype_conditions.insert(dt_name.to_string(), compiled_condition);
    }
    Ok(datatype_conditions)
}

/// Given the global config struct, a hash map of compiled datatype conditions (indexed by the text
/// versions of the conditions), and a parser, compile all of the rule conditions, add them to a
/// hash which has the following structure and return it:
/// ```
/// {
///      table_1: {
///          when_column_1: [rule_1, rule_2, ...],
///          ...
///      },
///      ...
/// }
/// ```
pub fn generate_rule_conditions(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    parser: &StartParser,
) -> Result<HashMap<String, HashMap<String, Vec<ColumnRule>>>> {
    let mut rule_conditions = HashMap::new();
    let tables_config = &config.table;
    let rules_config = &config.rule;
    for (rules_table, table_rules) in rules_config.iter() {
        for (column_rule_key, column_rules) in table_rules.iter() {
            for rule in column_rules {
                let table_columns = tables_config
                    .get(rules_table)
                    .unwrap()
                    .column
                    .keys()
                    .collect::<Vec<_>>();
                for column in vec![&rule.when_column, &rule.then_column] {
                    if !table_columns.contains(&column) {
                        return Err(ValveError::DataError(format!(
                            "Undefined column '{}.{}' in rules table",
                            rules_table, column
                        ))
                        .into());
                    }
                }
                let when_compiled =
                    compile_condition(&rule.when_condition, parser, &datatype_conditions)?;
                let then_compiled =
                    compile_condition(&rule.then_condition, parser, &datatype_conditions)?;

                if !rule_conditions.contains_key(rules_table) {
                    let table_rules = HashMap::new();
                    rule_conditions.insert(rules_table.to_string(), table_rules);
                }
                let table_rules = rule_conditions.get_mut(rules_table).unwrap();
                if !table_rules.contains_key(column_rule_key) {
                    table_rules.insert(column_rule_key.to_string(), vec![]);
                }
                let column_rules = table_rules.get_mut(column_rule_key).unwrap();
                column_rules.push(ColumnRule {
                    when: when_compiled,
                    then: then_compiled,
                });
            }
        }
    }
    Ok(rule_conditions)
}

/// Given the global config struct and a parser, parse all of the structure conditions, add them to
/// a hash map whose keys are given by the text versions of the conditions and whose values are
/// given by the parsed versions, and finally return the hashmap.
pub fn get_parsed_structure_conditions(
    config: &ValveConfig,
    parser: &StartParser,
) -> Result<HashMap<String, ParsedStructure>> {
    let mut parsed_structure_conditions = HashMap::new();
    let tables_config = &config.table;
    for (table, table_config) in tables_config.iter() {
        let columns_config = &table_config.column;
        for (column, column_config) in columns_config.iter() {
            let structure = &column_config.structure;
            if structure != "" {
                let parsed_structure = parser.parse(structure);
                if let Err(e) = parsed_structure {
                    return Err(ValveError::ConfigError(format!(
                        "While parsing structure: '{}' for column: '{}.{}' got error:\n{}",
                        structure, table, column, e
                    ))
                    .into());
                }
                let parsed_structure = parsed_structure.unwrap();
                let parsed_structure = &parsed_structure[0];
                let parsed_structure = ParsedStructure {
                    original: structure.to_string(),
                    parsed: *parsed_structure.clone(),
                };
                parsed_structure_conditions.insert(structure.to_string(), parsed_structure);
            }
        }
    }
    Ok(parsed_structure_conditions)
}

/// Takes as arguments a list of tables and a configuration struct describing all of the constraints
/// between tables. After validating that there are no cycles amongst the foreign, and tree
/// dependencies, returns (i) the list of tables sorted according to their foreign key
/// dependencies, such that if table_a depends on table_b, then table_b comes before table_a in the
/// list; (ii) A map from table names to the lists of tables that depend on a given table; (iii) a
/// map from table names to the lists of tables that a given table depends on.
pub fn verify_table_deps_and_sort(
    table_list: &Vec<String>,
    constraints: &ValveConstraintConfig,
) -> (
    Vec<String>,
    HashMap<String, Vec<String>>,
    HashMap<String, Vec<String>>,
) {
    fn get_cycles(g: &DiGraphMap<&str, ()>) -> Result<Vec<String>, Vec<Vec<String>>> {
        let mut cycles = vec![];
        match toposort(&g, None) {
            Err(cycle) => {
                let problem_node = cycle.node_id();
                let neighbours = g.neighbors_directed(problem_node, Direction::Outgoing);
                for neighbour in neighbours {
                    let ways_to_problem_node =
                        all_simple_paths::<Vec<_>, _>(&g, neighbour, problem_node, 0, None);
                    for mut way in ways_to_problem_node {
                        let mut cycle = vec![problem_node];
                        cycle.append(&mut way);
                        let cycle = cycle
                            .iter()
                            .map(|&item| item.to_string())
                            .collect::<Vec<_>>();
                        cycles.push(cycle);
                    }
                }
                Err(cycles)
            }
            Ok(sorted) => {
                let mut sorted = sorted
                    .iter()
                    .map(|&item| item.to_string())
                    .collect::<Vec<_>>();
                sorted.reverse();
                Ok(sorted)
            }
        }
    }

    // Check for intra-table cycles:
    let trees = &constraints.tree;
    for table_name in table_list {
        let mut dependency_graph = DiGraphMap::<&str, ()>::new();
        let table_trees = trees
            .get(table_name)
            .expect(&format!("Undefined table '{}'", table_name));
        for tree in table_trees {
            let child = &tree.child;
            let parent = &tree.parent;
            let c_index = dependency_graph.add_node(&child);
            let p_index = dependency_graph.add_node(&parent);
            dependency_graph.add_edge(c_index, p_index, ());
        }
        match get_cycles(&dependency_graph) {
            Ok(_) => (),
            Err(cycles) => {
                let mut message = String::new();
                for cycle in cycles {
                    message.push_str(
                        format!("Cyclic dependency in table '{}': ", table_name).as_str(),
                    );
                    let end_index = cycle.len() - 1;
                    for (i, child) in cycle.iter().enumerate() {
                        if i < end_index {
                            let dep = table_trees.iter().find(|d| d.child == *child).unwrap();
                            let parent = &dep.parent;
                            message.push_str(
                                format!("tree({}) references {}", child, parent).as_str(),
                            );
                        }
                        if i < (end_index - 1) {
                            message.push_str(" and ");
                        }
                    }
                    message.push_str(". ");
                }
                panic!("{}", message);
            }
        };
    }

    // Check for inter-table cycles:
    let foreign_keys = &constraints.foreign;
    let mut dependency_graph = DiGraphMap::<&str, ()>::new();
    for table_name in table_list {
        let t_index = dependency_graph.add_node(table_name);
        let fkeys = foreign_keys
            .get(table_name)
            .expect(&format!("Undefined table '{}'", table_name));
        for fkey in fkeys {
            let ftable = &fkey.ftable;
            let f_index = dependency_graph.add_node(&ftable);
            dependency_graph.add_edge(t_index, f_index, ());
        }
    }

    match get_cycles(&dependency_graph) {
        Ok(sorted_table_list) => {
            let mut table_dependencies_in = HashMap::new();
            for node in dependency_graph.nodes() {
                let neighbors = dependency_graph
                    .neighbors_directed(node, petgraph::Direction::Incoming)
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>();
                table_dependencies_in.insert(node.to_string(), neighbors);
            }
            let mut table_dependencies_out = HashMap::new();
            for node in dependency_graph.nodes() {
                let neighbors = dependency_graph
                    .neighbors_directed(node, petgraph::Direction::Outgoing)
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>();
                table_dependencies_out.insert(node.to_string(), neighbors);
            }
            let mut sorted_table_list = sorted_table_list.clone();
            let mut with_internals = INTERNAL_TABLES
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>();
            with_internals.append(&mut sorted_table_list);
            return (
                with_internals,
                table_dependencies_in,
                table_dependencies_out,
            );
        }
        Err(cycles) => {
            let mut message = String::new();
            for cycle in cycles {
                message.push_str(
                    format!("Cyclic dependency between tables {}: ", cycle.join(", ")).as_str(),
                );
                let end_index = cycle.len() - 1;
                for (i, table) in cycle.iter().enumerate() {
                    if i < end_index {
                        let dep_name = cycle.get(i + 1).unwrap().as_str();
                        let fkeys = foreign_keys.get(table).unwrap();
                        let column;
                        let ref_table;
                        let ref_column;
                        if let Some(dep) = fkeys.iter().find(|d| d.ftable == *dep_name) {
                            column = &dep.column;
                            ref_table = &dep.ftable;
                            ref_column = &dep.fcolumn;
                        } else {
                            panic!("{}. Unable to retrieve the details.", message);
                        }

                        message.push_str(
                            format!(
                                "{}.{} depends on {}.{}",
                                table, column, ref_table, ref_column,
                            )
                            .as_str(),
                        );
                    }
                    if i < (end_index - 1) {
                        message.push_str(" and ");
                    }
                }
                message.push_str(". ");
            }
            panic!("{}", message);
        }
    };
}

/// Given a global configuration struct, a map of compiled datatype conditions, and the name of a
/// datatype, returns the configuration information for all of the datatype's ancestors if
/// only_conditioned is set to false, otherwise returns the configuration information only for
/// those ancestors that define a datatype condition. Note that this function will panic if
/// the datatype name is not defined in the given config.
pub fn get_datatype_ancestors(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    dt_name: &str,
    only_conditioned: bool,
) -> Vec<ValveDatatypeConfig> {
    fn build_hierarchy(
        config: &ValveConfig,
        datatype_conditions: &HashMap<String, CompiledCondition>,
        start_dt_name: &str,
        dt_name: &str,
        only_conditioned: bool,
    ) -> Vec<ValveDatatypeConfig> {
        let mut datatypes = vec![];
        if dt_name != "" {
            let datatype = config
                .datatype
                .get(dt_name)
                .expect(&format!("Undefined datatype '{}'", dt_name));
            let dt_name = datatype.datatype.as_str();
            let dt_condition = datatype_conditions.get(dt_name);
            let dt_parent = datatype.parent.as_str();
            if dt_name != start_dt_name {
                if !only_conditioned {
                    datatypes.push(datatype.clone());
                } else {
                    if let Some(_) = dt_condition {
                        datatypes.push(datatype.clone());
                    }
                }
            }
            let mut more_datatypes = build_hierarchy(
                config,
                datatype_conditions,
                start_dt_name,
                dt_parent,
                only_conditioned,
            );
            datatypes.append(&mut more_datatypes);
        }
        datatypes
    }
    build_hierarchy(
        config,
        datatype_conditions,
        dt_name,
        dt_name,
        only_conditioned,
    )
}

/// Given a global config struct, return a list of defined datatype names such that:
/// - if datatype_1 is an ancestor of datatype_2, datatype_1 appears before datatype_2 in the list
/// - if datatype_1 is not an ancestor of datatype_2, then their relative order in the list is
///   arbitrary (unless otherwise constrained by their relations to other datatypes in the lsit).
/// Note that this function will panic if circular dependencies are encountered.
pub fn get_sorted_datatypes(config: &ValveConfig) -> Vec<&str> {
    let mut graph = DiGraphMap::<&str, ()>::new();
    let dt_config = &config.datatype;
    for (dt_name, dt_obj) in dt_config.iter() {
        let d_index = graph.add_node(dt_name);
        if dt_obj.parent != "" {
            let p_index = graph.add_node(&dt_obj.parent);
            graph.add_edge(d_index, p_index, ());
        }
    }

    let mut cycles = vec![];
    match toposort(&graph, None) {
        Err(cycle) => {
            let problem_node = cycle.node_id();
            let neighbours = graph.neighbors_directed(problem_node, Direction::Outgoing);
            for neighbour in neighbours {
                let ways_to_problem_node =
                    all_simple_paths::<Vec<_>, _>(&graph, neighbour, problem_node, 0, None);
                for mut way in ways_to_problem_node {
                    let mut cycle = vec![problem_node];
                    cycle.append(&mut way);
                    let cycle = cycle
                        .iter()
                        .map(|&item| item.to_string())
                        .collect::<Vec<_>>();
                    cycles.push(cycle);
                }
            }
            panic!(
                "Defined datatypes contain circular dependencies: {:?}",
                cycles
            );
        }
        Ok(mut sorted) => {
            sorted.reverse();
            sorted
        }
    }
}

/// Given a column configuration map, a label, and a table name, return the column name
/// corresponding to the label.
pub fn get_column_for_label(
    column_config_map: &HashMap<String, ValveColumnConfig>,
    label: &str,
    table: &str,
) -> Result<String> {
    let column_name = column_config_map
        .iter()
        .filter(|(_, v)| v.label == *label)
        .map(|(k, _)| k)
        .collect::<Vec<_>>();
    let number_of_matches = column_name.len();

    if number_of_matches == 1 {
        Ok(column_name[0].to_string())
    } else if number_of_matches > 1 {
        Err(ValveError::ConfigError(format!(
            "Error while reading column configuration for '{}': \
             Labels must be unique.",
            table,
        ))
        .into())
    } else {
        // If we cannot find the column corresponding to the label, then we conclude that the label
        // name we received was the column name of a column with an empty label. This is because an
        // an empty label signifies, not that the column has no label, but that the column's label
        // is the same as it's column name.
        Ok(label.to_string())
    }
}

/// Given a column configuration map, a column and a table, return the label corresponding to the
/// column.
pub fn get_label_for_column(
    column_config_map: &HashMap<String, ValveColumnConfig>,
    column: &str,
    table: &str,
) -> Result<String> {
    let label_name = column_config_map
        .iter()
        .filter(|(k, _)| *k == column)
        .map(|(_, v)| v.label.to_string())
        .collect::<Vec<_>>();
    let num_matches = label_name.len();

    if num_matches > 1 {
        Err(ValveError::ConfigError(format!(
            "Error while reading column configuration for '{}': \
             Columns must be unique.",
            table,
        ))
        .into())
    } else if num_matches == 0 {
        Err(ValveError::ConfigError(format!(
            "Error while reading column configuration for '{}': \
             Column not found.",
            table,
        ))
        .into())
    } else if label_name[0] != "" {
        Ok(label_name[0].to_string())
    } else {
        // If the label is empty then just return the column name:
        Ok(column.to_string())
    }
}

/// Given a table name, a column name, and a new name for the column, rename the column to the
/// new name in the database table and associated views, using the given database transaction.
pub async fn rename_column_tx(
    valve: &Valve,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    column: &str,
    new_name: &str,
    new_label: &Option<String>,
) -> Result<()> {
    // Update the `structure` column:
    let mut structure_params = vec![];
    let mut where_params = vec![table];
    let structure_sql = {
        let mut sql_lines = vec![];
        // from() structures that refer to the renamed column:
        for (fkey_table, fkeys) in valve.config.constraint.foreign.iter() {
            for fkey in fkeys {
                if fkey.ftable == table && fkey.fcolumn == column {
                    // Add the table that has a foreign key referencing the renamed column to
                    // the list of tables that need to be updated:
                    where_params.push(fkey_table);
                    sql_lines.push(format!(
                        r#"WHEN "structure" = {SQL_PARAM} THEN {SQL_PARAM}"#
                    ));
                    structure_params.push(fkey.original.to_string());
                    let new_name = {
                        if new_name.contains(char::is_whitespace) {
                            &format!("'{new_name}'")
                        } else {
                            new_name
                        }
                    };
                    structure_params
                        .push(format!("from({ftable}.{new_name})", ftable = fkey.ftable));
                }
            }
        }

        // tree() structures that refer to the renamed column:
        for tkey in valve
            .config
            .constraint
            .tree
            .get(table)
            .expect("No tree found for table")
        {
            if tkey.child.as_str() == column {
                sql_lines.push(format!(
                    r#"WHEN "structure" = {SQL_PARAM} THEN {SQL_PARAM}"#
                ));
                structure_params.push(tkey.original.to_string());
                let new_name = {
                    if new_name.contains(char::is_whitespace) {
                        &format!("'{new_name}'")
                    } else {
                        new_name
                    }
                };
                let new_cond = format!("tree({new_name})");
                structure_params.push(new_cond);
            }
        }

        // Only actually build the structure update clause if there were matching conditions:
        if !sql_lines.is_empty() {
            format!(
                r#""structure" = CASE
                         {case_clause}
                         ELSE "structure"
                       END, "#,
                case_clause = sql_lines.join("\n")
            )
        } else {
            "".to_string()
        }
    };

    let where_sql = format!(
        r#"WHERE "table" IN ({})"#,
        where_params
            .iter()
            .map(|_| SQL_PARAM)
            .collect::<Vec<_>>()
            .join(", "),
    );

    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"UPDATE "{column_table}"
               SET {structure_sql}
                   "column" = CASE WHEN "column" = {SQL_PARAM} THEN {SQL_PARAM}
                                   ELSE "column"
                              END,
                    "label" = CASE WHEN "column" = {SQL_PARAM} THEN {SQL_PARAM}
                                   ELSE "label"
                              END
               {where_sql}"#,
            column_table = &valve.config.special.column,
        ),
    );

    let mut query = sqlx_query(&sql);
    for param in &structure_params {
        query = query.bind(param);
    }
    query = query
        .bind(column)
        .bind(new_name)
        .bind(column)
        .bind(new_label);
    for param in &where_params {
        query = query.bind(param);
    }
    query.execute(tx.acquire().await?).await?;

    // Now update the when_column and/or the then_column of any rules that refer to the
    // datatype:
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"UPDATE "{rule_table}"
               SET "when column" = CASE WHEN "when column" = {SQL_PARAM} THEN {SQL_PARAM}
                                        ELSE "when column"
                                   END,
                   "then column" = CASE WHEN "then column" = {SQL_PARAM} THEN {SQL_PARAM}
                                        ELSE "then column"
                                   END
               WHERE "table" = {SQL_PARAM}"#,
            rule_table = &valve.config.special.rule,
        ),
    );

    let query = sqlx_query(&sql)
        .bind(column)
        .bind(new_name)
        .bind(column)
        .bind(new_name)
        .bind(table);
    query.execute(tx.acquire().await?).await?;

    // Although PostgreSQL supports an ALTER VIEW statement, SQLite does not, so we have to
    // manually drop and recreate views in that case. For simplicitly, then, we use the manual
    // method in both cases.

    // Begin by dropping the standard and text views:
    let sql = format!(r#"DROP VIEW "{table}_text_view""#);
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;
    let sql = format!(r#"DROP VIEW "{table}_view""#);
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;

    // Next, rename the column in both the table and its corresponding conflict table:
    for suffix in ["", "_conflict"] {
        let sql =
            format!(r#"ALTER TABLE "{table}{suffix}" RENAME COLUMN "{column}" TO "{new_name}""#);
        let query = sqlx_query(&sql);
        query.execute(tx.acquire().await?).await?;
    }

    // Now recreate both views:
    let sql = get_sql_for_standard_view(table, &valve.db_kind);
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;
    // For text views, we need to indicate the change to the column name via a HashMap from the
    // old name to the new:
    let mut changes = HashMap::new();
    changes.insert(column, new_name);
    let sql = get_sql_for_text_view(
        &valve.config.table,
        table,
        None,
        Some(changes),
        &valve.db_kind,
    );
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;

    Ok(())
}

/// Given a [Valve] instance, a database transaction, the current name of a datatype, and a new
/// name, rename the datatype to the new name.
pub async fn rename_datatype_tx(
    valve: &Valve,
    tx: &mut Transaction<'_, sqlx::Any>,
    datatype: &str,
    new_name: &str,
) -> Result<(u32, u32)> {
    let cols_for_query = valve
        .config
        .table
        .get(&valve.config.special.datatype)
        .ok_or(ValveError::ConfigError(
            "Datatype table not found in config".to_string(),
        ))?
        .column
        .keys()
        .filter(|col| *col != "datatype")
        .map(|col| format!(r#""{col}""#))
        .collect::<Vec<_>>();

    let main_column_list = cols_for_query.join(", ");
    let t2_column_list = cols_for_query
        .iter()
        .map(|col| format!("t2.{col}"))
        .collect::<Vec<_>>()
        .join(", ");

    // We begin by saving the row number and row order of the datatype to be renamed:
    let (saved_rn, saved_ro) = {
        let sql = local_sql_syntax(
            &valve.db_kind,
            &format!(
                r#"SELECT "row_number", "row_order" FROM "{datatype_table}"
                   WHERE "datatype" = {SQL_PARAM}"#,
                datatype_table = &valve.config.special.datatype,
            ),
        );
        let row = sqlx_query(&sql)
            .bind(datatype)
            .fetch_one(tx.acquire().await?)
            .await?;
        let rn: i64 = row.try_get::<i64, &str>("row_number")?;
        let ro: i64 = row.try_get::<i64, &str>("row_order")?;
        (rn, ro)
    };

    // We then add a new datatype corresponding to the new name:
    let (rn, ro) = get_next_new_row_tx(&valve.db_kind, tx, &valve.config.special.datatype).await?;
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"INSERT INTO "{datatype_table}"
                   ("row_number", "row_order", "datatype", {main_column_list})
                   SELECT
                      {rn} AS row_number,
                      {ro} as row_order,
                      {SQL_PARAM} as "datatype",
                      {t2_column_list}
                     FROM "{datatype_table}" t2
                    WHERE t2."datatype" = {SQL_PARAM}"#,
            datatype_table = &valve.config.special.datatype,
        ),
    );
    let query = sqlx_query(&sql).bind(new_name).bind(datatype);
    query.execute(tx.acquire().await?).await?;

    // Now update any columns that used the old name as their datatype to the new name:
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"UPDATE "{column_table}"
                     SET "datatype" = CASE
                       WHEN "datatype" = {SQL_PARAM} THEN {SQL_PARAM}
                       ELSE "datatype"
                     END, "nulltype" = CASE
                       WHEN "nulltype" = {SQL_PARAM} THEN {SQL_PARAM}
                       ELSE "nulltype"
                     END"#,
            column_table = &valve.config.special.column,
        ),
    );
    let query = sqlx_query(&sql)
        .bind(datatype)
        .bind(new_name)
        .bind(datatype)
        .bind(new_name);
    query.execute(tx.acquire().await?).await?;

    let maybe_add_to_sql = |column: &str,
                            ccond: &CompiledCondition,
                            sql_lines: &mut Vec<String>,
                            sql_params: &mut Vec<String>| {
        if let ValueType::List(reference_dt, list_sep) = &ccond.value_type {
            if reference_dt == datatype {
                let old_condition = ccond.original.to_string();
                let new_name = {
                    if new_name.contains(char::is_whitespace) {
                        &format!("'{new_name}'")
                    } else {
                        new_name
                    }
                };
                let new_condition = format!("list({new_name}, '{list_sep}')");
                sql_lines.push(format!(r#"WHEN "{column}" = {SQL_PARAM} THEN {SQL_PARAM}"#));
                sql_params.push(old_condition);
                sql_params.push(new_condition);
            }
        }
    };

    // Look through the datatype config for any datatypes that are list() types and
    // that refer to the current datatype as their base type. Then construct a SQL CASE
    // statement to update the "condition" column of the datatype table appropriately given the
    // renaming.
    let mut condition_params = vec![];
    let condition_sql = {
        let mut sql_lines = vec![];
        for (_, dt_condition) in valve.datatype_conditions.iter() {
            maybe_add_to_sql(
                "condition",
                dt_condition,
                &mut sql_lines,
                &mut condition_params,
            );
        }
        if !sql_lines.is_empty() {
            format!(
                r#""condition" = CASE
                          {case_clause}
                          ELSE "condition"
                        END, "#,
                case_clause = sql_lines.join("\n")
            )
        } else {
            "".to_string()
        }
    };

    // In addition to the (list) conditions of any datatypes that referred to the old name,
    // any datatypes whose parents refer to the datatype by its old name need to be updated
    // as well:
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"UPDATE "{datatype_table}"
                    SET {condition_sql}"parent" = CASE
                      WHEN "parent" = {SQL_PARAM} THEN {SQL_PARAM}
                      ELSE "parent"
                    END"#,
            datatype_table = &valve.config.special.datatype,
        ),
    );
    let mut query = sqlx_query(&sql);
    for condition_param in &condition_params {
        query = query.bind(condition_param);
    }
    for parent_param in [datatype, new_name] {
        query = query.bind(parent_param);
    }

    query.execute(tx.acquire().await?).await?;

    // Now update the when_column and/or the then_column of any rules that refer to the
    // datatype:
    let mut wcond_params = vec![];
    let mut tcond_params = vec![];
    // This closure is used to generate the SET clauses for "when condition" and
    // "then condition" in the UPDATE statement for the rule table, and to populate
    // wcond_params and tcond_params:
    let get_rule_cond_sql = |column: &str, cond_params: &mut Vec<String>| {
        let mut sql_lines = vec![];
        // When/then datatype conditions that match the renamed datatype should
        // be renamed as well:
        sql_lines.push(format!(r#"WHEN "{column}" = {SQL_PARAM} THEN {SQL_PARAM}"#));
        cond_params.push(datatype.to_string());
        cond_params.push(new_name.to_string());

        // When/then datatype conditions that are of the list() type, whose reference
        // datatype corresponds to the given datatype, should also be renamed:
        for (_, column_rules) in valve.rule_conditions.iter() {
            for (_, conditions) in column_rules.iter() {
                for cond in conditions.iter() {
                    if column == "when condition" {
                        maybe_add_to_sql(column, &cond.when, &mut sql_lines, cond_params);
                    } else if column == "then condition" {
                        maybe_add_to_sql(column, &cond.then, &mut sql_lines, cond_params);
                    } else {
                        panic!("Not a recognized condition column: {column}");
                    }
                }
            }
        }

        // Only actually build the clause if there were matching conditions:
        if !sql_lines.is_empty() {
            format!(
                r#""{column}" = CASE
                          {case_clause}
                          ELSE "{column}"
                        END"#,
                case_clause = sql_lines.join("\n")
            )
        } else {
            "".to_string()
        }
    };
    let when_condition_sql = get_rule_cond_sql("when condition", &mut wcond_params);
    let then_condition_sql = get_rule_cond_sql("then condition", &mut tcond_params);
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"UPDATE "{rule_table}"
               SET {when_condition_sql}, {then_condition_sql}"#,
            rule_table = &valve.config.special.rule
        ),
    );
    let mut query = sqlx_query(&sql);
    for param in &wcond_params {
        query = query.bind(param);
    }
    for param in &tcond_params {
        query = query.bind(param);
    }
    query.execute(tx.acquire().await?).await?;

    // Now delete the old datatype name from the database:
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"DELETE FROM "{datatype_table}" WHERE "datatype" = {SQL_PARAM}"#,
            datatype_table = &valve.config.special.datatype,
        ),
    );
    let query = sqlx_query(&sql).bind(datatype);
    query.execute(tx.acquire().await?).await?;

    // Finally update the newly added datatype's row number and row order to the values
    // that we saved earlier, to complete the logical rename operation:
    let sql = format!(
        r#"UPDATE "{datatype_table}"
                  SET "row_number" = {saved_rn}, "row_order" = {saved_ro}
                WHERE "row_number" = {rn}"#,
        datatype_table = &valve.config.special.datatype,
    );
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;

    let saved_rn = saved_rn as u32;
    let saved_ro = saved_ro as u32;
    Ok((saved_rn, saved_ro))
}

/// Given a table name and a new name for the table, rename the table in the database using the
/// given database transaction.
pub async fn rename_table_tx(
    valve: &Valve,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    new_name: &str,
) -> Result<(u32, u32)> {
    // We begin by saving the row number and row order of the table to be renamed:
    let (saved_rn, saved_ro) = {
        let sql = local_sql_syntax(
            &valve.db_kind,
            &format!(
                r#"SELECT "row_number", "row_order" FROM "{table_table}"
                   WHERE "table" = {SQL_PARAM}"#,
                table_table = &valve.config.special.table,
            ),
        );
        let row = sqlx_query(&sql)
            .bind(table)
            .fetch_one(tx.acquire().await?)
            .await?;
        let rn: i64 = row.try_get::<i64, &str>("row_number")?;
        let ro: i64 = row.try_get::<i64, &str>("row_order")?;
        (rn, ro)
    };

    let cols_for_query = valve
        .config
        .table
        .get(&valve.config.special.table)
        .expect("Table table not found in config")
        .column
        .keys()
        .filter(|col| *col != "table")
        .map(|col| format!(r#""{col}""#))
        .collect::<Vec<_>>();

    let main_column_list = cols_for_query.join(", ");
    let t2_column_list = cols_for_query
        .iter()
        .map(|col| format!("t2.{col}"))
        .collect::<Vec<_>>()
        .join(", ");

    // Now insert a new row to the table table. We will update the row number and row order
    // to the saved values later:
    let (rn, ro) = get_next_new_row_tx(&valve.db_kind, tx, &valve.config.special.table).await?;
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"INSERT INTO "{table_table}" (
                   "row_number", "row_order", "table",
                   {main_column_list}
               )
               SELECT
                   {rn} AS row_number,
                   {ro} as row_order,
                   {SQL_PARAM} as "table",
                   {t2_column_list}
               FROM "{table_table}" t2
               WHERE t2."table" = {SQL_PARAM}"#,
            table_table = &valve.config.special.table,
        ),
    );
    let query = sqlx_query(&sql).bind(new_name).bind(table);
    query.execute(tx.acquire().await?).await?;

    // Update from() structures in the column table that refer to the column.
    let mut structure_params = vec![];
    let structure_sql = {
        let mut sql_lines = vec![];
        for (_, parsed) in valve.structure_conditions.iter() {
            if let Expression::Function(name, args) = &parsed.parsed {
                if name == "from" {
                    if let Expression::Field(ftable, fcolumn) = &*args[0] {
                        if ftable == table {
                            sql_lines.push(format!(
                                r#"WHEN "structure" = {SQL_PARAM} THEN {SQL_PARAM}"#
                            ));
                            structure_params.push(parsed.original.to_string());
                            let new_name = {
                                if new_name.contains(char::is_whitespace) {
                                    &format!("'{new_name}'")
                                } else {
                                    new_name
                                }
                            };
                            let new_cond = format!("from({new_name}.{fcolumn})");
                            structure_params.push(new_cond);
                        }
                    }
                }
            }
        }

        // Only actually build the clause if there were matching conditions:
        if !sql_lines.is_empty() {
            format!(
                r#", "structure" = CASE
                          {case_clause}
                          ELSE "structure"
                        END"#,
                case_clause = sql_lines.join("\n")
            )
        } else {
            "".to_string()
        }
    };

    // Update column table:
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"UPDATE "{column_table}"
                      SET "table" = CASE
                        WHEN "table" = {SQL_PARAM} THEN {SQL_PARAM}
                        ELSE "table"
                      END{structure_sql}"#,
            column_table = &valve.config.special.column,
        ),
    );
    let mut query = sqlx_query(&sql).bind(table).bind(new_name);
    for param in &structure_params {
        query = query.bind(param);
    }
    query.execute(tx.acquire().await?).await?;

    // Update rule table:
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"UPDATE "{rule_table}"
               SET "table" = {SQL_PARAM}
               WHERE "table" = {SQL_PARAM}"#,
            rule_table = &valve.config.special.rule,
        ),
    );
    let query = sqlx_query(&sql).bind(new_name).bind(table);
    query.execute(tx.acquire().await?).await?;

    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"DELETE FROM "{table_table}" WHERE "table" = {SQL_PARAM}"#,
            table_table = &valve.config.special.table
        ),
    );
    let query = sqlx_query(&sql).bind(table);
    query.execute(tx.acquire().await?).await?;

    // Finally update the newly added table's row number and row order to the values
    // that we saved earlier, to complete the logical rename operation:
    let sql = format!(
        r#"UPDATE "{table_table}"
           SET "row_number" = {saved_rn}, "row_order" = {saved_ro}
           WHERE "row_number" = {rn}"#,
        table_table = &valve.config.special.table
    );
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;

    // Although PostgreSQL supports an ALTER VIEW statement, SQLite does not, so we have to
    // manually drop and recreate views in that case. For simplicitly, then, we use the manual
    // method for both.

    // Begin by dropping the standard and text views:
    let sql = format!(r#"DROP VIEW "{table}_text_view""#);
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;
    let sql = format!(r#"DROP VIEW "{table}_view""#);
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;

    // Next, rename the table and its corresponding conflict table:
    for suffix in ["", "_conflict"] {
        let sql = format!(r#"ALTER TABLE "{table}{suffix}" RENAME TO "{new_name}{suffix}""#);
        let query = sqlx_query(&sql);
        query.execute(tx.acquire().await?).await?;
    }

    // Now recreate both views:
    let sql = get_sql_for_standard_view(new_name, &valve.db_kind);
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;
    let sql = get_sql_for_text_view(
        &valve.config.table,
        table,
        Some(new_name),
        None,
        &valve.db_kind,
    );
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;

    // Return the row number and row order in case it is useful for the caller:
    Ok((saved_rn as u32, saved_ro as u32))
}

/// Given a global config map, a map of compiled datatype conditions, a table name and a column
/// name, get the value type of the column.
pub fn get_value_type(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &str,
    column_name: &str,
) -> ValueType {
    let datatype = &config
        .table
        .get(table_name)
        .expect(&format!("No config found for table '{}'", table_name))
        .column
        .get(column_name)
        .expect(&format!(
            "No config found for column '{}' of table '{}'",
            column_name, table_name
        ))
        .datatype;
    match datatype_conditions.get(datatype) {
        None => ValueType::Single,
        Some(condition) => condition.value_type.clone(),
    }
}

/// Given a global config map and a table name, return a list of the columns from the table
/// that may potentially result in database conflicts.
pub fn get_conflict_columns(config: &ValveConfig, table_name: &str) -> Vec<String> {
    let mut conflict_columns = vec![];
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
    // We take tree-children because these imply a unique database constraint on the corresponding
    // column.
    let tree_children = config
        .constraint
        .tree
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .iter()
        .map(|t| t.child.to_string())
        .collect::<Vec<_>>();
    let foreign_sources = config
        .constraint
        .foreign
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .iter()
        .map(|t| t.column.to_string())
        .collect::<Vec<_>>();
    let foreign_targets = config
        .constraint
        .foreign
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .iter()
        .filter(|t| t.ftable == *table_name)
        .map(|t| t.fcolumn.to_string())
        .collect::<Vec<_>>();

    for key_columns in vec![
        primaries,
        uniques,
        &tree_children,
        &foreign_sources,
        &foreign_targets,
    ] {
        for column in key_columns {
            if !conflict_columns.contains(column) {
                conflict_columns.push(column.to_string());
            }
        }
    }

    conflict_columns
}

/// Given the name of a table and a database kind (PostgreSQL or SQLite), generate SQL for
/// creating a view based on the table that provides a unified representation of the normal
/// and conflict versions of the table, plus columns summarising the information associated
/// with the given table that is contained in the message and history tables.
pub fn get_sql_for_standard_view(table: &str, kind: &DbKind) -> String {
    let message_t;
    if *kind == DbKind::Postgres {
        message_t = format!(
            indoc! {r#"
                (
                  SELECT JSON_AGG(m)::TEXT FROM (
                    SELECT "column", "value", "level", "rule", "message"
                    FROM "message"
                    WHERE "table" = '{t}'
                      AND "row" = union_t."row_number"
                    ORDER BY "column", "message_id"
                  ) m
                )
            "#},
            t = table,
        );
    } else {
        message_t = format!(
            indoc! {r#"
                (
                  SELECT NULLIF(
                    JSON_GROUP_ARRAY(
                      JSON_OBJECT(
                        'column', "column",
                        'value', "value",
                        'level', "level",
                        'rule', "rule",
                        'message', "message"
                      )
                    ),
                    '[]'
                  )
                  FROM "message"
                  WHERE "table" = '{t}'
                    AND "row" = union_t."row_number"
                  ORDER BY "column", "message_id"
                )
            "#},
            t = table,
        );
    }

    let history_t;
    if *kind == DbKind::Postgres {
        history_t = format!(
            indoc! {r#"
                (
                  SELECT '[' || STRING_AGG("summary", ',') || ']'
                  FROM (
                    SELECT "summary"
                    FROM "history"
                    WHERE "table" = '{t}'
                      AND "row" = union_t."row_number"
                      AND "summary" IS DISTINCT FROM NULL
                      AND "undone_by" IS NOT DISTINCT FROM NULL
                    ORDER BY "history_id"
                  ) h
                )
            "#},
            t = table,
        );
    } else {
        history_t = format!(
            indoc! {r#"
                (
                  SELECT '[' || GROUP_CONCAT("summary") || ']'
                  FROM (
                    SELECT "summary"
                    FROM "history"
                    WHERE "table" = '{t}'
                      AND "row" = union_t."row_number"
                      AND "summary" IS NOT NULL
                      AND "undone_by" IS NULL
                    ORDER BY "history_id"
                  ) h
                )
            "#},
            t = table,
        );
    }

    let create_view_sql = format!(
        indoc! {r#"
          CREATE VIEW "{t}_view" AS
            SELECT
              union_t.*,
              {message_t} AS "message",
              {history_t} AS "history"
            FROM (
              SELECT * FROM "{t}"
              UNION ALL
              SELECT * FROM "{t}_conflict"
            ) as union_t;
        "#},
        t = table,
        message_t = message_t,
        history_t = history_t,
    );

    create_view_sql
}

/// Given the tables configuration map, the name of a table and a database connection kind,
/// generate SQL for creating a more user-friendly version of the view than the one generated by
/// [get_sql_for_standard_view()]. Unlike the standard view generated by that function, the view
/// generated by this function (called my_table_text_view) always shows all of the values (which are
/// all rendered as text) of every column in the table, even when those values contain SQL datatype
/// errors. If `base_table_change` is given, then the view will be constructed over the table
/// indicated by it, otherwise it will be built over `table`. In either case, `table` is used to
/// actually look up the table's columns in the tables configuration. Similarly, if
/// `base_column_changes` is given, which is a map from currently configured column names to the
/// "new" names that should be used when constructing the view, then use the new name for a column
/// if it appears in the map. Otherwise just use the currently configured name. Note that
/// `base_table_change` and `base_column_changes` are used in the case where there is a mismatch
/// between the configured table/column name and the name of the corresponding database
/// table/column. Such mismatches can occur when one is in the middle of the process of renaming a
/// table or column.
pub fn get_sql_for_text_view(
    tables_config: &HashMap<String, ValveTableConfig>,
    table: &str,
    base_table_change: Option<&str>,
    base_column_changes: Option<HashMap<&str, &str>>,
    kind: &DbKind,
) -> String {
    let base_table = match base_table_change {
        None => table,
        Some(name) => name,
    };

    let is_clause = if *kind == DbKind::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };

    let real_columns = &tables_config
        .get(table)
        .and_then(|t| {
            Some(
                t.column_order
                    .iter()
                    .map(|col| match &base_column_changes {
                        Some(changes) => changes
                            .get(&col.as_str())
                            .unwrap_or(&col.as_str())
                            .to_string(),
                        None => col.to_string(),
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .expect(&format!("Undefined table '{}'", table));

    // Add a second "text view" such that the datatypes of all values are TEXT and appear
    // directly in their corresponsing columns (rather than as NULLs) even when they have
    // SQL datatype errors.
    let mut inner_columns = real_columns
        .iter()
        .map(|col| {
            format!(
                r#"CASE
                     WHEN "{column}" {is_clause} NULL THEN (
                       SELECT value
                       FROM "message"
                       WHERE "row" = "row_number"
                         AND "column" = '{column}'
                         AND "table" = '{base_table}'
                       ORDER BY "message_id" DESC
                       LIMIT 1
                     )
                     ELSE {casted_column}
                   END AS "{column}""#,
                casted_column = if *kind == DbKind::Sqlite {
                    cast_column_sql_to_text(col, "non-text")
                } else {
                    format!("\"{}\"::TEXT", col)
                },
                column = col,
            )
        })
        .collect::<Vec<_>>();

    let mut outer_columns = real_columns
        .iter()
        .map(|c| format!("t.\"{}\"", c))
        .collect::<Vec<_>>();

    let inner_columns = {
        let mut v = vec![
            "row_number".to_string(),
            "row_order".to_string(),
            "message".to_string(),
            "history".to_string(),
        ];
        v.append(&mut inner_columns);
        v
    };

    let outer_columns = {
        let mut v = vec![
            "t.row_number".to_string(),
            "t.row_order".to_string(),
            "t.message".to_string(),
            "t.history".to_string(),
        ];
        v.append(&mut outer_columns);
        v
    };

    let create_view_sql = format!(
        r#"CREATE VIEW "{base_table}_text_view" AS
           SELECT {outer_columns}
           FROM (
               SELECT {inner_columns}
               FROM "{base_table}_view"
           ) t"#,
        outer_columns = outer_columns.join(", "),
        inner_columns = inner_columns.join(", "),
    );

    create_view_sql
}

/// Given a column name and a database kind, construct an SQL string to extract the value of the
/// column from its table, such that when the value of a given column is null, the query attempts to
/// extract it from the message table. Returns a String representing the SQL to retrieve the value
/// of the column. The returned String will contain two SQL placeholders, which will need to be
/// reformatted (for instance using the function [local_sql_syntax()]) in accordance with the syntax
/// accepted by the underlying database, and then bound before being executed by sqlx.
/// These placeholders represent: (1) the column name and (2) the table name.
pub fn generic_select_with_message_value(column: &str, kind: &DbKind) -> String {
    let is_clause = if *kind == DbKind::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };

    format!(
        r#"CASE
             WHEN "{column}" {is_clause} NULL THEN (
               SELECT "value"
               FROM "message"
               WHERE "row" = "row_number"
                 AND "column" = {placeholder}
                 AND "table" = {placeholder}
               ORDER BY "message_id" DESC
               LIMIT 1
             )
             ELSE {casted_column}
           END AS "{column}""#,
        casted_column = if *kind == DbKind::Sqlite {
            cast_column_sql_to_text(column, "non-text")
        } else {
            format!("\"{}\"::TEXT", column)
        },
        placeholder = SQL_PARAM,
    )
}

/// Given a table name, a global configuration map, and a database connection kind, construct an
/// SQL query that one can use to get the logical contents of the table, such that when the value
/// of a given column is null, the query attempts to extract it from the message table. Returns a
/// String representing the query and a vector with the parameters that need to be bound before the
/// string is executed against the database. Note that the string returned from this function is
/// generic and must first be put into the syntax of the managed database before being executed.
/// The function [local_sql_syntax()] is provided for this purpose.
pub fn generic_select_with_message_values(
    table: &str,
    config: &ValveConfig,
    kind: &DbKind,
) -> (String, Vec<String>) {
    let real_columns = config
        .table
        .get(table)
        .expect(&format!("Undefined table '{}'", table))
        .column
        .keys()
        .collect::<Vec<_>>();

    let mut sql_params = vec![];
    let mut inner_columns = real_columns
        .iter()
        .map(|column| {
            sql_params.append(&mut vec![column.to_string(), table.to_string()]);
            generic_select_with_message_value(column, kind)
        })
        .collect::<Vec<_>>();

    let mut outer_columns = real_columns
        .iter()
        .map(|c| format!("t.\"{}\"", c))
        .collect::<Vec<_>>();

    let inner_columns = {
        let mut v = vec!["row_number".to_string(), "message".to_string()];
        v.append(&mut inner_columns);
        v
    };

    let outer_columns = {
        let mut v = vec!["t.row_number".to_string(), "t.message".to_string()];
        v.append(&mut outer_columns);
        v
    };

    (
        format!(
            r#"SELECT {outer_columns}
                 FROM (
                   SELECT {inner_columns}
                   FROM "{table}_view"
                 ) t"#,
            outer_columns = outer_columns.join(", "),
            inner_columns = inner_columns.join(", "),
            table = table,
        ),
        sql_params,
    )
}

/// Given a [SerdeValue] of the type [SerdeValue::String], as well as the SQL type that it will
/// be converted to when it is inserted to the database (which can be a non-string), return a
/// [QueryParam] corresponding to the value.
pub fn get_query_param(value: &SerdeValue, sql_type: &str) -> QueryParam {
    let param_value = value
        .as_str()
        .expect(&format!("'{}' is not a string", value));
    match sql_type {
        "numeric" => {
            let numeric_value: f64 = param_value
                .parse()
                .expect(&format!("{param_value} is not numeric"));
            QueryParam::Numeric(numeric_value)
        }
        "integer" => {
            let integer_value: i32 = param_value
                .parse()
                .expect(&format!("{param_value} is not an integer"));
            QueryParam::Integer(integer_value)
        }
        "real" => {
            let real_value: f64 = param_value
                .parse()
                .expect(&format!("{param_value} is not a real"));
            QueryParam::Real(real_value)
        }
        _ => QueryParam::String(param_value.to_string()),
    }
}

/// Given a SQL type, return the appropriate CAST(...) statement for casting the SQL_PARAM
/// from a TEXT column.
pub fn cast_sql_param_from_text(sql_type: &str) -> String {
    let s = sql_type.to_lowercase();
    if s == "numeric" {
        format!("CAST(NULLIF({}, '') AS NUMERIC)", SQL_PARAM)
    } else if s == "integer" {
        format!("CAST(NULLIF({}, '') AS INTEGER)", SQL_PARAM)
    } else if s == "real" {
        format!("CAST(NULLIF({}, '') AS REAL)", SQL_PARAM)
    } else {
        String::from(SQL_PARAM)
    }
}

/// Given a SQL type, return the appropriate CAST(...) statement for casting the SQL_PARAM
/// to a TEXT column.
pub fn cast_column_sql_to_text(column: &str, sql_type: &str) -> String {
    if sql_type.to_lowercase() == "text" {
        format!(r#""{}""#, column)
    } else {
        format!(r#"CAST("{}" AS TEXT)"#, column)
    }
}

/// Given the config map, the name of a datatype, and a database kind used to determine
/// the database type, climb the datatype tree (as required), and return the first 'SQL type' found.
/// If there is no SQL type defined for the given datatype, return TEXT.
pub fn get_sql_type(
    dt_config: &HashMap<String, ValveDatatypeConfig>,
    datatype: &String,
    kind: &DbKind,
) -> String {
    match dt_config.get(datatype) {
        None => return "TEXT".to_string(),
        Some(datatype) if datatype.sql_type != "" => {
            return datatype.sql_type.to_string();
        }
        _ => (),
    };

    let parent_datatype = dt_config
        .get(datatype)
        .and_then(|d| Some(d.parent.to_string()))
        .expect(&format!("Undefined datatype '{}'", datatype));

    return get_sql_type(dt_config, &parent_datatype, kind);
}

/// Given the global config map, a table name, a column name, and a database kind,
/// return the column's SQL type.
pub fn get_sql_type_from_global_config(
    config: &ValveConfig,
    table: &str,
    column: &str,
    kind: &DbKind,
) -> String {
    let dt_config = &config.datatype;
    let dt = &config
        .table
        .get(table)
        .and_then(|t| t.column.get(column))
        .and_then(|c| Some(c.datatype.to_string()))
        .expect(&format!(
            "Could not determine datatype for '{}.{}'",
            table, column
        ));
    get_sql_type(dt_config, &dt, kind)
}

/// Given a SQL type and a value, return true if the value does not conform to the SQL type.
pub fn is_sql_type_error(sql_type: &str, value: &str) -> bool {
    let sql_type = sql_type.to_lowercase();
    if sql_type == "numeric" {
        // f64
        let numeric_value: Result<f64, std::num::ParseFloatError> = value.parse();
        match numeric_value {
            Ok(_) => false,
            Err(_) => true,
        }
    } else if sql_type == "integer" {
        // i32
        let integer_value: Result<i32, std::num::ParseIntError> = value.parse();
        match integer_value {
            Ok(_) => false,
            Err(_) => true,
        }
    } else if sql_type == "real" {
        // f64 (actually f32)
        let float_value: Result<f64, std::num::ParseFloatError> = value.parse();
        match float_value {
            Ok(_) => false,
            Err(_) => true,
        }
    } else {
        false
    }
}

/// Given a SQL string, possibly with unbound parameters represented by the placeholder string
/// SQL_PARAM, and given a database kind, if the kind is Sqlite, then change the syntax used
/// for unbound parameters to Sqlite syntax, which uses "?", otherwise use Postgres syntax, which
/// uses numbered parameters, i.e., $1, $2, ...
pub fn local_sql_syntax(kind: &DbKind, sql: &String) -> String {
    // Do not replace instances of SQL_PARAM if they are within quotation marks.
    let rx = Regex::new(&format!(
        r#"('[^'\\]*(?:\\.[^'\\]*)*'|"[^"\\]*(?:\\.[^"\\]*)*")|\b{}\b"#,
        SQL_PARAM
    ))
    .unwrap();

    let mut final_sql = String::from("");
    let mut pg_param_idx = 1;
    let mut saved_start = 0;
    for m in rx.find_iter(sql) {
        let this_match = &sql[m.start()..m.end()];
        final_sql.push_str(&sql[saved_start..m.start()]);
        if this_match == SQL_PARAM {
            if *kind == DbKind::Postgres {
                final_sql.push_str(&format!("${}", pg_param_idx));
                pg_param_idx += 1;
            } else {
                final_sql.push_str(&format!("?"));
            }
        } else {
            final_sql.push_str(&format!("{}", this_match));
        }
        saved_start = m.start() + this_match.len();
    }
    final_sql.push_str(&sql[saved_start..]);
    final_sql
}

/// Given a database connection kind, a database transaction, a table name, a column name, and a row
/// number, get the current value of the given column in the database.
pub async fn get_db_value_tx(
    table: &str,
    column: &str,
    row_number: &u32,
    kind: &DbKind,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<String> {
    let is_clause = if *kind == DbKind::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };
    let sql = local_sql_syntax(
        kind,
        &format!(
            r#"SELECT
                 CASE
                   WHEN "{column}" {is_clause} NULL THEN (
                     SELECT value
                     FROM "message"
                     WHERE "row" = "row_number"
                       AND "column" = {SQL_PARAM}
                       AND "table" = {SQL_PARAM}
                     ORDER BY "message_id" DESC
                     LIMIT 1
                   )
                   ELSE {casted_column}
                 END AS "{column}"
               FROM "{table}_view" WHERE "row_number" = {row_number}
            "#,
            casted_column = if *kind == DbKind::Sqlite {
                cast_column_sql_to_text(column, "non-text")
            } else {
                format!("\"{}\"::TEXT", column)
            },
        ),
    );

    let mut query = sqlx_query(&sql);
    for param in vec![column, table] {
        query = query.bind(param);
    }
    let rows = query.fetch_all(tx.acquire().await?).await?;
    if rows.len() == 0 {
        return Err(ValveError::DataError(
            format!(
                "In get_db_value_tx(). No rows found for row_number: {}",
                row_number
            )
            .into(),
        )
        .into());
    }
    let result_row = &rows[0];
    let value: &str = result_row.try_get(column).unwrap();
    Ok(value.to_string())
}

/// Given a global configuration map, a database connection kind, a database transaction, a table
/// name and a row number, get the logical contents of that row (whether or not it is valid),
/// including any messages, from the database, such that all values are cast as strings.
pub async fn get_text_row_from_db_tx(
    config: &ValveConfig,
    kind: &DbKind,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    row_number: &u32,
) -> Result<JsonRow> {
    let (sql, sql_params) = generic_select_with_message_values(table, config, kind);
    let sql = format!(
        "{} WHERE row_number = {}",
        local_sql_syntax(kind, &sql),
        row_number
    );
    let mut query = sqlx_query(&sql);
    for param in &sql_params {
        query = query.bind(param);
    }

    let rows = query.fetch_all(tx.acquire().await?).await?;
    if rows.len() == 0 {
        return Err(ValveError::DataError(
            format!(
                "In get_text_row_from_db_tx(). No rows found for row_number: {}",
                row_number
            )
            .into(),
        )
        .into());
    }
    let any_row = &rows[0];
    // The extended query returned by generic_select_with_message_values() casts all
    // column values to text, so we use "text" as the global sql type for the row's columns:
    let valve_row = ValveRow::from_any_row(config, kind, table, any_row, &Some("text"))?;
    valve_row.contents_to_rich_json()
}

/// Given a Valve configuration, the database kind, a table name, and a [JsonRow] representing
/// a row in the table, such that all of the column values are represented as strings, uses the
/// configuration to find the actual SQL types of each column in the row, and then converts the
/// value of each column from a string to the appropriate type, before returning the modified map.
pub fn correct_row_datatypes(
    config: &ValveConfig,
    db_kind: &DbKind,
    table: &str,
    row: &JsonRow,
) -> Result<JsonRow> {
    let mut corrected_row = JsonRow::new();
    for (cname, cell) in row {
        let numeric_re = Regex::new(r"^[0-9]*\.?[0-9]+$").expect("Programmer error");
        let sql_type =
            get_sql_type_from_global_config(&config, table, cname, &db_kind).to_lowercase();

        let cell = {
            let cell_err = |cell: &SerdeValue| -> ValveError {
                ValveError::DataError(format!("{:?} is not a valid cell", cell))
            };
            let mut corrected_cell = cell.as_object().ok_or(cell_err(cell))?.clone();
            let value = {
                let value = cell.get("value").ok_or(cell_err(cell))?;
                let strvalue: &str = value.as_str().ok_or(cell_err(cell))?;
                if sql_type == "text"
                    || sql_type.starts_with("varchar")
                    || !numeric_re.is_match(&strvalue)
                {
                    value.clone()
                } else if sql_type == "numeric" {
                    let value: f64 = strvalue.parse()?;
                    json!(value)
                } else if sql_type == "integer" {
                    let value: i32 = strvalue.parse()?;
                    json!(value)
                } else if sql_type == "real" {
                    let value: f64 = strvalue.parse()?;
                    json!(value)
                } else {
                    return Err(ValveError::DataError(format!(
                        "Unrecognized SQL type: {}",
                        sql_type
                    ))
                    .into());
                }
            };
            corrected_cell.insert("value".to_string(), value);
            corrected_cell
        };
        corrected_row.insert(cname.to_string(), json!(cell));
    }
    Ok(corrected_row)
}

/// Given a row and a column name, return the contents of the row as a JSON array.
pub fn get_json_array_from_column(row: &AnyRow, column: &str) -> Option<Vec<SerdeValue>> {
    let raw_value = row
        .try_get_raw(column)
        .expect("Unable to get raw value from row");
    if !raw_value.is_null() {
        let value: &str = row.get(column);
        match serde_json::from_str::<SerdeValue>(value) {
            Err(e) => {
                log::warn!("{}", e);
                None
            }
            Ok(SerdeValue::Array(value)) => Some(value),
            _ => {
                log::warn!("{} is not an array.", value);
                None
            }
        }
    } else {
        None
    }
}

/// Given a row and a column name, return the contents of the column as a JSON object.
pub fn get_json_object_from_column(row: &AnyRow, column: &str) -> Option<JsonRow> {
    let raw_value = row
        .try_get_raw(column)
        .expect("Unable to get raw value from row");
    if !raw_value.is_null() {
        let value: &str = row.get(column);
        match serde_json::from_str::<SerdeValue>(value) {
            Err(e) => {
                log::warn!("{}", e);
                None
            }
            Ok(SerdeValue::Object(value)) => Some(value),
            _ => {
                log::warn!("{} is not an object.", value);
                None
            }
        }
    } else {
        None
    }
}

/// Given a database row, the name of a column, and it's SQL type, return the value of that column
/// from the given row as a String.
pub fn get_column_value_as_string(row: &AnyRow, column: &str, sql_type: &str) -> String {
    let s = sql_type.to_lowercase();
    if s == "numeric" {
        let value: f64 = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        value.to_string()
    } else if s == "integer" {
        let value: i32 = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        value.to_string()
    } else if s == "real" {
        let value: f64 = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        value.to_string()
    } else {
        let value: &str = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        value.to_string()
    }
}

/// Given a database row, the name of a column, and it's SQL type, return the value of that column
/// from the given row as a [SerdeValue].
pub fn get_column_value(row: &AnyRow, column: &str, sql_type: &str) -> SerdeValue {
    let s = sql_type.to_lowercase();
    if s == "numeric" {
        let value: f64 = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        json!(value)
    } else if s == "integer" {
        let value: i32 = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        json!(value)
    } else if s == "real" {
        let value: f64 = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        json!(value)
    } else {
        let value: &str = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        json!(value)
    }
}

/// Given a table name, a row number, and a database transaction, return the row_order
/// corresponding to the given row number in the given table. Note that the row order is
/// represented using the signed type i64 but it will never be negative.
pub async fn get_row_order_tx(
    table: &str,
    row_number: &u32,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<i64> {
    let sql = format!(
        r#"SELECT "row_order" FROM "{}_view" WHERE "row_number" = {}"#,
        table, row_number
    );
    let rows = sqlx_query(&sql).fetch_all(tx.acquire().await?).await?;
    if rows.len() == 0 {
        return Err(ValveError::DataError(
            format!(
                "Unable to fetch row_order for row {} of table '{}'",
                row_number, table,
            )
            .into(),
        )
        .into());
    }
    Ok(rows[0].get("row_order"))
}

/// Given a table name, a row number, and a transaction through which to access the database,
/// search for and return the row number of the row that is marked as previous to the given row.
pub async fn get_previous_row_tx(
    table: &str,
    row: &u32,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<u32> {
    let curr_row_order = get_row_order_tx(table, row, tx).await?;
    let sql = format!(
        r#"SELECT "row_number"
             FROM "{}_view"
            WHERE "row_order" < {}
            ORDER BY "row_order" DESC
            LIMIT 1"#,
        table, curr_row_order
    );
    let rows = sqlx_query(&sql).fetch_all(tx.acquire().await?).await?;
    if rows.len() == 0 {
        Ok(0)
    } else {
        let rn: i64 = rows[0].get("row_number");
        Ok(rn as u32)
    }
}

/// Return the row_number and row_order that will be assigned to the next row created
/// in the given table. Uses the given transaction and the database kind to determine
/// the SQL syntax to use when querying the database.
pub async fn get_next_new_row_tx(
    db_kind: &DbKind,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
) -> Result<(u32, u32)> {
    let sql = local_sql_syntax(
        db_kind,
        &format!(
            r#"SELECT MAX("row_number") AS "row_number" FROM (
                 SELECT MAX("row_number") AS "row_number"
                   FROM "{table}_view"
                 UNION ALL
                 SELECT MAX("row") AS "row_number"
                   FROM "history"
                   WHERE "table" = {SQL_PARAM}
               ) t"#,
        ),
    );
    let query = sqlx_query(&sql).bind(table);
    let result_rows = query.fetch_all(tx.acquire().await?).await?;
    let new_row_number: i64;
    if result_rows.len() == 0 {
        new_row_number = 1;
    } else {
        let result_row = &result_rows[0];
        let result = result_row.try_get_raw("row_number")?;
        if result.is_null() {
            new_row_number = 1;
        } else {
            new_row_number = result_row.get("row_number");
        }
    }
    let new_row_number = new_row_number as u32 + 1;
    let new_row_order = new_row_number * MOVE_INTERVAL;
    Ok((new_row_number, new_row_order))
}

/// Given a table name, a row number, the new row order to assign to the row, and a database
/// transaction, update the row order for the row in the database. Note that the row_order is
/// represented using the signed i64 type but it can never actually be negative. This function
/// will return an error when a negative row_order is provided.
pub async fn update_row_order_tx(
    table: &str,
    row: &u32,
    row_order: &i64,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<()> {
    if *row_order < 0 {
        return Err(ValveError::InputError(format!(
            "Refusing to assign a negative row_order, {}, to row {} of table {}.",
            row_order, row, table
        ))
        .into());
    }

    let sql = format!(
        r#"UPDATE "{}" SET "row_order" = {} WHERE "row_number" = {} RETURNING 1 AS "updated""#,
        table, row_order, row,
    );
    let rows = sqlx_query(&sql).fetch_all(tx.acquire().await?).await?;
    if rows.len() == 0 {
        // If the update of the normal table yielded zero affected rows, then the row must be
        // in the conflict table, so we try updating that instead:
        let sql = format!(
            r#"UPDATE "{}_conflict" SET "row_order" = {} WHERE "row_number" = {}"#,
            table, row_order, row,
        );
        sqlx_query(&sql).execute(tx.acquire().await?).await?;
    }
    Ok(())
}

/// Given a database transaction, a table name, `table`, a row number, `row`, and the number of the
/// row, `previous_row`, representing the row number that will come immediately before `row` in the
/// ordering of rows after the move has been completed: Set the `row_order` field corresponding to
/// `row` in the database so that `row` comes immediately after `previous_row` in the ordering of
/// rows.
pub async fn move_row_tx(
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    row: &u32,
    previous_row: &u32,
) -> Result<()> {
    // Get the row order, (A), of `previous_row`:
    let row_order_prev = {
        if *previous_row > 0 {
            get_row_order_tx(table, previous_row, tx).await?
        } else {
            // It is not possible for a row to be assigned a row number of zero. We allow
            // it as a possible value of `previous_row`, however, which is used as a special value
            // signifying that `row` should become the very first row in the table ordering. We
            // represent this by assigning it the row order 0
            0
        }
    };

    // Run a query to get the minimum row order, (B), that is greater than (A).
    let row_order_next = {
        let sql = format!(
            r#"SELECT MIN("row_order") AS "row_order" FROM "{}_view" WHERE "row_order" > {}"#,
            table, row_order_prev
        );
        let result_row = sqlx_query(&sql).fetch_one(tx.acquire().await?).await?;
        let raw_row_order = result_row.try_get_raw("row_order")?;
        if raw_row_order.is_null() {
            // The raw_row_order will be null if we ask Valve to move a row to a position after
            // the last row in the table.
            row_order_prev + MOVE_INTERVAL as i64
        } else {
            result_row.get("row_order")
        }
    };

    let new_row_order = {
        if row_order_prev + 1 < row_order_next {
            // If the next row_order is not occupied just use it:
            row_order_prev + 1
        } else {
            // Otherwise, get all the row_orders that need to be moved. We sort the results in
            // descending order so that when we later update each value, no duplicate key
            // violations will ensue:
            let upper_bound =
                (row_order_next as f32 / MOVE_INTERVAL as f32).ceil() as i64 * MOVE_INTERVAL as i64;
            let sql = format!(
                r#"SELECT "row_order"
                   FROM "{}_view"
                   WHERE "row_order" >= {} AND "row_order" < {}
                   ORDER BY "row_order" DESC"#,
                table, row_order_next, upper_bound
            );
            let rows = sqlx_query(&sql).fetch_all(tx.acquire().await?).await?;
            let highest_row_order: i64 = rows[0].get("row_order");
            if highest_row_order + 1 >= upper_bound {
                // Return an error
                return Err(ValveError::DataError(format!(
                    "Impossible to move row {} after row {}: No more room",
                    row, previous_row
                ))
                .into());
            }
            for row in rows {
                let current_row_order: i64 = row.get("row_order");
                for table in vec![table.to_string(), format!("{}_conflict", table)] {
                    let sql = format!(
                        r#"UPDATE "{}"
                           SET "row_order" = "row_order" + 1
                           WHERE "row_order" = {}"#,
                        table, current_row_order
                    );
                    sqlx_query(&sql).execute(tx.acquire().await?).await?;
                }
            }
            // Now that we have made some room, we can use row_order_prev + 1,
            // which should no longer be occupied:
            row_order_prev + 1
        }
    };

    update_row_order_tx(table, row, &new_row_order, tx).await?;
    Ok(())
}

/// Given a global config struct, compiled datatype and rule conditions, a database connection pool,
/// a database transaction, a table name, a row represented as a [ValveRow],
/// insert it to the database using the given transaction, then return the new row number. If
/// the optional parameter `new_row_number` is given, then that is the row number used when
/// creating the row, and is the number returned. Otherwise the row number inserted is determined
/// automatically by Valve. If skip_validation is set to true, omit the implicit call to
/// [validate_row_tx()].
#[async_recursion]
pub async fn insert_new_row_tx(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<sqlx::Any>,
    table: &str,
    row: &ValveRow,
    skip_validation: bool,
    do_not_recurse: bool,
) -> Result<u32> {
    // Send the row through the row validator to determine if any fields are problematic and
    // to mark them with appropriate messages:
    let row = if !skip_validation {
        validate_row_tx(
            config,
            datatype_conditions,
            rule_conditions,
            pool,
            Some(tx),
            table,
            &row,
            None,
        )
        .await?
    } else {
        row.clone()
    };

    // Now prepare the row and messages for insertion to the database.
    let new_row_number = match row.row_number {
        Some(rn) => rn,
        None => {
            let (rn, _) = get_next_new_row_tx(&DbKind::from_pool(pool)?, tx, table).await?;
            rn
        }
    };

    let mut insert_columns = vec![];
    let mut insert_values = vec![];
    let mut insert_params = vec![];
    let mut all_messages = vec![];
    let sorted_datatypes = get_sorted_datatypes(config);
    let conflict_columns = get_conflict_columns(config, table);
    let mut use_conflict_table = false;
    for (column, cell) in row.contents.iter() {
        insert_columns.append(&mut vec![format!(r#""{}""#, column)]);
        let messages = sort_messages(&sorted_datatypes, &cell.messages);
        for message in messages {
            all_messages.push(json!({
                "column": column,
                "value": &cell.value,
                "level": message.level,
                "rule": message.rule,
                "message": message.message,
            }));
        }

        let sql_type =
            get_sql_type_from_global_config(config, table, column, &DbKind::from_pool(pool)?);
        if cell.nulltype != None || is_sql_type_error(&sql_type, &cell.strvalue()) {
            insert_values.push(String::from("NULL"));
        } else {
            insert_values.push(cast_sql_param_from_text(&sql_type));
            insert_params.push(String::from(cell.strvalue()));
        }

        if !use_conflict_table && !cell.valid && conflict_columns.contains(&column) {
            use_conflict_table = true;
        }
    }

    // Used to validate the given row, counterfactually, "as if" the version of the row in the
    // database currently were replaced with `row`:
    let query_as_if = QueryAsIf {
        kind: QueryAsIfKind::Add,
        table: table.to_string(),
        alias: format!("{}_as_if", table),
        row_number: new_row_number,
        row: Some(row.clone()),
    };

    // Look through the valve config to see which tables are dependent on this table
    // and find the rows that need to be updated:
    let (_, updates_tree, _, updates_after) = {
        if do_not_recurse {
            (
                IndexMap::new(),
                IndexMap::new(),
                IndexMap::new(),
                IndexMap::new(),
            )
        } else {
            get_rows_to_update_tx(config, &DbKind::from_pool(pool)?, tx, table, &query_as_if)
                .await?
        }
    };

    // Check it to see if the row should be redirected to the conflict table:
    let table_to_write = {
        if use_conflict_table {
            format!("{}_conflict", table)
        } else {
            String::from(table)
        }
    };

    // Add the new row to the table:
    let insert_stmt = local_sql_syntax(
        &DbKind::from_pool(pool)?,
        &format!(
            r#"INSERT INTO "{table}" ("row_number", "row_order", {data_columns})
               VALUES ({row_number}, {row_order}, {data_values})"#,
            table = table_to_write,
            data_columns = insert_columns.join(", "),
            row_number = new_row_number,
            row_order = new_row_number * MOVE_INTERVAL,
            data_values = insert_values.join(", "),
        ),
    );
    let mut query = sqlx_query(&insert_stmt);
    for param in &insert_params {
        query = query.bind(param);
    }
    query.execute(tx.acquire().await?).await?;

    // Next add any validation messages to the message table:
    for m in all_messages {
        let column = m.get("column").and_then(|c| c.as_str()).unwrap();
        let value = match m.get("value") {
            Some(SerdeValue::String(s)) => s.to_string(),
            Some(v) => format!("{}", v),
            _ => panic!("Message '{}' has no value", m),
        };
        let level = m.get("level").and_then(|c| c.as_str()).unwrap();
        let rule = m.get("rule").and_then(|c| c.as_str()).unwrap();
        let message = m.get("message").and_then(|c| c.as_str()).unwrap();
        let message_sql = local_sql_syntax(
            &DbKind::from_pool(pool)?,
            &format!(
                r#"INSERT INTO "message"
                   ("table", "row", "column", "value", "level", "rule", "message")
                   VALUES ({SQL_PARAM}, {new_row_number}, {SQL_PARAM}, {SQL_PARAM},
                           {SQL_PARAM}, {SQL_PARAM}, {SQL_PARAM})"#,
            ),
        );
        let mut query = sqlx_query(&message_sql);
        for param in [table, column, &value, level, rule, &message] {
            query = query.bind(param);
        }
        query.execute(tx.acquire().await?).await?;
    }

    // Now process the updates that need to be performed because of an insertion to a tree
    // column:
    process_updates_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        &updates_tree,
        &query_as_if,
        true,
    )
    .await?;

    // Now process the updates that need to be performed after the update of the target row:
    process_updates_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        &updates_after,
        &query_as_if,
        false,
    )
    .await?;

    Ok(new_row_number)
}

/// Given a global config map, maps of datatype and rule conditions, a database connection pool, a
/// database transaction, a table name, and a row number, delete the given row from the database.
#[async_recursion]
pub async fn delete_row_tx(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<sqlx::Any>,
    table: &str,
    row_number: &u32,
) -> Result<()> {
    // Used to validate the given row, counterfactually, "as if" the row did not exist in the
    // database:
    let query_as_if = QueryAsIf {
        kind: QueryAsIfKind::Remove,
        table: table.to_string(),
        alias: format!("{}_as_if", table),
        row_number: *row_number,
        row: None,
    };

    // Look through the valve config to see which tables are dependent on this table and find the
    // rows that need to be updated. Since this is a delete there will only be rows to update
    // before and none after the delete:
    let (updates_before, updates_tree, updates_unique, _) =
        get_rows_to_update_tx(config, &DbKind::from_pool(pool)?, tx, table, &query_as_if).await?;

    // Process the updates that need to be performed before the update of the target row:
    process_updates_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        &updates_before,
        &query_as_if,
        false,
    )
    .await?;

    // Now delete the row:
    let sql1 = format!(
        "DELETE FROM \"{}\" WHERE row_number = {}",
        table, row_number,
    );
    let sql2 = format!(
        "DELETE FROM \"{}_conflict\" WHERE row_number = {}",
        table, row_number
    );
    for sql in vec![sql1, sql2] {
        let query = sqlx_query(&sql);
        query.execute(tx.acquire().await?).await?;
    }

    let sql = local_sql_syntax(
        &DbKind::from_pool(pool)?,
        &format!(r#"DELETE FROM "message" WHERE "table" = {SQL_PARAM} AND "row" = {row_number}"#,),
    );
    let query = sqlx_query(&sql).bind(table);
    query.execute(tx.acquire().await?).await?;

    // Now process the updates that need to be performed because of an insertion to a tree
    // column:
    process_updates_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        &updates_tree,
        &query_as_if,
        true,
    )
    .await?;

    // Finally process the rows from the same table as the target table that need to be re-validated
    // because of unique or primary constraints:
    process_updates_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        &updates_unique,
        &query_as_if,
        true,
    )
    .await?;

    Ok(())
}

/// Given global config struct, maps of compiled datatype and rule conditions, a database connection
/// pool, a database transaction, a table name, a row represented as a [ValveRow],
/// and the row number to update, update the corresponding row in the database. If skip_validation
/// is set, skip the implicit call to [validate_row_tx()]. If do_not_recurse is set, do not look for
/// rows which could be affected by this update.
#[async_recursion]
pub async fn update_row_tx(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<sqlx::Any>,
    table: &str,
    row: &ValveRow,
    skip_validation: bool,
    do_not_recurse: bool,
) -> Result<()> {
    // First, look through the valve config to see which tables are dependent on this table and find
    // the rows that need to be updated. The variable query_as_if is used to validate the given row,
    // counterfactually, "as if" the version of the row in the database currently were replaced with
    // `row`:
    let row_number = row
        .row_number
        .ok_or(ValveError::InputError("Row has no row number".to_string()))?;
    let query_as_if = QueryAsIf {
        kind: QueryAsIfKind::Replace,
        table: table.to_string(),
        alias: format!("{}_as_if", table),
        row_number: row_number,
        row: Some(row.clone()),
    };
    let (updates_before, updates_tree, updates_unique, updates_after) = {
        if do_not_recurse {
            (
                IndexMap::new(),
                IndexMap::new(),
                IndexMap::new(),
                IndexMap::new(),
            )
        } else {
            get_rows_to_update_tx(config, &DbKind::from_pool(pool)?, tx, table, &query_as_if)
                .await?
        }
    };

    // Process the updates that need to be performed before the update of the target row:
    process_updates_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        &updates_before,
        &query_as_if,
        false,
    )
    .await?;

    // Send the row through the row validator to determine if any fields are problematic and
    // to mark them with appropriate messages:
    let row = if !skip_validation {
        let mut row = row.clone();
        row.row_number = Some(row_number);
        validate_row_tx(
            config,
            datatype_conditions,
            rule_conditions,
            pool,
            Some(tx),
            table,
            &row,
            None,
        )
        .await?
    } else {
        row.clone()
    };

    // Get the previous row number for the given row:
    let old_previous_rn = get_previous_row_tx(table, &row_number, tx).await?;

    // Perform the update in two steps:
    delete_row_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        table,
        &row_number,
    )
    .await?;
    insert_new_row_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        table,
        &row,
        false,
        do_not_recurse,
    )
    .await?;

    // Move the row back to the position it was in before it was deleted:
    move_row_tx(tx, table, &row_number, &old_previous_rn).await?;

    // Now process the updates that need to be performed because of an insertion to a tree
    // column:
    process_updates_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        &updates_tree,
        &query_as_if,
        true,
    )
    .await?;

    // Now process the rows from the same table as the target table that need to be re-validated
    // because of unique or primary constraints:
    process_updates_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        &updates_unique,
        &query_as_if,
        true,
    )
    .await?;

    // Finally process the updates from other tables that need to be performed after the update of
    // the target row:
    process_updates_tx(
        config,
        datatype_conditions,
        rule_conditions,
        pool,
        tx,
        &updates_after,
        &query_as_if,
        false,
    )
    .await?;

    Ok(())
}

/// Given a [Valve] instance, a table name, a column name, a [JsonRow] representing the column's
/// column configuration, and a database transaction, add the given column to the given table,
/// returning the row_number and row_order for the new column entry in the column table.
pub async fn add_column_tx(
    valve: &Valve,
    table: &str,
    column: &str,
    column_details: &JsonRow,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<(u32, u32)> {
    let make_err = |err_str: &str| -> ValveError { ValveError::InputError(err_str.to_string()) };

    // A datatype is required but the other fields are optional:
    let datatype = match column_details.get("datatype") {
        Some(datatype) => datatype.as_str().ok_or(make_err("Not a string"))?,
        None => return Err(make_err("No datatype given").into()),
    };
    let label = column_details.get("label").and_then(|l| l.as_str());
    let nulltype = column_details.get("nulltype").and_then(|l| l.as_str());
    let structure = column_details.get("structure").and_then(|l| l.as_str());
    let description = column_details.get("description").and_then(|l| l.as_str());

    // Generate an insert statement and execute it:
    let mut fields = vec![r#""table""#, r#""column""#, r#""datatype""#];
    let mut placeholders = vec![SQL_PARAM, SQL_PARAM, SQL_PARAM];
    let mut field_params = vec![table, column, datatype];
    for (field, field_param) in [
        (r#""label""#, label),
        (r#""nulltype""#, nulltype),
        (r#""structure""#, structure),
        (r#""description""#, description),
    ] {
        if let Some(param) = field_param {
            fields.push(field);
            if param == "" {
                placeholders.push("NULL");
            } else {
                placeholders.push(SQL_PARAM);
                field_params.push(param);
            }
        }
    }

    let (rn, ro) = get_next_new_row_tx(&valve.db_kind, tx, &valve.config.special.column).await?;
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"INSERT INTO "{column_table}" ("row_number", "row_order", {fields})
               VALUES ({rn}, {ro}, {placeholders})"#,
            fields = fields.join(", "),
            placeholders = placeholders.join(", "),
            column_table = &valve.config.special.column,
        ),
    );
    let mut query = sqlx_query(&sql);
    for param in &field_params {
        query = query.bind(param);
    }
    query.execute(tx.acquire().await?).await?;

    Ok((rn, ro))
}

/// Given a [Valve] instance the name of a datatype, and a database transaction,
/// add the datatype to the datatype table, returning its row_number and row_order.
pub async fn add_datatype_tx(
    valve: &Valve,
    dt_map: &JsonRow,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<(u32, u32)> {
    // Separate the datatype fields into the columns and column parameters that we will
    // use to construct the INSERT statement:
    let mut columns = vec![];
    let mut placeholders = vec![];
    let mut params = vec![];
    for (column, value) in dt_map.iter() {
        // If the map contains a row_number or row_order field, we ignore it here. The caller
        // is responsible for dealing with it in case it needs to be updated.
        if !["row_number", "row_order"].contains(&column.as_str()) {
            columns.push(format!(r#""{column}""#));
            let value = value.as_str().ok_or(ValveError::InputError(
                format!("Not a string: {value}").into(),
            ))?;
            if value == "" {
                placeholders.push("NULL");
            } else {
                placeholders.push(SQL_PARAM);
                params.push(value.to_string());
            }
        }
    }

    // Generate an INSERT statement:
    let (rn, ro) = get_next_new_row_tx(&valve.db_kind, tx, &valve.config.special.datatype).await?;
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"INSERT INTO "{datatype_table}" ("row_number", "row_order", {columns})
               VALUES ({rn}, {ro}, {placeholders})"#,
            columns = columns.join(", "),
            placeholders = placeholders.join(", "),
            datatype_table = &valve.config.special.datatype,
        ),
    );

    // Insert the datatype:
    let mut query = sqlx_query(&sql);
    for param in params {
        query = query.bind(param);
    }
    query.execute(tx.acquire().await?).await?;

    Ok((rn, ro))
}

/// Given a [Valve] instance, a table and column name, and a database transaction, delete the
/// given column from the column table.
pub async fn delete_column_tx(
    valve: &Valve,
    table: &str,
    column: &str,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<(u32, u32)> {
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"DELETE FROM "{column_table}"
               WHERE "table" = {SQL_PARAM} AND "column" = {SQL_PARAM}
               RETURNING "row_number" AS "row_number", "row_order" AS "row_order""#,
            column_table = &valve.config.special.column,
        ),
    );

    let query = sqlx_query(&sql).bind(table).bind(column);
    let row = query.fetch_one(tx.acquire().await?).await?;
    let rn = row.get::<i64, _>("row_number") as u32;
    let ro = row.get::<i64, _>("row_order") as u32;

    Ok((rn, ro))
}

/// Given a [Valve] instance, a datatype name, and a database transaction, delete the given
/// datatype from the datatype table.
pub async fn delete_datatype_tx(
    valve: &Valve,
    datatype: &str,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<(u32, u32)> {
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(
            r#"DELETE FROM "{datatype_table}" WHERE "datatype" = {SQL_PARAM}
               RETURNING "row_number" AS "row_number", "row_order" AS "row_order""#,
            datatype_table = &valve.config.special.datatype,
        ),
    );
    let query = sqlx_query(&sql).bind(datatype);
    let row = query.fetch_one(tx.acquire().await?).await?;
    let rn = row.get::<i64, _>("row_number") as u32;
    let ro = row.get::<i64, _>("row_order") as u32;

    Ok((rn, ro))
}

/// Given a [Valve] instance, a database transation and a list of table names, drop all of the
/// tables in the list.
pub async fn drop_tables_tx(
    valve: &Valve,
    tables: &Vec<&str>,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<()> {
    let drop_list = valve.add_dependencies(tables, true)?;
    for table in &drop_list {
        let table_config = valve.get_table_config(table)?;
        if table_config.path != "" {
            if table_config.options.contains("conflict") {
                let sql = format!(r#"DROP VIEW IF EXISTS "{}_text_view""#, table);
                let query = sqlx_query(&sql);
                query.execute(tx.acquire().await?).await?;
                let sql = format!(r#"DROP VIEW IF EXISTS "{}_view""#, table);
                let query = sqlx_query(&sql);
                query.execute(tx.acquire().await?).await?;
                let sql = format!(r#"DROP TABLE IF EXISTS "{}_conflict""#, table);
                let query = sqlx_query(&sql);
                query.execute(tx.acquire().await?).await?;
            }
            let type_to_drop = match table_config.options.contains("db_view") {
                true => "VIEW",
                false => "TABLE",
            };
            let sql = format!(r#"DROP {} IF EXISTS "{}""#, type_to_drop, table);
            let query = sqlx_query(&sql);
            query.execute(tx.acquire().await?).await?;
        }
    }

    Ok(())
}

/// Given a [Valve] instance, a table name, a database transaction and the database kind, delete
/// the given table from both the table table and the column table.
pub async fn delete_table_tx(
    valve: &Valve,
    table: &str,
    tx: &mut Transaction<'_, sqlx::Any>,
    db_kind: &DbKind,
) -> Result<()> {
    // Drop the table:
    drop_tables_tx(valve, &vec![table], tx).await?;

    // Delete it from the column table:
    let sql = local_sql_syntax(
        db_kind,
        &format!(
            r#"DELETE FROM "{column_table}" WHERE "table" = {SQL_PARAM}"#,
            column_table = &valve.config.special.column
        ),
    );
    let query = sqlx_query(&sql).bind(table);
    query.execute(tx.acquire().await?).await?;

    // Delete it from tbe table table:
    let sql = local_sql_syntax(
        db_kind,
        &format!(
            r#"DELETE FROM "{table_table}" WHERE "table" = {SQL_PARAM}"#,
            table_table = &valve.config.special.table
        ),
    );
    let query = sqlx_query(&sql).bind(table);
    query.execute(tx.acquire().await?).await?;

    Ok(())
}

/// Given a global configuration struct, a database connection pool used to determine the SQL syntax
/// to use, a database transaction through which to execute queries over the database, a table name,
/// a row number, the old previous row for the row, and the new previous row for the row, record the
/// move in the history table in the database.
pub async fn record_row_move_tx(
    config: &ValveConfig,
    kind: &DbKind,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    row_num: &u32,
    old_previous_row: &u32,
    new_previous_row: &u32,
    user: &str,
) -> Result<()> {
    let row = get_text_row_from_db_tx(config, kind, tx, table, row_num).await?;
    let mut from_row = row.clone();
    let mut to_row = row.clone();
    from_row.insert("previous_row".to_string(), json!(old_previous_row));
    to_row.insert("previous_row".to_string(), json!(new_previous_row));
    record_row_change_tx(
        kind,
        tx,
        table,
        row_num,
        Some(&from_row),
        Some(&to_row),
        &user,
    )
    .await
}

/// Given a database connection pool (used to determine the type of the underlying database), a
/// database transaction, a table name, a row number, optionally: the version of the row we
/// are going to change it from, optionally: the version of the row we are going to change it to,
/// and the name of the user making the change, record the change to the history table in the
/// database. Note that `from` and `to` cannot both be None: Either we are changing the row from
/// something to nothing (i.e., deleting), changing it from nothing to something (i.e., inserting),
/// or changing it from something to something else (i.e., updating).
/// Note that `from` and `to` must both be in the ('rich') format:
/// ```
/// {
///     "column_1": {
///         "valid": <true|false>,
///         "messages": [{"level": level, "rule": rule, "message": message}, ...],
///         "value": value1
///     },
///     "column_2": {
///         "valid": <true|false>,
///         "messages": [{"level": level, "rule": rule, "message": message}, ...],
///         "value": value2
///     },
///     ...
/// },
/// ```
pub async fn record_row_change_tx(
    kind: &DbKind,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    row_number: &u32,
    from: Option<&JsonRow>,
    to: Option<&JsonRow>,
    user: &str,
) -> Result<()> {
    if let (None, None) = (from, to) {
        return Err(ValveError::InputError(
            "Arguments 'from' and 'to' to record_row_change_tx() cannot both be None".into(),
        )
        .into());
    }

    fn to_text(row: Option<&JsonRow>, omit_redundant_fields: bool) -> String {
        match row {
            None => "NULL".to_string(),
            Some(row) => {
                let mut row = row.clone();
                if omit_redundant_fields {
                    row.remove("column");
                    row.remove("value");
                }
                format!("{}", json!(row))
            }
        }
    }

    fn format_value(value: &String, numeric_re: &Regex) -> String {
        if numeric_re.is_match(value) {
            value.to_string()
        } else {
            format!("'{}'", value)
        }
    }

    fn summarize(from: Option<&JsonRow>, to: Option<&JsonRow>) -> Result<String> {
        // Constructs a summary of the form:
        // [{
        //    "column":"bar",
        //    "level":"update|move",
        //    "message":"Value changed from 'A' to 'B'",
        //    "old_value":"'A'",
        //    "value":"'B'"
        //  },
        //  ...
        // ]
        let mut summary = vec![];
        match (from, to) {
            (None, _) | (_, None) => Ok("NULL".to_string()),
            (Some(from), Some(to)) => {
                let numeric_re = Regex::new(r"^[0-9]*\.?[0-9]+$")?;
                for (column, cell) in from.iter() {
                    let old_value = match column.as_str() {
                        "previous_row" => match cell {
                            SerdeValue::Number(n) => format!("{}", n),
                            _ => {
                                return Err(ValveError::DataError(format!(
                                    "Value {:?} for column '{}' is not a number",
                                    cell, column,
                                ))
                                .into())
                            }
                        },
                        _ => cell
                            .get("value")
                            .and_then(|v| match v {
                                SerdeValue::String(s) => Some(format!("{}", s)),
                                SerdeValue::Number(n) => Some(format!("{}", n)),
                                SerdeValue::Bool(b) => Some(format!("{}", b)),
                                _ => None,
                            })
                            .ok_or(ValveError::DataError(
                                format!("No value in {}", cell).into(),
                            ))?,
                    };
                    let new_value = match column.as_str() {
                        "previous_row" => to
                            .get(column)
                            .and_then(|v| match v {
                                SerdeValue::Number(n) => Some(format!("{}", n)),
                                _ => None,
                            })
                            .ok_or(ValveError::DataError(
                                format!(
                                    "Value of column: '{}' in row {:?} is not a number",
                                    column, to
                                )
                                .into(),
                            ))?,
                        _ => to
                            .get(column)
                            .and_then(|v| v.get("value"))
                            .and_then(|v| match v {
                                SerdeValue::String(s) => Some(format!("{}", s)),
                                SerdeValue::Number(n) => Some(format!("{}", n)),
                                SerdeValue::Bool(b) => Some(format!("{}", b)),
                                _ => None,
                            })
                            .ok_or(ValveError::DataError(
                                format!("No value for column: {} in {:?}", column, to).into(),
                            ))?,
                    };
                    if new_value != old_value {
                        let mut column_summary = JsonRow::new();
                        column_summary.insert("column".to_string(), json!(column));
                        column_summary.insert(
                            "level".to_string(),
                            match column.as_str() {
                                "previous_row" => json!("move"),
                                _ => json!("update"),
                            },
                        );
                        column_summary.insert("old_value".to_string(), json!(old_value));
                        column_summary.insert("value".to_string(), json!(new_value));
                        column_summary.insert(
                            "message".to_string(),
                            json!(format!(
                                "Value changed from {} to {}",
                                format_value(&old_value.to_string(), &numeric_re),
                                format_value(&new_value.to_string(), &numeric_re),
                            )),
                        );
                        let column_summary = to_text(Some(&column_summary), false);
                        summary.push(column_summary);
                    }
                }
                Ok(format!("[{}]", summary.join(",")))
            }
        }
    }

    let summary = summarize(from, to)?;
    let (from, to) = (to_text(from, true), to_text(to, true));
    let mut params = vec![table];
    let from_param = {
        if from == "NULL" {
            from
        } else {
            params.push(&from);
            SQL_PARAM.to_string()
        }
    };
    let to_param = {
        if to == "NULL" {
            to
        } else {
            params.push(&to);
            SQL_PARAM.to_string()
        }
    };
    let summary_param = {
        if summary == "NULL" {
            summary
        } else {
            params.push(&summary);
            SQL_PARAM.to_string()
        }
    };
    params.push(user);

    let sql = format!(
        r#"INSERT INTO "history" ("table", "row", "from", "to", "summary", "user")
           VALUES ({SQL_PARAM}, {row_number}, {from_param},
                   {to_param}, {summary_param}, {SQL_PARAM})"#,
    );
    let sql = local_sql_syntax(kind, &sql);

    let mut query = sqlx_query(&sql);
    for param in &params {
        query = query.bind(param)
    }
    query.execute(tx.acquire().await?).await?;

    Ok(())
}

/// Given the name of a configuration table type (the supported types are "column", "datatype", and
/// "table"), a row number, the previous state of the given row, the new state of the given row,
/// the user who initiated the change, a database transaction and the database kind, record the
/// configuration change in the history table.
pub async fn record_config_change_tx(
    table_type: &str,
    row_number: &u32,
    from: Option<&JsonRow>,
    to: Option<&JsonRow>,
    user: &str,
    tx: &mut Transaction<'_, sqlx::Any>,
    kind: &DbKind,
) -> Result<()> {
    fn to_text(details: Option<&JsonRow>) -> String {
        match details {
            None => "NULL".to_string(),
            Some(details) => format!("{}", json!(details)),
        }
    }

    // The summary column will just contain a short description of the generic operation:
    // add/delete/rename column/datatype/table. More detailed info is found in `from` and `to`.
    let summary = match (from, to) {
        (None, None) => {
            return Err(ValveError::InputError(
                "Arguments 'from' and 'to' to record_config_change_tx() cannot both be None".into(),
            )
            .into())
        }
        (None, _) => match table_type {
            "column" => "add column",
            "datatype" => "add datatype",
            "table" => "add table",
            _ => {
                return Err(ValveError::InputError(
                    format!(
                        "In record_config_change_tx(): Unrecognized config table_type \
                         '{table_type}'."
                    )
                    .into(),
                )
                .into())
            }
        },
        (_, None) => match table_type {
            "column" => "delete column",
            "datatype" => "delete datatype",
            "table" => "delete table",
            _ => {
                return Err(ValveError::InputError(
                    format!(
                        "In record_config_change_tx(): Unrecognized config table_type \
                         '{table_type}'."
                    )
                    .into(),
                )
                .into())
            }
        },
        (Some(_), Some(_)) => match table_type {
            "column" => "rename column",
            "datatype" => "rename datatype",
            "table" => "rename table",
            _ => {
                return Err(ValveError::InputError(
                    format!(
                        "In record_config_change_tx(): Unrecognized config table_type \
                         '{table_type}'."
                    )
                    .into(),
                )
                .into())
            }
        },
    };

    let (from, to) = (to_text(from), to_text(to));
    let mut params = vec![table_type];
    let from_param = match from.as_str() {
        "NULL" => from.to_string(),
        _ => {
            params.push(&from);
            SQL_PARAM.to_string()
        }
    };
    let to_param = match to.as_str() {
        "NULL" => to.to_string(),
        _ => {
            params.push(&to);
            SQL_PARAM.to_string()
        }
    };
    let summary_param = match summary {
        "NULL" => summary.to_string(),
        _ => {
            params.push(&summary);
            SQL_PARAM.to_string()
        }
    };
    params.push(user);

    let sql = format!(
        r#"INSERT INTO "history" ("table", "row", "from", "to", "summary", "user")
           VALUES ({SQL_PARAM}, {row_number}, {from_param},
                   {to_param}, {summary_param}, {SQL_PARAM})"#,
    );
    let sql = local_sql_syntax(kind, &sql);

    let mut query = sqlx_query(&sql);
    for param in &params {
        query = query.bind(param)
    }
    query.execute(tx.acquire().await?).await?;

    Ok(())
}

/// Return an ordered list of the undone changes that can be redone. If `limit` is nonzero, return
/// no more than that many database records.
pub async fn get_db_records_to_redo(pool: &AnyPool, limit: usize) -> Result<Vec<AnyRow>> {
    let is_not = if DbKind::from_pool(pool)? == DbKind::Sqlite {
        "IS NOT"
    } else {
        "IS DISTINCT FROM"
    };
    let limit_clause = if limit > 0 {
        format!(" LIMIT {limit}")
    } else {
        "".to_string()
    };
    let sql = format!(
        r#"SELECT * FROM "history"
           WHERE "undone_by" {is_not} NULL
           ORDER BY "timestamp" DESC{limit_clause}"#
    );
    let query = sqlx_query(&sql);
    Ok(query.fetch_all(pool).await?)
}

/// Get the history_id corresponding to the next record that can be redone.
pub async fn get_next_redo_id(pool: &AnyPool) -> Result<u16> {
    let records = get_db_records_to_redo(pool, 1).await?;
    match records.len() {
        0 => Ok(0),
        1 => {
            let next_redo = &records[0];
            let undone_by = next_redo.try_get_raw("undone_by")?;
            if undone_by.is_null() {
                Ok(0)
            } else {
                let history_id: i32 = next_redo.get("history_id");
                let history_id = history_id as u16;
                Ok(history_id)
            }
        }
        _ => Err(
            ValveError::DataError(format!("Too many records to redo: {}", records.len())).into(),
        ),
    }
}

/// Return an ordered list of the changes that can be undone. If `limit` is nonzero, return no
/// more than that many database records.
pub async fn get_db_records_to_undo(pool: &AnyPool, limit: usize) -> Result<Vec<AnyRow>> {
    let is = if DbKind::from_pool(pool)? == DbKind::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };
    let limit_clause = if limit > 0 {
        format!(" LIMIT {limit}")
    } else {
        "".to_string()
    };
    let sql = format!(
        r#"SELECT * FROM "history"
            WHERE "undone_by" {is} NULL
           ORDER BY "history_id" DESC{limit_clause}"#
    );
    let query = sqlx_query(&sql);
    Ok(query.fetch_all(pool).await?)
}

/// Get the history_id corresponding to the next record that can be undone.
pub async fn get_next_undo_id(pool: &AnyPool) -> Result<u16> {
    let records = get_db_records_to_undo(pool, 1).await?;
    match records.len() {
        0 => Ok(0),
        1 => {
            let next_undo = &records[0];
            let history_id: i32 = next_undo.get("history_id");
            let history_id = history_id as u16;
            Ok(history_id)
        }
        _ => {
            return Err(ValveError::DataError(format!(
                "Too many records to undo: {}",
                records.len()
            ))
            .into())
        }
    }
}

/// Given a table name, an [AnyRow] representing the last change to the database (which is
/// expected to be a move operation), a history id, a row number, a database transaction, and a
/// flag indicating whether the given move should be undone (true) or redone (false), undoes or
/// redoes the move.
pub async fn undo_or_redo_move_tx(
    table: &str,
    last_change: &AnyRow,
    history_id: u16,
    row_number: &u32,
    tx: &mut Transaction<'_, sqlx::Any>,
    undo: bool,
) -> Result<()> {
    let summary = match get_json_array_from_column(&last_change, "summary") {
        None => {
            return Err(ValveError::DataError(format!(
                "No summary found in undo record for history_id == {}",
                history_id,
            ))
            .into())
        }
        Some(summary) => summary[0].clone(),
    };
    let column = summary
        .get("column")
        .and_then(|c| c.as_str())
        .ok_or(ValveError::DataError(format!(
            "No property 'column' found in summary for history_id == {}",
            history_id,
        )))?;
    if column != "previous_row" {
        return Err(ValveError::DataError(format!(
            "Unexpected column '{}' in summary record for history_id == {}. \
             Expected: 'previous_row'",
            column, history_id
        ))
        .into());
    }
    let level = summary
        .get("level")
        .and_then(|l| l.as_str())
        .ok_or(ValveError::DataError(format!(
            "No property 'level' found in summary for history_id == {}",
            history_id,
        )))?;
    if level != "move" {
        return Err(ValveError::DataError(format!(
            "Unexpected level '{}' in summary record for history_id == {}. \
             Expected 'move'",
            level, history_id
        ))
        .into());
    }
    let update_value = {
        if undo {
            summary
                .get("old_value")
                .and_then(|l| l.as_str())
                .ok_or(ValveError::DataError(format!(
                    "No property 'old_value' found in summary for history_id == {}",
                    history_id,
                )))?
                .parse::<u32>()?
        } else {
            summary
                .get("value")
                .and_then(|l| l.as_str())
                .ok_or(ValveError::DataError(format!(
                    "No property 'old_value' found in summary for history_id == {}",
                    history_id,
                )))?
                .parse::<u32>()?
        }
    };
    move_row_tx(tx, table, &row_number, &update_value).await?;
    Ok(())
}

/// Given a user, a history_id, a database transaction, and an undone_state indicating whether to
/// set the associated history record as undone (if undone_state == true) or as not undone
/// (otherwise), do the following: When setting the record to undone, `user` is used for the
/// 'undone_by' field of the history table, otherwise undone_by is set to NULL and the user is
/// indicated as the one responsible for the change (instead of whoever made the change originally).
pub async fn switch_undone_state_tx(
    user: &str,
    history_id: u16,
    undone_state: bool,
    tx: &mut Transaction<'_, sqlx::Any>,
    kind: &DbKind,
) -> Result<()> {
    // Set the history record to undone:
    let timestamp = {
        if *kind == DbKind::Sqlite {
            "STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')"
        } else {
            "CURRENT_TIMESTAMP"
        }
    };
    let undone_by = if undone_state == true {
        format!(
            r#""undone_by" = {}, "timestamp" = {}"#,
            SQL_PARAM, timestamp
        )
    } else {
        format!(
            r#""undone_by" = NULL, "user" = {}, "timestamp" = {}"#,
            SQL_PARAM, timestamp
        )
    };
    let sql = local_sql_syntax(
        kind,
        &format!(
            r#"UPDATE "history" SET {} WHERE "history_id" = {}"#,
            undone_by, history_id
        ),
    );
    let query = sqlx_query(&sql).bind(user);
    query.execute(tx.acquire().await?).await?;
    Ok(())
}

/// Given a global config map, a database connection pool, a database transaction, a table name, a
/// column name, and a value for that column: get the rows, other than the one indicated by
/// `except`, that would need to be revalidated if the given value were to replace the actual
/// value of the column in that row.
pub async fn get_affected_rows_tx(
    table: &str,
    column: &str,
    value: &str,
    except: Option<&u32>,
    config: &ValveConfig,
    kind: &DbKind,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<Vec<ValveRow>> {
    // Since the consequence of an update could involve currently invalid rows
    // (in the conflict table) becoming valid or vice versa, we need to check rows for
    // which the value of the column is the same as `value`
    let (sql, mut sql_params) = generic_select_with_message_values(table, config, kind);
    let sql = local_sql_syntax(
        kind,
        &format!(
            r#"{sql} WHERE "{column}" = {SQL_PARAM}{except}"#,
            except = match except {
                None => "".to_string(),
                Some(row_number) => {
                    format!(" AND row_number != {}", row_number)
                }
            },
        ),
    );
    sql_params.push(value.to_string());

    let mut query = sqlx_query(&sql);
    for param in &sql_params {
        query = query.bind(param);
    }
    let mut valve_rows = vec![];
    for row in query.fetch_all(tx.acquire().await?).await? {
        let mut contents = IndexMap::new();
        let mut row_number: Option<u32> = None;
        for column in row.columns() {
            let cname = column.name();
            if cname == "row_number" {
                row_number = Some(row.get::<i64, _>("row_number") as u32);
            } else if cname != "message" {
                let raw_value = row.try_get_raw(format!(r#"{}"#, cname).as_str()).unwrap();
                let value;
                if !raw_value.is_null() {
                    value = get_column_value_as_string(&row, &cname, "text");
                } else {
                    value = String::from("");
                }
                contents.insert(cname.to_string(), ValveCell::new(&json!(value)));
            }
        }
        let row_number =
            row_number.ok_or(ValveError::DataError("Row: has no row number".to_string()))?;
        valve_rows.push(ValveRow {
            row_number: Some(row_number),
            contents: contents,
        });
    }

    Ok(valve_rows)
}

/// Given a global config map, a database connection pool, a database transaction, a table name,
/// and a [QueryAsIf] struct representing a custom modification to the query of the table, get
/// the rows that will potentially be affected by the database change to the row indicated in
/// query_as_if. These are divided into three: The rows that must be updated before the current
/// update, the rows that must be updated after the current update, and the rows from the same
/// table as the current update that need to be updated.
pub async fn get_rows_to_update_tx(
    config: &ValveConfig,
    kind: &DbKind,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    query_as_if: &QueryAsIf,
) -> Result<(
    IndexMap<String, Vec<ValveRow>>,
    IndexMap<String, Vec<ValveRow>>,
    IndexMap<String, Vec<ValveRow>>,
    IndexMap<String, Vec<ValveRow>>,
)> {
    fn get_cell_value(row: &ValveRow, column: &str) -> Result<String> {
        row.contents
            .get(column)
            .and_then(|cell| Some(cell.strvalue()))
            .ok_or(
                ValveError::InputError(format!(
                    "Value missing or of unknown type in column {} of row to update: {:?}",
                    column, row
                ))
                .into(),
            )
    }

    // Collect foreign key dependencies:
    let foreign_dependencies = {
        let mut foreign_dependencies = vec![];
        let global_fconstraints = &config.constraint.foreign;
        for (_, fconstraints) in global_fconstraints {
            for entry in fconstraints {
                if entry.ftable == *table {
                    foreign_dependencies.push(entry);
                }
            }
        }
        foreign_dependencies
    };

    let mut rows_to_update_before = IndexMap::new();
    let mut rows_to_update_after = IndexMap::new();
    let mut rows_to_update_unique = IndexMap::new();
    let mut rows_to_update_tree = IndexMap::new();
    for fdep in &foreign_dependencies {
        let dependent_table = &fdep.table;
        let dependent_column = &fdep.column;
        let target_column = &fdep.fcolumn;
        let target_table = &fdep.ftable;

        // Query the database using `row_number` to get the current value of the column for
        // the row.
        let updates_before = match query_as_if.kind {
            QueryAsIfKind::Add => {
                if let None = query_as_if.row {
                    log::warn!(
                        "No row in query_as_if: {:?} for {:?}",
                        query_as_if,
                        query_as_if.kind
                    );
                }
                vec![]
            }
            _ => {
                let current_value = get_db_value_tx(
                    target_table,
                    target_column,
                    &query_as_if.row_number,
                    kind,
                    tx,
                )
                .await?;

                // Query dependent_table.dependent_column for the rows that will be affected by the
                // change from the current value:
                get_affected_rows_tx(
                    dependent_table,
                    dependent_column,
                    &current_value,
                    None,
                    config,
                    kind,
                    tx,
                )
                .await?
            }
        };

        let updates_after = match &query_as_if.row {
            None => {
                if query_as_if.kind != QueryAsIfKind::Remove {
                    log::warn!(
                        "No row in query_as_if: {:?} for {:?}",
                        query_as_if,
                        query_as_if.kind
                    );
                }
                vec![]
            }
            Some(row) => {
                // Fetch the cell corresponding to `column` from `row`, and the value of that cell,
                // which is the new value for the row.
                let new_value = get_cell_value(&row, target_column)?;
                get_affected_rows_tx(
                    dependent_table,
                    dependent_column,
                    &new_value,
                    None,
                    config,
                    kind,
                    tx,
                )
                .await?
            }
        };
        rows_to_update_before.insert(dependent_table.to_string(), updates_before);
        rows_to_update_after.insert(dependent_table.to_string(), updates_after);
    }

    let primaries = config
        .constraint
        .primary
        .get(table)
        .expect(&format!("Undefined table '{}'", table));
    let uniques = config
        .constraint
        .unique
        .get(table)
        .expect(&format!("Undefined table '{}'", table));
    let columns = config
        .table
        .get(table)
        .expect(&format!("Undefined table '{}'", table))
        .column
        .keys()
        .map(|k| k.to_string())
        .collect::<Vec<_>>();

    for column in &columns {
        if !uniques.contains(column) && !primaries.contains(column) {
            continue;
        }

        // Query the database using `row_number` to get the current value of the column for
        // the row. We only look for rows to update that match the current value of the column.
        // Rows matching the column's new value don't also need to be updated. Those will result
        // in a validation error for the new/modified row but that is fine.
        let updates = match query_as_if.kind {
            QueryAsIfKind::Add => {
                if let None = query_as_if.row {
                    log::warn!(
                        "No row in query_as_if: {:?} for {:?}",
                        query_as_if,
                        query_as_if.kind
                    );
                }
                vec![]
            }
            _ => {
                let current_value =
                    get_db_value_tx(table, column, &query_as_if.row_number, kind, tx).await?;

                // Query table.column for the rows that will be affected by the change from the
                // current to the new value:
                get_affected_rows_tx(
                    table,
                    column,
                    &current_value,
                    Some(&query_as_if.row_number),
                    config,
                    kind,
                    tx,
                )
                .await?
            }
        };
        rows_to_update_unique.insert(table.to_string(), updates);
    }

    // Collect tree-foreign dependencies:
    let trees = config
        .constraint
        .tree
        .get(table)
        .expect(&format!("Undefined table '{}'", table));

    for tree in trees {
        let dependent_column = &tree.parent;
        let target_column = &tree.child;
        // Query the database using `row_number` to get the current value of the column for
        // the row.
        let updates_tree = match &query_as_if.row {
            None => {
                if query_as_if.kind != QueryAsIfKind::Remove {
                    log::warn!(
                        "No row in query_as_if: {:?} for {:?}",
                        query_as_if,
                        query_as_if.kind
                    );
                }
                vec![]
            }
            Some(row) => {
                // Fetch the cell corresponding to `column` from `row`, and the value of that cell,
                // which is the new value for the row.
                let new_value = get_cell_value(&row, target_column)?;
                let rows = get_affected_rows_tx(
                    table,
                    dependent_column,
                    &new_value,
                    Some(&query_as_if.row_number),
                    config,
                    kind,
                    tx,
                )
                .await?;
                rows
            }
        };
        rows_to_update_tree.insert(table.to_string(), updates_tree);
    }

    Ok((
        rows_to_update_before,
        rows_to_update_tree,
        rows_to_update_unique,
        rows_to_update_after,
    ))
}

/// Given a global config map, maps of datatype and rule conditions, a database connection pool,
/// a database transaction, some updates to process, a [QueryAsIf] struct indicating how
/// we should counterfactually modify the current state of the database, and a flag indicating
/// whether we should allow recursive updates, validate and then update each row indicated in
/// `updates`.
pub async fn process_updates_tx(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<'_, sqlx::Any>,
    updates: &IndexMap<String, Vec<ValveRow>>,
    query_as_if: &QueryAsIf,
    do_not_recurse: bool,
) -> Result<()> {
    for (update_table, rows_to_update) in updates {
        for row in rows_to_update {
            // Validate each row 'counterfactually':
            let vrow = validate_row_tx(
                config,
                datatype_conditions,
                rule_conditions,
                pool,
                Some(tx),
                update_table,
                row,
                Some(&query_as_if),
            )
            .await?;

            // Update the row in the database:
            update_row_tx(
                config,
                datatype_conditions,
                rule_conditions,
                pool,
                tx,
                update_table,
                &vrow,
                false,
                do_not_recurse,
            )
            .await?;
        }
    }
    Ok(())
}

/// Given a list of messages and a HashMap, messages_stats, with which to collect counts of
/// message types, count the various message types encountered in the list and increment the counts
/// in messages_stats accordingly.
pub fn add_message_counts(
    messages: &Vec<ValveCellMessage>,
    messages_stats: &mut HashMap<String, usize>,
) {
    for message in messages {
        if message.level == "error" {
            let current_errors = messages_stats.get("error").unwrap();
            messages_stats.insert("error".to_string(), current_errors + 1);
        } else if message.level == "warning" {
            let current_warnings = messages_stats.get("warning").unwrap();
            messages_stats.insert("warning".to_string(), current_warnings + 1);
        } else if message.level == "info" {
            let current_infos = messages_stats.get("info").unwrap();
            messages_stats.insert("info".to_string(), current_infos + 1);
        } else {
            log::warn!("Unknown message type: {}", message.level);
        }
    }
}

/// Given a sorted list of datatypes and a list of messages for a given cell of some table, sort
/// the messages in the following way and return the sorted list of messages:
/// 1. Messages pertaining to datatype rule violations, sorted according to the order specified in
///    `sorted_datatypes`, followed by:
/// 2. Messages pertaining to violations of one of the rules in the rule table, followed by:
/// 3. Messages pertaining to structure violations.
pub fn sort_messages(
    sorted_datatypes: &Vec<&str>,
    cell_messages: &Vec<ValveCellMessage>,
) -> Vec<ValveCellMessage> {
    let mut datatype_messages = vec![];
    let mut structure_messages = vec![];
    let mut rule_messages = vec![];
    for message in cell_messages {
        let rule = message.rule.splitn(2, ":").collect::<Vec<_>>();
        if rule[0] == "rule" {
            rule_messages.push(message.clone());
        } else if rule[0] == "datatype" {
            datatype_messages.push(message.clone());
        } else {
            structure_messages.push(message.clone());
        }
    }

    if datatype_messages.len() > 0 {
        datatype_messages = {
            let mut sorted_messages = vec![];
            for datatype in sorted_datatypes {
                let mut messages = datatype_messages
                    .iter()
                    .filter(|m| m.rule == format!("datatype:{}", datatype))
                    .map(|m| m.clone())
                    .collect::<Vec<_>>();
                sorted_messages.append(&mut messages);
            }
            sorted_messages
        }
    }

    let mut messages = datatype_messages;
    messages.append(&mut rule_messages);
    messages.append(&mut structure_messages);
    messages
}

/// Given a configuration struct, a table name, some rows, their corresponding chunk number,
/// a struct used for recording message statistics, a verbose flag, and a database connection pool
/// used to determine the database type, return (i) an insert statement to the table, (ii)
/// parameters to bind to that SQL statement, (iii) an insert statement to the conflict version of
/// the table, (iv) parameters to bind to that SQL statement, (v) an insert statement to the
/// message table, and (vi) parameters to bind to that SQL statement. If the verbose flag is set,
/// the number of errors, warnings, and information messages generated are added to messages_stats.
pub async fn make_inserts(
    config: &ValveConfig,
    table_name: &String,
    rows: &mut Vec<ValveRow>,
    chunk_number: usize,
    messages_stats: &mut HashMap<String, usize>,
    verbose: bool,
    kind: &DbKind,
) -> Result<(
    String,
    Vec<String>,
    String,
    Vec<String>,
    String,
    Vec<String>,
)> {
    fn is_conflict_row(row: &ValveRow, conflict_columns: &Vec<String>) -> bool {
        for (column, cell) in &row.contents {
            if !cell.valid && conflict_columns.contains(&column) {
                return true;
            }
        }
        return false;
    }

    fn generate_sql(
        config: &ValveConfig,
        main_table: &String,
        columns: &Vec<String>,
        rows: &mut Vec<ValveRow>,
        chunk_number: usize,
        messages_stats: &mut HashMap<String, usize>,
        verbose: bool,
        kind: &DbKind,
    ) -> (
        String,
        Vec<String>,
        String,
        Vec<String>,
        String,
        Vec<String>,
    ) {
        let mut main_lines = vec![];
        let mut main_params = vec![];
        let mut conflict_lines = vec![];
        let mut conflict_params = vec![];
        let mut message_lines = vec![];
        let mut message_params = vec![];
        let sorted_datatypes = get_sorted_datatypes(config);
        let conflict_columns = get_conflict_columns(config, main_table);
        for (i, row) in rows.iter_mut().enumerate() {
            // enumerate begins at 0 but we need to begin at 1:
            let i = i + 1;
            row.row_number = Some(i as u32 + chunk_number as u32 * CHUNK_SIZE as u32);
            let row_number = row.row_number.unwrap();
            // The row order defaults to the row number:
            let row_order = row_number * MOVE_INTERVAL;
            let use_conflict_table = is_conflict_row(&row, &conflict_columns);
            let mut row_values = vec![format!("{}, {}", row_number, row_order)];
            let mut row_params = vec![];
            // If a table has been configured with more columns than are actually in its
            // associated .tsv file, then when we try to get a value for one of the extra
            // columns from the row, it will not be found. Instead of panicking we will
            // use unwrap_or() with an empty ValveCell as the default:
            let default_cell = ValveCell {
                ..Default::default()
            };
            for column in columns {
                let cell = row.contents.get(column).unwrap_or(&default_cell);
                // Insert the value of the cell into the column unless inserting it will cause a db
                // error or it has the nulltype field set, in which case insert NULL:
                let sql_type = get_sql_type_from_global_config(config, &main_table, column, kind);
                if cell.nulltype != None || is_sql_type_error(&sql_type, &cell.strvalue()) {
                    row_values.push(String::from("NULL"));
                } else {
                    row_values.push(cast_sql_param_from_text(&sql_type));
                    row_params.push(cell.strvalue());
                }

                // Generate values and params to be used for the insert to the message table:
                if verbose {
                    add_message_counts(&cell.messages, messages_stats);
                }

                for message in sort_messages(&sorted_datatypes, &cell.messages) {
                    let row = row_number.to_string();
                    let message_values = vec![
                        SQL_PARAM, &row, SQL_PARAM, SQL_PARAM, SQL_PARAM, SQL_PARAM, SQL_PARAM,
                    ];

                    message_params.push(main_table.clone());
                    message_params.push(column.clone());
                    message_params.push(cell.strvalue());
                    message_params.push(message.level);
                    message_params.push(message.rule);
                    message_params.push(message.message);
                    let line = message_values.join(", ");
                    let line = format!("({})", line);
                    message_lines.push(line);
                }
            }
            let line = row_values.join(", ");
            let line = format!("({})", line);
            if use_conflict_table {
                conflict_lines.push(line);
                conflict_params.append(&mut row_params);
            } else {
                main_lines.push(line);
                main_params.append(&mut row_params);
            }
        }

        // Generate the SQL output for the insert to the table:
        fn get_table_output(lines: &Vec<String>, table: &str, columns: &Vec<String>) -> String {
            let mut output = String::from("");
            if !lines.is_empty() {
                output.push_str(&format!(
                    r#"INSERT INTO "{}" ("row_number", "row_order", {}) VALUES"#,
                    table,
                    {
                        let mut quoted_columns = vec![];
                        for column in columns {
                            let quoted_column = format!(r#""{}""#, column);
                            quoted_columns.push(quoted_column);
                        }
                        quoted_columns.join(", ")
                    }
                ));
                output.push_str("\n");
                output.push_str(&lines.join(",\n"));
                output.push_str(";");
            }
            output
        }

        let main_output = get_table_output(&main_lines, &main_table, &columns);
        let conflict_table = format!("{}_conflict", main_table);
        let conflict_output = get_table_output(&conflict_lines, &conflict_table, &columns);

        // Generate the output for the insert to the message table:
        let mut message_output = String::from("");
        if !message_lines.is_empty() {
            message_output.push_str(r#"INSERT INTO "message" "#);
            message_output
                .push_str(r#"("table", "row", "column", "value", "level", "rule", "message") "#);
            message_output.push_str("VALUES");
            message_output.push_str("\n");
            message_output.push_str(&message_lines.join(",\n"));
            message_output.push_str(";");
        }

        (
            main_output,
            main_params,
            conflict_output,
            conflict_params,
            message_output,
            message_params,
        )
    }

    // Use the "column_order" field of the table config for this table to retrieve the column names
    // in the correct order:
    let column_names = &config
        .table
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .column_order;

    let (main_sql, main_params, conflict_sql, conflict_params, message_sql, message_params) =
        generate_sql(
            config,
            &table_name,
            column_names,
            rows,
            chunk_number,
            messages_stats,
            verbose,
            kind,
        );

    Ok((
        main_sql,
        main_params,
        conflict_sql,
        conflict_params,
        message_sql,
        message_params,
    ))
}

/// Given a configuration map, a database connection pool, a table name, some rows to load,
/// and the chunk number corresponding to the rows, load the rows to the database. If the validate
/// flag is set, do inter-row validation on the rows before inserting  them to the table. If the
/// verbose flag is set to true, keep track of the number of error/warning/info statistics and
/// record them using `messages_stats`.
pub async fn insert_chunk(
    config: &ValveConfig,
    pool: &AnyPool,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &String,
    rows: &mut Vec<ValveRow>,
    chunk_number: usize,
    messages_stats: &mut HashMap<String, usize>,
    verbose: bool,
    validate: bool,
) -> Result<()> {
    async fn validate_and_insert(
        // This function implements the full validation process without any shortcuts. It will be
        // called only in the last resort. Otherwise we try to use the database to tell us whether
        // validation needs to be done and only do it in that case.
        config: &ValveConfig,
        pool: &AnyPool,
        datatype_conditions: &HashMap<String, CompiledCondition>,
        table_name: &String,
        rows: &mut Vec<ValveRow>,
        chunk_number: usize,
        messages_stats: &mut HashMap<String, usize>,
        verbose: bool,
    ) -> Result<()> {
        // Validate all remaining constraints:
        validate_rows_constraints(config, pool, datatype_conditions, table_name, rows).await?;
        // Construct the SQL statements and corresponding parameters for the database update:
        let (main_sql, main_params, conflict_sql, conflict_params, message_sql, message_params) =
            make_inserts(
                config,
                table_name,
                rows,
                chunk_number,
                messages_stats,
                verbose,
                &DbKind::from_pool(pool)?,
            )
            .await?;

        // Add data to the main table:
        let main_sql = local_sql_syntax(&DbKind::from_pool(pool)?, &main_sql);
        let mut main_query = sqlx_query(&main_sql);
        for param in &main_params {
            main_query = main_query.bind(param);
        }
        main_query.execute(pool).await?;

        // Add data to the conflict table:
        let conflict_sql = local_sql_syntax(&DbKind::from_pool(pool)?, &conflict_sql);
        let mut conflict_query = sqlx_query(&conflict_sql);
        for param in &conflict_params {
            conflict_query = conflict_query.bind(param);
        }
        conflict_query.execute(pool).await?;

        // Add data to the message table:
        let message_sql = local_sql_syntax(&DbKind::from_pool(pool)?, &message_sql);
        let mut message_query = sqlx_query(&message_sql);
        for param in &message_params {
            message_query = message_query.bind(param);
        }
        message_query.execute(pool).await?;
        Ok(())
    }

    // Determine whether any of the columns of this table are list types constrained by from()
    // structures, or are the basis for a tree() constraint. Such columns are not tied to foreign
    // keys in the database and therefore we cannot rely on the database to complain when they are
    // violated:
    let has_list_with_from = {
        let table_config = config
            .table
            .get(table_name)
            .expect(&format!("Cannot find config for table '{}'", table_name));
        // Check, for each column, whether it is a list type, and if it is, check if the table
        // has a foreign constraint on that column:
        table_config.column.keys().any(|column_name| {
            match get_value_type(config, datatype_conditions, table_name, column_name) {
                ValueType::List(_, _) => {
                    let foreigns = config.constraint.foreign.get(table_name).expect(&format!(
                        "Cannot find foreign constraints for table '{}'",
                        table_name
                    ));
                    foreigns
                        .iter()
                        .any(|foreign| foreign.column == *column_name)
                }
                ValueType::Single => false,
            }
        })
    };
    let has_trees = {
        let trees = config
            .constraint
            .tree
            .get(table_name)
            .expect(&format!("Cannot find trees for table '{}'", table_name));
        !trees.is_empty()
    };

    // If the validation flag is set, then if the table has a list column with a from() structure,
    // or if the table has a tree, we always perform validation. Otherwise we rely on the database
    // to complain when we try to insert the data, and only do the full validation when it does.
    if validate && (has_list_with_from || has_trees) {
        validate_and_insert(
            config,
            pool,
            datatype_conditions,
            table_name,
            rows,
            chunk_number,
            messages_stats,
            verbose,
        )
        .await
    } else {
        // Insertion with optional inter-table validation:
        // Try to insert the rows to the db first without validating unique and foreign constraints.
        // If there are constraint violations this will cause a database error, in which case we
        // then explicitly do the constraint validation and insert the resulting rows. Note that
        // instead of passing messages_stats here, we are going to initialize an empty map and pass
        // that instead. The reason is that if a database error gets thrown, and then we
        // redo the validation later, some of the messages will be double-counted. So to avoid
        // that we send an empty map here, and in the case of no database error, we will just add
        // the contents of the temporary map to messages_stats (in the Ok branch of the match
        // statement below).
        let mut tmp_messages_stats = HashMap::new();
        tmp_messages_stats.insert("error".to_string(), 0);
        tmp_messages_stats.insert("warning".to_string(), 0);
        tmp_messages_stats.insert("info".to_string(), 0);
        let (main_sql, main_params, conflict_sql, conflict_params, message_sql, message_params) =
            make_inserts(
                config,
                table_name,
                rows,
                chunk_number,
                &mut tmp_messages_stats,
                verbose,
                &DbKind::from_pool(pool)?,
            )
            .await?;

        let main_sql = local_sql_syntax(&DbKind::from_pool(pool)?, &main_sql);
        let mut main_query = sqlx_query(&main_sql);
        for param in &main_params {
            main_query = main_query.bind(param);
        }
        let main_result = main_query.execute(pool).await;
        match main_result {
            Ok(_) => {
                let conflict_sql = local_sql_syntax(&DbKind::from_pool(pool)?, &conflict_sql);
                let mut conflict_query = sqlx_query(&conflict_sql);
                for param in &conflict_params {
                    conflict_query = conflict_query.bind(param);
                }
                conflict_query.execute(pool).await?;

                let message_sql = local_sql_syntax(&DbKind::from_pool(pool)?, &message_sql);
                let mut message_query = sqlx_query(&message_sql);
                for param in &message_params {
                    message_query = message_query.bind(param);
                }
                message_query.execute(pool).await?;

                if verbose {
                    let curr_errors = messages_stats.get("error").unwrap();
                    messages_stats.insert(
                        "error".to_string(),
                        curr_errors + tmp_messages_stats.get("error").unwrap(),
                    );
                    let curr_warnings = messages_stats.get("warning").unwrap();
                    messages_stats.insert(
                        "warning".to_string(),
                        curr_warnings + tmp_messages_stats.get("warning").unwrap(),
                    );
                    let curr_infos = messages_stats.get("info").unwrap();
                    messages_stats.insert(
                        "info".to_string(),
                        curr_infos + tmp_messages_stats.get("info").unwrap(),
                    );
                }
                Ok(())
            }
            Err(e) => {
                if validate {
                    validate_and_insert(
                        config,
                        pool,
                        datatype_conditions,
                        table_name,
                        rows,
                        chunk_number,
                        messages_stats,
                        verbose,
                    )
                    .await
                } else {
                    Err(ValveError::DatabaseError(e).into())
                }
            }
        }
    }
}

/// Given a configuration map, a database connection pool, maps for compiled datatype and rule
/// conditions, a table name, some chunks of rows to insert into the table in the database,
/// and the headers of the rows to be inserted, load the rows to the given table. If the validate
/// flag is set, do validate the rows before inserting them to the table. If the verbose flag is
/// set, keep track of the number of error/warning/info statistics and record them using
/// `messages_stats`.
pub async fn insert_chunks(
    config: &ValveConfig,
    pool: &AnyPool,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    chunks: &IntoChunks<StringRecordsIter<'_, std::fs::File>>,
    headers: &Vec<String>,
    messages_stats: &mut HashMap<String, usize>,
    verbose: bool,
    validate: bool,
) -> Result<()> {
    if !MULTI_THREADED {
        for (chunk_number, chunk) in chunks.into_iter().enumerate() {
            let mut rows: Vec<_> = chunk.collect();
            // Intra-table validation:
            let mut intra_validated_rows = {
                let only_nulltype = !validate;
                validate_rows_intra(
                    config,
                    datatype_conditions,
                    rule_conditions,
                    table_name,
                    headers,
                    &mut rows,
                    only_nulltype,
                )
            };
            // Insertion with optional inter-table and tree validation:
            insert_chunk(
                config,
                pool,
                datatype_conditions,
                table_name,
                &mut intra_validated_rows,
                chunk_number,
                messages_stats,
                verbose,
                validate,
            )
            .await?;
        }
        Ok(())
    } else {
        // Here is how this works. First of all note that we are given a number of chunks of rows,
        // where the number of rows in each chunk is determined by CHUNK_SIZE (defined above). We
        // then divide the chunks into batches, where the number of chunks in each batch is
        // determined by the number of CPUs present on the system. We then iterate over the
        // batches one by one, assigning each chunk in a given batch to a worker thread whose
        // job is to perform intra-row validation on that chunk. The workers work in parallel, one
        // per CPU, and after all the workers have completed and their results have been collected,
        // we then perform inter-row validation on the chunks in the batch, this time serially.
        // Once this is done, we move on to the next batch and continue in this fashion.
        let num_cpus = num_cpus::get();
        let batches = chunks.into_iter().chunks(num_cpus);
        let mut chunk_number = 0;
        for batch in batches.into_iter() {
            let mut results = BTreeMap::new();
            crossbeam::scope(|scope| {
                let mut workers = vec![];
                for chunk in batch.into_iter() {
                    let mut rows: Vec<_> = chunk.collect();
                    workers.push(scope.spawn(move |_| {
                        let only_nulltype = !validate;
                        validate_rows_intra(
                            config,
                            datatype_conditions,
                            rule_conditions,
                            table_name,
                            headers,
                            &mut rows,
                            only_nulltype,
                        )
                    }));
                }

                for worker in workers {
                    let result = worker.join().unwrap();
                    results.insert(chunk_number, result);
                    chunk_number += 1;
                }
            })
            .expect("A child thread panicked");

            for (chunk_number, mut intra_validated_rows) in results {
                insert_chunk(
                    config,
                    pool,
                    datatype_conditions,
                    table_name,
                    &mut intra_validated_rows,
                    chunk_number,
                    messages_stats,
                    verbose,
                    validate,
                )
                .await?;
            }
        }

        Ok(())
    }
}
