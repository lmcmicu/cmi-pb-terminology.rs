//! The Valve API

use crate::{
    ast::Expression,
    guess::guess,
    internal::generate_internal_table_ddl,
    toolkit,
    toolkit::{
        add_column_tx, add_datatype_tx, add_message_counts, cast_column_sql_to_text,
        correct_row_datatypes, delete_column_tx, delete_datatype_tx, delete_row_tx,
        delete_table_tx, generate_datatype_conditions, generate_rule_conditions,
        get_column_for_label, get_column_value_as_string, get_db_records_to_redo,
        get_db_records_to_undo, get_json_array_from_column, get_json_object_from_column,
        get_next_new_row_tx, get_next_undo_id, get_parsed_structure_conditions,
        get_pool_from_connection_string, get_previous_row_tx, get_sql_for_standard_view,
        get_sql_for_text_view, get_sql_type, get_sql_type_from_global_config,
        get_text_row_from_db_tx, insert_chunks, insert_new_row_tx, local_sql_syntax, move_row_tx,
        normalize_options, read_config_files, read_db_table_into_vector_tx,
        record_config_change_tx, record_row_change_tx, record_row_move_tx, rename_column_tx,
        rename_datatype_tx, rename_table_tx, switch_undone_state_tx, undo_or_redo_move_tx,
        update_row_tx, verify_table_deps_and_sort, ColumnRule, CompiledCondition, DbKind,
        ParsedStructure, ValueType,
    },
    validate::{validate_row_tx, validate_tree_foreign_keys, with_tree_sql},
    valve_grammar::StartParser,
    CHUNK_SIZE, INTERNAL_TABLES, PRINTF_RE, SQL_PARAM, SQL_TYPES,
};
use anyhow::Result;
use csv::{QuoteStyle, ReaderBuilder, WriterBuilder};
use enquote::unquote;
use futures::{executor::block_on, TryStreamExt};
use indexmap::IndexMap;
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as SerdeValue};
use sprintf::sprintf;
use sqlx::{
    any::{AnyPool, AnyRow},
    query as sqlx_query, Acquire, Column, Row, ValueRef,
};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    fs::File,
    path::Path,
    process::Command,
};

/// Alias for [Map](serde_json::map)<[String], [Value](serde_json::value)>.
pub type JsonRow = serde_json::Map<String, SerdeValue>;

/// Given a row, represented as a JSON object in the following format:
/// ```
/// {
///     "column_1": value1,
///     "column_2": value2,
///     "column_3": value3,
///     ...
/// },
/// ```
/// if a given value happens to be a string, attempt to parse it as a complex type (i.e., as an
/// [Array](SerdeValue::Array) or [Object](SerdeValue::Object)) and replace the field with the
/// parsed value if the parsing has been successful. For instance, supppose `value1` above happens
/// to be a [String](SerdeValue::String) that happens to be parsable as an
/// [Object](SerdeValue::Object) with the fields "column", "value", "level", "rule",
/// and "message", suppose `value2` happens to be an array, and suppose `value3` is a number. Then
/// the JSON object returned from this function will look like:
/// ```
/// {
///     "column_1": {
///         "column": value1_column,
///         "value": value1_value,
///         "level": value1_level,
///         "rule": value1_rule,
///         "message": value1_message,
///     },
///     "column_2": [value2_a, value2_b, ...],
///     "column_3": numeric_value_3,
///     ...
/// }
/// ```
pub fn unfold_json_row(json_row: &JsonRow) -> Result<JsonRow> {
    let mut rich_row = JsonRow::new();
    for (column, value) in json_row.iter() {
        let value = match value {
            SerdeValue::String(strvalue) => match serde_json::from_str::<SerdeValue>(strvalue) {
                Ok(SerdeValue::Array(v)) => json!(v),
                Ok(SerdeValue::Object(v)) => json!(v),
                Err(_) | Ok(_) => value.clone(),
            },
            _ => value.clone(),
        };
        rich_row.insert(column.to_string(), value);
    }
    Ok(rich_row)
}

/// Represents a particular row of data with validation results.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveRow {
    /// The row number of this row. None indicates "unspecified".
    pub row_number: Option<u32>,
    /// A map from column names to the cells corresponding to each column in the row.
    pub contents: IndexMap<String, ValveCell>,
}

impl ValveRow {
    /// Given a row represented as a JSON object in the following ('simple')
    /// format:
    /// ```
    /// {
    ///     "column_1": value1,
    ///     "column_2": value2,
    ///     ...
    /// }
    /// ```
    /// and, optionally, a row number, convert the row into a [ValveRow] and return it.
    pub fn from_simple_json(row: &JsonRow, row_number: Option<u32>) -> Result<Self> {
        let mut valve_cells = IndexMap::new();
        for (column, value) in row.iter() {
            valve_cells.insert(column.to_string(), ValveCell::new(value));
        }
        Ok(Self {
            row_number: row_number,
            contents: valve_cells,
        })
    }

    /// Given a [ValveRow], convert its contents to a [JsonRow] in simple format and return it.
    pub fn contents_to_simple_json(&self) -> Result<JsonRow> {
        let row_contents = serde_json::to_value(self.contents.clone())?;
        let row_contents = row_contents
            .as_object()
            .ok_or(ValveError::InputError(format!(
                "Could not convert {:?} to a rich JSON object",
                row_contents
            )))?;
        let row_contents = row_contents
            .iter()
            .map(|(key, cell)| (key.clone(), cell.get("value").expect("No value").clone()))
            .collect::<JsonRow>();
        Ok(row_contents)
    }

    /// Given a row, with the given row number, represented as a JSON object in the following
    /// ('rich') format:
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
    /// convert it into a [ValveRow] and return it.
    pub fn from_rich_json(row_number: Option<u32>, row: &JsonRow) -> Result<Self> {
        Ok(serde_json::from_str(
            &json!({
                "row_number": row_number,
                "contents": row,
            })
            .to_string(),
        )?)
    }

    /// Given a [ValveRow], convert it to a [JsonRow] using [serde_json::to_value()] and return it.
    pub fn to_rich_json(&self) -> Result<JsonRow> {
        let value = serde_json::to_value(self)?;
        value
            .as_object()
            .ok_or(
                ValveError::InputError(format!(
                    "Could not convert {:?} to a rich JSON object",
                    value
                ))
                .into(),
            )
            .cloned()
    }

    /// Given a [ValveRow], convert the `contents` field of the row to a [JsonRow] using
    /// [serde_json::to_value()] and return it.
    pub fn contents_to_rich_json(&self) -> Result<JsonRow> {
        let value = serde_json::to_value(self.contents.clone())?;
        value
            .as_object()
            .ok_or(
                ValveError::InputError(format!(
                    "Could not convert {:?} to a rich JSON object",
                    value
                ))
                .into(),
            )
            .cloned()
    }

    /// Given a configuration map, a database kind, a table name, and a database row, construct
    /// a [ValveRow] and return it. If the global_sql_type flag is set, then all of the columns
    /// of the given row are interpreted as being of that type.
    pub fn from_any_row(
        config: &ValveConfig,
        kind: &DbKind,
        table: &str,
        any_row: &AnyRow,
        global_sql_type: &Option<&str>,
    ) -> Result<Self> {
        let messages = {
            let raw_messages = any_row.try_get_raw("message")?;
            if raw_messages.is_null() {
                vec![]
            } else {
                let messages: &str = any_row.get("message");
                match serde_json::from_str::<SerdeValue>(messages) {
                    Err(e) => return Err(e.into()),
                    Ok(SerdeValue::Array(m)) => m,
                    _ => {
                        return Err(ValveError::DataError(
                            format!("{} is not an array.", messages).into(),
                        )
                        .into())
                    }
                }
            }
        };
        let row_number: i64 = any_row.get::<i64, _>("row_number");
        let row_number = row_number as u32;
        let mut row = ValveRow {
            row_number: Some(row_number),
            ..Default::default()
        };
        for column in any_row.columns() {
            let cname = column.name();
            if !vec!["row_number", "message"].contains(&cname) {
                let raw_value = any_row.try_get_raw(format!(r#"{}"#, cname).as_str())?;
                let value;
                if !raw_value.is_null() {
                    let sql_type = match global_sql_type {
                        Some(sql_type) => sql_type.to_string(),
                        None => get_sql_type_from_global_config(config, table, cname, kind),
                    };
                    value = get_column_value_as_string(&any_row, &cname, &sql_type);
                } else {
                    value = String::from("");
                }
                let column_messages = messages
                    .iter()
                    .filter(|m| m.get("column").unwrap_or(&json!("")).as_str() == Some(cname))
                    .map(|message| {
                        let level = message
                            .get("level")
                            .and_then(|l| l.as_str())
                            .unwrap_or("No 'level' found");
                        let rule = message
                            .get("rule")
                            .and_then(|l| l.as_str())
                            .unwrap_or("No 'rule' found");
                        let message = message
                            .get("message")
                            .and_then(|l| l.as_str())
                            .unwrap_or("No 'message' found");
                        ValveCellMessage {
                            level: level.to_string(),
                            rule: rule.to_string(),
                            message: message.to_string(),
                        }
                    })
                    .collect::<Vec<_>>();
                let valid = column_messages.iter().all(|m| m.level != "error");
                let cell = ValveCell {
                    value: json!(value),
                    valid: valid,
                    messages: column_messages,
                    ..Default::default()
                };
                row.contents.insert(cname.to_string(), cell);
            }
        }
        Ok(row)
    }
}

/// Represents a particular cell in a particular row of data with vaildation results.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveCell {
    /// If present, indicates that the value of the cell is considered to be a null value of
    /// the given type. If not present, indicates that the contents of the cell are not empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nulltype: Option<String>,
    /// The value of the cell.
    pub value: SerdeValue,
    /// Whether the value of the cell is valid or invalid.
    pub valid: bool,
    /// Any messages associated with the cell value.
    pub messages: Vec<ValveCellMessage>,
}

impl ValveCell {
    /// Initializes a new [ValveCell] based on the given [SerdeValue] and returns it. Note that
    /// any value that is not a [String](SerdeValue::String), [Number](SerdeValue::Number), or
    /// [Bool](SerdeValue::Bool) is converted to a [String](SerdeValue::String).
    pub fn new(value: &SerdeValue) -> Self {
        let value = match value {
            SerdeValue::String(_) | SerdeValue::Number(_) | SerdeValue::Bool(_) => value.clone(),
            _ => json!(value.to_string()),
        };
        Self {
            nulltype: None,
            value: value,
            valid: true,
            messages: vec![],
        }
    }

    /// Given a [ValveCell], convert it to a JSON object using [serde_json::to_value()]
    /// and return it.
    pub fn to_rich_json(&self) -> Result<serde_json::Map<String, SerdeValue>> {
        let value = serde_json::to_value(self)?;
        value
            .as_object()
            .ok_or(
                ValveError::InputError(format!(
                    "Could not convert {:?} to a rich JSON object",
                    value
                ))
                .into(),
            )
            .cloned()
    }

    /// Returns the value of the cell as a String.
    pub fn strvalue(&self) -> String {
        match &self.value {
            SerdeValue::String(s) => s.to_string(),
            _ => self.value.to_string(),
        }
    }
}

/// Represents one of the messages in a [ValveCell]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveCellMessage {
    /// The severity of the message.
    pub level: String,
    /// The rule violation that the message is about.
    pub rule: String,
    /// The contents of the message.
    pub message: String,
}

/// Generic enum representing various error types returned by Valve methods
#[derive(Debug)]
pub enum ValveError {
    /// An error in the Valve configuration:
    ConfigError(String),
    /// An error that occurred while reading or writing to a CSV/TSV:
    CsvError(csv::Error),
    /// An error involving the data:
    DataError(String),
    /// An error generated by the underlying database:
    DatabaseError(sqlx::Error),
    /// An error in the inputs to a function:
    InputError(String),
    /// An error that occurred while reading/writing to stdio:
    IOError(std::io::Error),
    /// An error that occurred while serialising or deserialising to/from JSON:
    SerdeJsonError(serde_json::Error),
    /// An error that occurred while parsing a regex:
    RegexError(regex::Error),
    /// An error that occurred because of a user's action
    UserError(String),
}

impl std::fmt::Display for ValveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ValveError {}

/// Represents a message associated with a particular value of a particular column.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValveMessage {
    /// The name of the column
    pub column: String,
    /// The value of the column
    pub value: String,
    /// The rule violated by the value
    pub rule: String,
    /// The severity of the violation
    pub level: String,
    /// A description of the violation
    pub message: String,
}

/// Represents a change to a row in a database table.
#[derive(Debug, Deserialize, Serialize)]
pub struct ValveRowChange {
    /// An identifier for this particular change
    pub history_id: u16,
    /// The name of the table that the change is from
    pub table: String,
    /// The row number of the changed row
    pub row: u32,
    /// A summary of the change to the row
    pub message: String,
    /// The changes to each cell
    pub changes: Vec<ValveChange>,
}

impl ValveRowChange {
    /// Given a database row representing either an undo or a redo record from the history table,
    /// returns a [ValveRowChange] struct containing the same information.
    pub fn from_undo_or_redo_record(db_rec: &AnyRow) -> Result<Self> {
        let table: &str = db_rec.try_get("table")?;
        let row_number: i64 = db_rec.try_get("row")?;
        let row_number = row_number as u32;
        let history_id: i32 = db_rec.try_get("history_id")?;
        let history_id = history_id as u16;
        let from = get_json_object_from_column(&db_rec, "from");
        let to = get_json_object_from_column(&db_rec, "to");
        let summary = {
            let summary = db_rec.try_get_raw("summary")?;
            if !summary.is_null() {
                let summary: &str = db_rec.try_get("summary")?;
                match serde_json::from_str::<SerdeValue>(summary) {
                    Ok(SerdeValue::Array(v)) => Some(v),
                    _ => {
                        return Err(
                            ValveError::InputError(format!("{summary} is not an array.")).into(),
                        )
                    }
                }
            } else {
                None
            }
        };

        // If the `summary` part of the record is present, then just use it to populate the fields
        // of the ValveChange structs representing the changes to each column in the table row.
        if let Some(summary) = summary {
            let mut column_changes = vec![];
            for entry in summary {
                let column = entry
                    .get("column")
                    .and_then(|s| s.as_str())
                    .and_then(|s| Some(s.to_string()))
                    .ok_or(ValveError::InputError("No 'column' found".to_string()))?;
                let level = entry
                    .get("level")
                    .and_then(|s| s.as_str())
                    .and_then(|s| Some(s.to_string()))
                    .ok_or(ValveError::InputError("No 'level' found".to_string()))?;
                let old_value = entry
                    .get("old_value")
                    .and_then(|s| s.as_str())
                    .and_then(|s| Some(s.to_string()))
                    .ok_or(ValveError::InputError("No 'old_value' found".to_string()))?;
                let value = entry
                    .get("value")
                    .and_then(|s| s.as_str())
                    .and_then(|s| Some(s.to_string()))
                    .ok_or(ValveError::InputError("No 'value' found".to_string()))?;
                let message = entry
                    .get("message")
                    .and_then(|s| s.as_str())
                    .and_then(|s| Some(s.to_string()))
                    .ok_or(ValveError::InputError("No 'message' found".to_string()))?;
                column_changes.push(ValveChange {
                    column: column,
                    level: level,
                    old_value: old_value,
                    value: value,
                    message: message,
                });
            }
            let message = {
                let moves = column_changes
                    .iter()
                    .filter(|cc| cc.level.to_lowercase() == "move")
                    .collect::<Vec<_>>();
                if moves.len() == 0 {
                    format!("Update row {row_number} of '{table}'")
                } else if moves.len() == 1 {
                    format!(
                        "Move row {row_number} of '{table}' from {before} to {after}",
                        before = {
                            if moves[0].old_value == "0" {
                                "first".to_string()
                            } else {
                                format!("after row {}", moves[0].old_value)
                            }
                        },
                        after = {
                            if moves[0].value == "0" {
                                "first".to_string()
                            } else {
                                format!("after row {}", moves[0].value)
                            }
                        },
                    )
                } else {
                    return Err(ValveError::InputError(
                        "Summary for move (history ID: {history_id}) contains too many records"
                            .to_string(),
                    )
                    .into());
                }
            };
            return Ok(ValveRowChange {
                history_id: history_id,
                table: table.to_string(),
                row: row_number,
                message: message,
                changes: column_changes,
            });
        }

        // If the `summary` part of the record is not present, then there are two possibilities.
        // (1) If `from` is not null and `to` is null then this is a delete (i.e., the row has changed
        // from something to nothing). (2) If `from` is null and `to` is not null then this is a
        // create.
        match (from, to) {
            (Some(from), None) => {
                let mut column_changes = vec![];
                for (column, change) in &from {
                    match change {
                        SerdeValue::Number(rn) if column == "previous_row" => {
                            column_changes.push(ValveChange {
                                column: column.to_string(),
                                level: "delete".to_string(),
                                old_value: rn.to_string(),
                                value: "".to_string(),
                                message: "".to_string(),
                            })
                        }
                        SerdeValue::Object(details) => {
                            let old_value = details
                                .get("value")
                                .and_then(|s| Some(s.to_string()))
                                .ok_or(ValveError::InputError("No 'value' found".to_string()))?;
                            column_changes.push(ValveChange {
                                column: column.to_string(),
                                level: "delete".to_string(),
                                old_value: old_value.to_string(),
                                value: "".to_string(),
                                message: "".to_string(),
                            });
                        }
                        _ => {
                            return Err(
                                ValveError::InputError(format!("Invalid change: {change}")).into()
                            )
                        }
                    };
                }
                Ok(ValveRowChange {
                    history_id: history_id,
                    table: table.to_string(),
                    row: row_number,
                    message: format!("Delete row {} from '{}'", row_number, table),
                    changes: column_changes,
                })
            }
            (None, Some(to)) => {
                let mut column_changes = vec![];
                for (column, change) in &to {
                    match change {
                        SerdeValue::Number(rn) if column == "previous_row" => {
                            column_changes.push(ValveChange {
                                column: column.to_string(),
                                level: "insert".to_string(),
                                old_value: rn.to_string(),
                                value: "".to_string(),
                                message: "".to_string(),
                            })
                        }
                        SerdeValue::Object(details) => {
                            let value = details
                                .get("value")
                                .and_then(|s| Some(s.to_string()))
                                .ok_or(ValveError::InputError("No 'value' found".to_string()))?;
                            column_changes.push(ValveChange {
                                column: column.to_string(),
                                level: "insert".to_string(),
                                old_value: "".to_string(),
                                value: value.to_string(),
                                message: "".to_string(),
                            });
                        }
                        _ => {
                            return Err(
                                ValveError::InputError(format!("Invalid change: {change}")).into()
                            )
                        }
                    };
                }
                Ok(ValveRowChange {
                    history_id: history_id,
                    table: table.to_string(),
                    row: row_number,
                    message: format!("Add row {} to '{}'", row_number, table),
                    changes: column_changes,
                })
            }
            _ => Err(ValveError::InputError("Invalid undo/redo record".to_string()).into()),
        }
    }
}

// TODO: We should probably change the type of old_value and value to SerdeValue.
/// Represents a change to a value in a row of a database table.
#[derive(Debug, Deserialize, Serialize)]
pub struct ValveChange {
    /// The name of the column that the value is from
    pub column: String,
    /// The kind of change (update, insert, delete)
    pub level: String,
    /// The previous contents of this column value
    pub old_value: String,
    /// The new contents of this column value
    pub value: String,
    /// A description of the change
    pub message: String,
}

/// Configuration information specific to Valve's special tables
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveSpecialConfig {
    /// The name of the table table
    pub table: String,
    /// The name of the column table
    pub column: String,
    /// The name of the datatype table
    pub datatype: String,
    /// The name of the rule table, or an empty string if there isn't any
    pub rule: String,
}

/// Configuration information for a particular table.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveTableConfig {
    /// The name of a table
    pub table: String,
    /// The table's type
    pub table_type: String,
    /// The options for this table
    pub options: HashSet<String>,
    /// A description of the table
    pub description: String,
    /// The location of the TSV file representing the table in the filesystem
    pub path: String,
    /// The table's column configuration
    pub column: HashMap<String, ValveColumnConfig>,
    /// The order in which the table's columns should appear in the database and in TSV files.
    pub column_order: Vec<String>,
}

/// Configuration information for a particular column of a particular table
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ValveColumnConfig {
    /// The table that the column belongs to
    pub table: String,
    /// The column's name
    pub column: String,
    /// The column's datatype
    pub datatype: String,
    /// The column's description
    pub description: String,
    /// The column's label
    pub label: String,
    /// The structural constraint that should be satisfied by all of the column's values
    pub structure: String,
    /// The datatype of the column's nulltype (or the empty string if the column has none)
    pub nulltype: String,
    /// The default for a column indicates which value should be inserted for the column in a given
    /// row when the value of that column has not been specified in an INSERT database statement.
    /// An empty string indicates that the column has no default.
    pub default: SerdeValue,
}

/// Configuration information for a particular datatype
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValveDatatypeConfig {
    /// The datatype's corresponding SQL type
    pub sql_type: String,
    /// The regular expression which all data values of this type must match
    pub condition: String,
    /// The name of the datatype
    pub datatype: String,
    /// The datatype's description
    pub description: String,
    /// The parent datatype of the datatype
    pub parent: String,
}

/// Configuration information for a particular table rule
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValveRuleConfig {
    /// The description of the rule
    pub description: String,
    /// The level of the rule (error, warning, info)
    pub level: String,
    /// The table with which the rule is associated
    pub table: String,
    /// The column on which the antecedent of the rule is based
    pub when_column: String,
    /// The condition that the antecedent column must satisfy for the rule to apply
    pub when_condition: String,
    /// The column to which the rule applies whenever the antecedent condition is satisfied
    pub then_column: String,
    /// The condition that the then_column must satisfy whenever the antecedent condition applies
    pub then_condition: String,
}

/// Configuration information for a particular 'tree' constraint
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValveTreeConstraint {
    /// The way that the tree constraint is represented in the source data
    pub original: String,
    /// The table that the tree constraint is defined with respect to
    pub table: String,
    /// The child node associated with this tree
    pub child: String,
    /// The child's parent
    pub parent: String,
}

/// Configuration information for a particular foreign key constraint
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValveForeignConstraint {
    /// The way that the foreign constraint is represented in the source data
    pub original: String,
    /// The table to which the column constrained by the key belongs
    pub table: String,
    /// The column constrained by the key
    pub column: String,
    /// The table referenced by the foreign key
    pub ftable: String,
    /// The column referenced by the foreign key
    pub fcolumn: String,
}

/// Configuration information for the constraints enforced by a particular Valve instance
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveConstraintConfig {
    /// A map from table names to that table's primary key constraints
    // TODO: primary would be better as HashMap<String, String>, since it is not possible to
    // have more than one primary key per table, but the below reflects the current implementation
    // which in principle allows for more than one.
    pub primary: HashMap<String, Vec<String>>,
    /// A map from table names to each given table's unique key constraints
    pub unique: HashMap<String, Vec<String>>,
    /// A map from table names to each given table's foreign key constraints
    pub foreign: HashMap<String, Vec<ValveForeignConstraint>>,
    /// A map from table names to each given table's tree constraints
    pub tree: HashMap<String, Vec<ValveTreeConstraint>>,
}

/// Configuration information for a particular Valve instance
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValveConfig {
    /// Configuration specific to Valve's special tables
    pub special: ValveSpecialConfig,
    /// A map from table names to the configuration information for that table
    pub table: HashMap<String, ValveTableConfig>,
    /// A list of table names in the order they appear in the "table" table.
    pub table_order: Vec<String>,
    /// A map from datatype names to the configuration information for that datatype
    pub datatype: HashMap<String, ValveDatatypeConfig>,
    /// A map from table names to a further map, for each column in the given table, to the
    /// conditional 'when-then' rules associated with that column. Note that 'associated with'
    /// means that the given column is the when-column of some rule defined on the table.
    pub rule: HashMap<String, HashMap<String, Vec<ValveRuleConfig>>>,
    /// Configuration specific to Valve's database and tree constraints
    pub constraint: ValveConstraintConfig,
}

impl fmt::Display for ValveConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let json = serde_json::to_string(&self);
        match json {
            Ok(json) => {
                write!(f, "{}", json)
            }
            Err(e) => {
                log::error!(
                    "Unable to render Valve configuration: {:?} as JSON: '{}'",
                    self,
                    e
                );
                Err(fmt::Error)
            }
        }
    }
}

/// Main entrypoint for the Valve API.
#[derive(Clone, Debug)]
pub struct Valve {
    /// The valve configuration map.
    pub config: ValveConfig,
    /// The full list of tables managed by valve, in dependency order.
    pub sorted_table_list: Vec<String>,
    /// Lists of tables that depend on a given table, indexed by table.
    pub table_dependencies_in: HashMap<String, Vec<String>>,
    /// Lists of tables that a given table depends on, indexed by table.
    pub table_dependencies_out: HashMap<String, Vec<String>>,
    /// A map from string representations of datatype conditions to the pre-compiled regexes
    /// associated with them.
    pub datatype_conditions: HashMap<String, CompiledCondition>,
    /// A map from string representations of rule conditions to the pre-compiled regexes
    /// associated with them.
    pub rule_conditions: HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    /// A map from string representations of structure conditions to the parsed versions
    /// associated with them.
    pub structure_conditions: HashMap<String, ParsedStructure>,
    /// The kind of database being managed.
    pub db_kind: DbKind,
    /// The string used to connect to the database:
    pub db_path: String,
    /// The database connection pool.
    pub pool: AnyPool,
    /// The user associated with this valve instance.
    pub user: String,
    /// When set to true, the valve instance produces more logging output.
    pub verbose: bool,
    /// When set to true, valve will ask for confirmation before automatically dropping/truncating
    /// database tables in order to satisfy a dependency.
    pub interactive: bool,
    /// Activates optimizations used for the initial loading of data.
    pub initial_load: bool,
    /// Private field used to store startup error messages. Note that these are also accessible via
    /// the 'message' database table. Startup messages represent errors and warnings that are
    /// encountered while configuring Valve which cannot be handled at load time. They are always
    /// associated with the 'table' table.
    startup_table_messages: IndexMap<u32, Vec<ValveMessage>>,
}

impl Valve {
    /// Given a path to a table table, a path to a database, and a flag indicating whether the
    /// database should be configured for initial loading: Set up a database connection, configure
    /// VALVE, and return a new Valve struct. Note that when `table_path` does not end
    /// (case-insensitively) in .tsv this argument is ignored, and the Valve configuration is read,
    /// instead, from a table called 'table' in the given `database`.
    pub async fn build(table_path: &str, database: &str) -> Result<Self> {
        let _ = env_logger::try_init();
        let pool = get_pool_from_connection_string(database).await?;
        let db_kind = DbKind::from_pool(&pool)?;
        if db_kind == DbKind::Sqlite {
            sqlx_query("PRAGMA foreign_keys = ON")
                .execute(&pool)
                .await?;
        }

        let parser = StartParser::new();
        let (
            specials_config,
            tables_config,
            table_order,
            datatypes_config,
            rules_config,
            constraints_config,
            sorted_table_list,
            table_dependencies_in,
            table_dependencies_out,
            startup_table_messages,
        ) = read_config_files(table_path, &parser, &pool).await?;

        let config = ValveConfig {
            special: specials_config,
            table: tables_config,
            table_order: table_order,
            datatype: datatypes_config,
            rule: rules_config,
            constraint: constraints_config,
        };

        let datatype_conditions = generate_datatype_conditions(&config, &parser)?;
        let rule_conditions = generate_rule_conditions(&config, &datatype_conditions, &parser)?;
        let structure_conditions = get_parsed_structure_conditions(&config, &parser)?;

        Ok(Self {
            config: config,
            sorted_table_list: sorted_table_list.clone(),
            table_dependencies_in: table_dependencies_in,
            table_dependencies_out: table_dependencies_out,
            datatype_conditions: datatype_conditions,
            rule_conditions: rule_conditions,
            structure_conditions: structure_conditions,
            db_kind: db_kind,
            db_path: database.to_string(),
            pool: pool,
            user: String::from("VALVE"),
            verbose: false,
            interactive: false,
            initial_load: false,
            startup_table_messages: startup_table_messages,
        })
    }

    /// Reload the valve configuration from the configuration table files.
    pub async fn reconfigure(&mut self) -> Result<()> {
        let table_path = self.get_path()?;
        let parser = StartParser::new();
        let (
            specials_config,
            tables_config,
            table_order,
            datatypes_config,
            rules_config,
            constraints_config,
            sorted_table_list,
            table_dependencies_in,
            table_dependencies_out,
            _startup_table_messages,
        ) = read_config_files(&table_path, &parser, &self.pool).await?;

        let config = ValveConfig {
            special: specials_config,
            table: tables_config,
            table_order: table_order,
            datatype: datatypes_config,
            rule: rules_config,
            constraint: constraints_config,
        };

        let datatype_conditions = generate_datatype_conditions(&config, &parser)?;
        let rule_conditions = generate_rule_conditions(&config, &datatype_conditions, &parser)?;
        let structure_conditions = get_parsed_structure_conditions(&config, &parser)?;

        self.config = config;
        self.sorted_table_list = sorted_table_list.clone();
        self.table_dependencies_in = table_dependencies_in;
        self.table_dependencies_out = table_dependencies_out;
        self.datatype_conditions = datatype_conditions;
        self.rule_conditions = rule_conditions;
        self.structure_conditions = structure_conditions;
        Ok(())
    }

    /// Configures a SQLite database session for initial loading by setting a number of PRAGMAs
    /// that are unsafe in general but appropriate when setting up a Valve database for the first
    /// time. Note that if Valve's managed database is not a SQLite database, calling this function
    /// has no effect. Note that once a database session has been configured for initial loading,
    /// it cannot be unconfigured. To connect to the database in a normal manner it is required to
    /// create a new [Valve] instance.
    pub async fn configure_for_initial_load(&mut self) -> Result<&mut Self> {
        if self.initial_load {
            Ok(self)
        } else {
            self.initial_load = true;
            if self.db_kind == DbKind::Sqlite {
                // These pragmas are unsafe but they are used during initial loading since data
                // integrity is not a priority in this case.
                self.execute_sql("PRAGMA journal_mode = OFF").await?;
                self.execute_sql("PRAGMA synchronous = 0").await?;
                self.execute_sql("PRAGMA cache_size = 1000000").await?;
                self.execute_sql("PRAGMA temp_store = MEMORY").await?;
            }
            Ok(self)
        }
    }

    /// Convenience function to retrieve the path to Valve's "table table", the main entrypoint
    /// to Valve's configuration.
    pub fn get_path(&self) -> Result<String> {
        Ok(self
            .config
            .table
            .get(&self.config.special.table)
            .and_then(|t| Some(t.path.as_str()))
            .ok_or(ValveError::ConfigError(
                "Table table is undefined".to_string(),
            ))?
            .to_string())
    }

    /// The maximum length of a username.
    pub const USERNAME_MAX_LEN: usize = 20;

    /// Sets the user name, which must be a short (see [USERNAME_MAX_LEN](Self::USERNAME_MAX_LEN)),
    /// trimmed, string without newlines, for this Valve instance.
    pub fn set_user(&mut self, user: &str) -> Result<&mut Self> {
        if user.len() > Self::USERNAME_MAX_LEN {
            return Err(ValveError::InputError(format!(
                "Username '{}' is longer than {} characters.",
                user,
                Self::USERNAME_MAX_LEN
            ))
            .into());
        } else {
            let user_regex = Regex::new(r#"^\S([^\n]*\S)*$"#)?;
            if !user_regex.is_match(user) {
                return Err(ValveError::InputError(format!(
                    "Username '{}' is not a short, trimmed, string without newlines.",
                    user,
                ))
                .into());
            }
        }
        self.user = user.to_string();
        Ok(self)
    }

    /// Configure verbosity
    pub fn set_verbose(&mut self, verbose: bool) -> &mut Self {
        self.verbose = verbose;
        self
    }

    /// Configure interactive mode
    pub fn set_interactive(&mut self, interactive: bool) -> &mut Self {
        self.interactive = interactive;
        self
    }

    /// Given a map from datatype table column names to their corresponding values, add the
    /// given datatype to Valve.
    pub async fn add_datatype(&mut self, dt_map: &JsonRow) -> Result<()> {
        // Begin a transaction:
        let mut tx = self.pool.begin().await?;

        // Add the datatype and record the change:
        let (rn, ro) = add_datatype_tx(self, dt_map, &mut tx).await?;
        let mut dt_map = dt_map.clone();
        dt_map.insert("row_order".to_string(), json!(ro));
        record_config_change_tx(
            &self.config.special.datatype,
            &rn,
            None,
            Some(&dt_map),
            &self.user,
            &mut tx,
            &self.db_kind,
        )
        .await?;

        // Commit the transaction:
        tx.commit().await?;

        // Save the datatype table and then reconfigure valve:
        self.save_tables(&vec![&self.config.special.datatype], &None)
            .await?;
        self.reconfigure().await
    }

    /// Given the name of a datatype, delete it from the datatype table.
    pub async fn delete_datatype(&mut self, datatype: &str) -> Result<()> {
        // Begin a transaction:
        let mut tx = self.pool.begin().await?;

        // Retrieve the datatype configuration, which we will save in case the delete operation
        // needs to be undone later:
        let dt_config = self
            .config
            .datatype
            .get(datatype)
            .ok_or(ValveError::InputError(format!(
                "No datatype found '{datatype}'",
            )))?;
        let dt_config = json!(dt_config);
        let mut dt_config = dt_config
            .as_object()
            .ok_or(ValveError::ConfigError(format!(
                "Not an object '{:?}'",
                dt_config
            )))?
            .clone();

        // Delete the datatype and store a record of the change in the history table:
        let (dt_rn, dt_ro) = delete_datatype_tx(self, datatype, &mut tx).await?;
        dt_config.insert("row_order".to_string(), json!(dt_ro));
        record_config_change_tx(
            &self.config.special.datatype,
            &dt_rn,
            Some(&dt_config),
            None,
            &self.user,
            &mut tx,
            &self.db_kind,
        )
        .await?;

        // Commit the transaction:
        tx.commit().await?;

        // Save the datatype table and then reconfigure valve:
        self.save_tables(&vec![&self.config.special.datatype], &None)
            .await?;
        self.reconfigure().await
    }

    /// Change the name of the given datatype to the given new name.
    pub async fn rename_datatype(&mut self, datatype: &str, new_name: &str) -> Result<()> {
        // Begin a transaction:
        let mut tx = self.pool.begin().await?;

        // Rename the datatype:
        let (dt_rn, _) = rename_datatype_tx(&self, &mut tx, datatype, new_name).await?;

        // Save a record of the renaming to the history table:
        let mut from = JsonRow::new();
        let mut to = JsonRow::new();
        from.insert("datatype".to_string(), datatype.into());
        to.insert("datatype".to_string(), new_name.into());
        record_config_change_tx(
            &self.config.special.datatype,
            &dt_rn,
            Some(&from),
            Some(&to),
            &self.user,
            &mut tx,
            &self.db_kind,
        )
        .await?;

        // Commit the transaction:
        tx.commit().await?;

        // Save the column table and the data table and then reconfigure valve:
        self.save_tables(
            &vec![
                &self.config.special.datatype,
                &self.config.special.column,
                &self.config.special.rule,
            ],
            &None,
        )
        .await?;
        self.reconfigure().await
    }

    /// Given a table name, the path of the table's associated TSV file, and other parameters
    /// used as input to the function, [guess()], guess the table's configuration on the basis
    /// of the contents of the TSV file and the other, and add it to the database.
    pub async fn add_table(
        &mut self,
        table: &str,
        path: &str,
        sample_size: &usize,
        error_rate: &f32,
        seed: &Option<u64>,
    ) -> Result<()> {
        // Begin a transaction:
        let mut tx = self.pool.begin().await?;

        // Use guess() to propose a table and column configuration:
        let table_added = guess(
            self,
            &mut tx,
            Some(table),
            path,
            seed,
            sample_size,
            error_rate,
        )
        .await;

        if table_added {
            // Read the current table and column configurations from the database:
            let table_config =
                read_db_table_into_vector_tx(&mut tx, &self.config.special.table, true).await?;
            let table_config = table_config
                .iter()
                .filter(|t| t.get("table").unwrap() == table)
                .collect::<Vec<_>>();
            let column_config =
                read_db_table_into_vector_tx(&mut tx, &self.config.special.column, true).await?;
            let column_config = column_config
                .iter()
                .filter(|t| t.get("table").unwrap() == table)
                .collect::<Vec<_>>();

            // The `type` column of the table table maps to the `table_type` field of the
            // ValveTableConfig struct, so we make the correction here:
            let table_config = {
                let mut corrected_table_config = JsonRow::new();
                for (key, value) in table_config[0].iter() {
                    if key == "type" {
                        corrected_table_config.insert(format!("table_{key}"), value.clone());
                    } else {
                        corrected_table_config.insert(format!("{key}"), value.clone());
                    }
                }
                corrected_table_config
            };

            // Extract the row number from the table table configuration so that we can save it
            // to the history record
            let rn = match table_config.get("row_number") {
                Some(SerdeValue::Number(n)) => match n.as_i64() {
                    Some(rn) => rn as u32,
                    _ => {
                        return Err(
                            ValveError::DataError("Can't convert row number".to_string()).into(),
                        )
                    }
                },
                _ => {
                    return Err(
                        ValveError::DataError("Row number is not a number".to_string()).into(),
                    )
                }
            };

            // The `to` field of the history table entry contains the table and column configuration
            // of the table that was just added:
            let mut to = JsonRow::new();
            to.insert("table".to_string(), json!(table_config));
            to.insert("column".to_string(), json!(column_config));

            record_config_change_tx(
                &self.config.special.table,
                &rn,
                None,
                Some(&to),
                &self.user,
                &mut tx,
                &self.db_kind,
            )
            .await?;

            // Commit the transaction:
            tx.commit().await?;

            // Save the configuration, reconfigure valve, and create the data table:
            self.save_tables(
                &vec![&self.config.special.table, &self.config.special.column],
                &None,
            )
            .await?;
            self.reconfigure().await?;
            self.ensure_all_tables_created(&vec![table]).await?;
        }
        Ok(())
    }

    /// Delete the given table from the table and column configuration and drop it.
    pub async fn delete_table(&mut self, table: &str) -> Result<()> {
        // Begin a DB transaction:
        let mut tx = self.pool.begin().await?;

        // Save the row number that is associated with this table in the table table for later:
        let (table_rn, table_ro) = {
            let table_table = &self.config.special.table;
            let sql = local_sql_syntax(
                &self.db_kind,
                &format!(
                    r#"SELECT "row_number", "row_order"
                       FROM "{table_table}"
                       WHERE "table" = {SQL_PARAM}"#
                ),
            );
            let row = sqlx_query(&sql).bind(table).fetch_one(&self.pool).await?;
            let rn = row.try_get::<i64, _>("row_number").unwrap();
            let ro = row.try_get::<i64, _>("row_order").unwrap();
            (rn as u32, ro as u32)
        };

        // Save the row numbers that are associated with this table's columns in the column table
        // for later:
        let (column_rns, column_ros) = {
            let mut column_rns = HashMap::new();
            let mut column_ros = HashMap::new();
            let column_table = &self.config.special.column;
            let sql = local_sql_syntax(
                &self.db_kind,
                &format!(
                    r#"SELECT "column", "row_number", "row_order"
                       FROM "{column_table}"
                       WHERE "table" = {SQL_PARAM}"#
                ),
            );
            let rows = sqlx_query(&sql).bind(table).fetch_all(&self.pool).await?;
            for row in &rows {
                let column: &str = row.get("column");
                let rn = row.get::<i64, _>("row_number") as u32;
                let ro = row.get::<i64, _>("row_order") as u32;
                column_rns.insert(column.to_string(), rn);
                column_ros.insert(column.to_string(), ro);
            }
            (column_rns, column_ros)
        };

        // Drop the table and delete it from the table and column configuration in the database:
        delete_table_tx(&self, table, &mut tx, &self.db_kind).await?;

        // Get the table config and use it to populate the `from` field for the record that we
        // will save to the history table:
        let tconfig = self
            .config
            .table
            .get(table)
            .ok_or(ValveError::InputError(format!("No table found '{table}'")))?;

        let from = {
            let mut from = JsonRow::new();

            let mut from_table = JsonRow::new();
            from_table.insert("description".to_string(), json!(tconfig.description));
            from_table.insert("path".to_string(), json!(tconfig.path));
            from_table.insert("row_number".to_string(), json!(table_rn));
            from_table.insert("row_order".to_string(), json!(table_ro));
            from_table.insert("table".to_string(), json!(tconfig.table));
            from_table.insert("table_type".to_string(), json!(tconfig.table_type));
            from_table.insert(
                "options".to_string(),
                json!(tconfig.options.iter().join(" ")),
            );
            from.insert("table".to_string(), json!(from_table));

            let mut from_columns = vec![];
            for (column, details) in tconfig.column.iter() {
                let mut this_column = JsonRow::new();
                this_column.insert("column".to_string(), json!(details.column));
                this_column.insert("datatype".to_string(), json!(details.datatype));
                this_column.insert("default".to_string(), json!(details.default));
                this_column.insert("description".to_string(), json!(details.description));
                this_column.insert("label".to_string(), json!(details.label));
                this_column.insert("nulltype".to_string(), json!(details.nulltype));
                this_column.insert("structure".to_string(), json!(details.structure));
                this_column.insert("table".to_string(), json!(details.table));
                let column_rn = column_rns.get(column).ok_or(ValveError::DataError(format!(
                    "Row number not found for '{column}'"
                )))?;
                this_column.insert("row_number".to_string(), json!(column_rn));
                let column_ro = column_ros.get(column).ok_or(ValveError::DataError(format!(
                    "Row order not found for '{column}'"
                )))?;
                this_column.insert("row_order".to_string(), json!(column_ro));
                from_columns.push(this_column);
            }
            from.insert("column".to_string(), json!(from_columns));

            from
        };

        record_config_change_tx(
            &self.config.special.table,
            &table_rn,
            Some(&from),
            None,
            &self.user,
            &mut tx,
            &self.db_kind,
        )
        .await?;

        // Commit the transaction:
        tx.commit().await?;

        // Save the column and table tables and reconfigure valve:
        self.save_tables(
            &vec![&self.config.special.table, &self.config.special.column],
            &None,
        )
        .await?;
        self.reconfigure().await
    }

    /// Change the name of the given table to the given new name.
    pub async fn rename_table(&mut self, table: &str, new_name: &str) -> Result<()> {
        if table == &self.config.special.table {
            return Err(ValveError::InputError("Can't rename the table table".to_string()).into());
        }
        let mut tx = self.pool.begin().await?;

        // Rename the table in the database:
        let (table_rn, _) = rename_table_tx(&self, &mut tx, table, new_name).await?;

        let mut from = JsonRow::new();
        let mut to = JsonRow::new();
        from.insert("table".to_string(), table.into());
        to.insert("table".to_string(), new_name.into());
        record_config_change_tx(
            &self.config.special.table,
            &table_rn,
            Some(&from),
            Some(&to),
            &self.user,
            &mut tx,
            &self.db_kind,
        )
        .await?;

        // Commit the transaction:
        tx.commit().await?;

        // Save the column table and the data table and then reconfigure valve:
        self.save_tables(
            &vec![
                &self.config.special.table,
                &self.config.special.column,
                &self.config.special.rule,
            ],
            &None,
        )
        .await?;
        self.reconfigure().await
    }

    /// Add a column to the database corresponding to the given column details. If table is not
    /// given, then the column details must include a "table" field. If column is not given, then
    /// the column details must include a "column" field.
    pub async fn add_column(
        &mut self,
        table: &Option<String>,
        column: &Option<String>,
        column_details: &JsonRow,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        let make_err =
            |err_str: &str| -> ValveError { ValveError::InputError(err_str.to_string()) };

        // Extract the required and optional fields from the column_details JSON:
        let table = match column_details.get("table") {
            Some(input_table) => match table {
                Some(table) if table != input_table => {
                    return Err(make_err(
                        "Mismatch between input table and positional parameter, TABLE",
                    )
                    .into())
                }
                None | Some(_) => input_table.as_str().ok_or(make_err("Not a string"))?,
            },
            None => match table {
                Some(table) => table,
                None => return Err(make_err("No table given").into()),
            },
        };
        let column = match column_details.get("column") {
            Some(input_column) => match column {
                Some(column) if column != input_column => {
                    return Err(make_err(
                        "Mismatch between input column and positional parameter, COLUMN",
                    )
                    .into())
                }
                None | Some(_) => input_column.as_str().ok_or(make_err("Not a string"))?,
            },
            None => match column {
                Some(column) => column,
                None => return Err(make_err("No column given").into()),
            },
        };

        // Add the column using the low-level toolkit function:
        let (rn, ro) = add_column_tx(self, table, column, column_details, &mut tx).await?;
        let mut column_details = column_details.clone();
        column_details.insert("row_order".to_string(), json!(ro));

        // Record the configuration change to the history table:
        record_config_change_tx(
            &self.config.special.column,
            &rn,
            None,
            Some(&column_details),
            &self.user,
            &mut tx,
            &self.db_kind,
        )
        .await?;

        // Commit:
        tx.commit().await?;

        // Save the column table and the data table and then reconfigure valve:
        self.save_tables(&vec![&self.config.special.column, table], &None)
            .await?;
        self.reconfigure().await?;

        // Refresh the database but do not load any data:
        self.ensure_all_tables_created(&vec![table]).await?;
        Ok(())
    }

    /// Given a table and column name, delete the column from the table.
    pub async fn delete_column(&mut self, table: &str, column: &str) -> Result<()> {
        // Begin a transaction:
        let mut tx = self.pool.begin().await?;

        let colconfig = self
            .config
            .table
            .get(table)
            .ok_or(ValveError::InputError(format!("No table found '{table}'",)))?
            .column
            .get(column)
            .ok_or(ValveError::InputError(format!(
                "No column found '{column}'",
            )))?;

        let colconfig = json!(colconfig);
        let mut colconfig = colconfig
            .as_object()
            .ok_or(ValveError::InputError(format!(
                "Not an object '{:?}'",
                colconfig
            )))?
            .clone();

        let (col_rn, col_ro) = delete_column_tx(self, table, column, &mut tx).await?;
        colconfig.insert("row_order".to_string(), json!(col_ro));

        record_config_change_tx(
            &self.config.special.column,
            &col_rn,
            Some(&colconfig),
            None,
            &self.user,
            &mut tx,
            &self.db_kind,
        )
        .await?;

        // Commit:
        tx.commit().await?;

        // Save the column table and the data table and then reconfigure valve:
        self.save_tables(&vec![&self.config.special.column, table], &None)
            .await?;
        self.reconfigure().await?;

        // Refresh the database:
        self.ensure_all_tables_created(&vec![table]).await?;

        Ok(())
    }

    /// Rename the given column from the given table to the given new name, optionally with the
    /// given label.
    pub async fn rename_column(
        &mut self,
        table: &str,
        column: &str,
        new_name: &str,
        new_label: &Option<String>,
    ) -> Result<()> {
        // Begin a transaction:
        let mut tx = self.pool.begin().await?;

        // We also need to rename the column name of the data table in the database, because
        // otherwise, when we call save_table() on the data table, when it tries to find the
        // newly renamed column it won't be there and it will save all the values in the column
        // as null.
        rename_column_tx(&self, &mut tx, table, column, new_name, new_label).await?;

        // Save the row number of the column which we will later add to the history record:
        let (col_rn, _) = {
            let column_table = &self.config.special.column;
            let sql = local_sql_syntax(
                &self.db_kind,
                &format!(
                    r#"SELECT "row_number", "row_order"
                       FROM "{column_table}"
                       WHERE "table" = {SQL_PARAM} AND "column" = {SQL_PARAM}"#
                ),
            );
            let row = sqlx_query(&sql)
                .bind(table)
                .bind(column)
                .fetch_one(&self.pool)
                .await?;
            let rn = row.try_get::<i64, _>("row_number").unwrap_or_default();
            let ro = row.try_get::<i64, _>("row_order").unwrap_or_default();
            (rn as u32, ro as u32)
        };

        let mut from = JsonRow::new();
        let mut to = JsonRow::new();
        from.insert("table".to_string(), table.into());
        from.insert("column".to_string(), column.into());
        let old_label = &self
            .config
            .table
            .get(table)
            .ok_or(ValveError::ConfigError(format!(
                "Table config for '{table}' not found"
            )))?
            .column
            .get(column)
            .ok_or(ValveError::ConfigError(format!(
                "Column config for '{table}.{column}' not found"
            )))?
            .label;
        if old_label != "" {
            from.insert("label".to_string(), json!(old_label));
        }
        to.insert("table".to_string(), table.into());
        to.insert("column".to_string(), new_name.into());
        if let Some(new_label) = new_label {
            to.insert("label".to_string(), json!(new_label));
        }
        record_config_change_tx(
            &self.config.special.column,
            &col_rn,
            Some(&from),
            Some(&to),
            &self.user,
            &mut tx,
            &self.db_kind,
        )
        .await?;

        // Commit the transaction:
        tx.commit().await?;

        // Save the column and rule tables, and the data table, and then reconfigure valve:
        self.save_tables(
            &vec![
                &self.config.special.column,
                &self.config.special.rule,
                table,
            ],
            &None,
        )
        .await?;
        self.reconfigure().await?;

        // Refresh the database but do not load any data:
        self.ensure_all_tables_created(&vec![table]).await?;

        Ok(())
    }

    /// (Private function.) Given a SQL string, execute it using the connection pool associated
    /// with the Valve instance.
    async fn execute_sql(&self, sql: &str) -> Result<()> {
        sqlx_query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    /// (Private function.) Given a filename containing SQL statements, execute the statements
    /// contained in the file.
    async fn execute_sql_file(&self, path: &str) -> Result<()> {
        let sql = std::fs::read_to_string(path)?;
        let sql_lines = sql_split::split(&sql);
        for line in &sql_lines {
            self.execute_sql(&line).await?;
        }
        Ok(())
    }

    /// (Private function.) Given the filename of an executable script, execute it and return the
    /// result.
    pub fn execute_script(&self, path: &str, args: &Vec<&str>) -> Result<()> {
        let mut command = Command::new(path);
        if !args.is_empty() {
            command.args(args);
        }
        let output = command.output()?;
        let exit_code = output.status.code();
        match exit_code {
            None => {
                let stdout = std::str::from_utf8(&output.stdout)?;
                let stderr = std::str::from_utf8(&output.stdout)?;
                Err(ValveError::DataError(format!(
                    "Execution of '{}' was interrupted. STDOUT: '{}', STDERR: '{}'.",
                    path, stdout, stderr
                ))
                .into())
            }
            Some(exit_code) if exit_code != 0 => {
                let stdout = std::str::from_utf8(&output.stdout)?;
                let stderr = std::str::from_utf8(&output.stdout)?;
                Err(ValveError::DataError(format!(
                    "Execution of '{}' failed with exit code {}. STDOUT: '{}', STDERR: '{}'.",
                    path, exit_code, stdout, stderr
                ))
                .into())
            }
            _ => Ok(()),
        }
    }

    /// Return the list of configured tables that have the given option in sorted order, or
    /// reverse sorted order if the reverse flag is set.
    pub fn get_sorted_table_list_with_option(&self, option: &str, reverse: bool) -> Vec<&str> {
        let mut sorted_tables = self
            .sorted_table_list
            .iter()
            .filter(|t| {
                let table_options = self
                    .get_table_options_from_config(t)
                    .expect(&format!("Error getting options for table '{}'", t));
                table_options.contains(option)
            })
            .map(|i| i.as_str())
            .collect::<Vec<_>>();
        if reverse {
            sorted_tables.reverse();
        }
        sorted_tables
    }

    /// Return the list of configured tables in sorted order.
    pub fn get_sorted_table_list(&self) -> Vec<&str> {
        self.sorted_table_list
            .iter()
            .map(|i| i.as_str())
            .collect::<Vec<_>>()
    }

    /// Return the list of configured tables in reverse sorted order.
    pub fn get_reverse_sorted_table_list(&self) -> Vec<&str> {
        let mut sorted_tables = self.get_sorted_table_list();
        sorted_tables.reverse();
        sorted_tables
    }

    /// Given a subset of the configured tables, return them in reverse dependency order.
    pub fn reverse_sort_tables(&self, table_subset: &Vec<&str>) -> Result<Vec<String>> {
        let mut sorted_subset = self.sort_tables(table_subset)?;
        sorted_subset.reverse();
        Ok(sorted_subset)
    }

    /// Given a subset of the configured tables, return them sorted in dependency order.
    pub fn sort_tables(&self, table_subset: &Vec<&str>) -> Result<Vec<String>> {
        let full_table_list = self.get_sorted_table_list();
        if !table_subset
            .iter()
            .all(|item| full_table_list.contains(item))
        {
            return Err(ValveError::InputError(format!(
                "[{}] contains tables that are not in the configured table list: [{}]",
                table_subset.join(", "),
                full_table_list.join(", ")
            ))
            .into());
        }

        // Filter out internal tables since they are not represented in the constraints config and
        // anyway they will be added implicitly later when we call verify_table_deps_and_sort():
        let filtered_subset = table_subset
            .iter()
            .filter(|m| !INTERNAL_TABLES.contains(m))
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        let (sorted_subset, _, _) =
            verify_table_deps_and_sort(&filtered_subset, &self.config.constraint);

        // Since the result of verify_table_deps_and_sort() will include dependencies of the tables
        // in its input list, we filter those out here:
        Ok(sorted_subset
            .iter()
            .filter(|m| table_subset.contains(&m.as_str()))
            .map(|s| s.to_string())
            .collect::<Vec<_>>())
    }

    /// Get all the incoming (tables that depend on it) or outgoing (tables it depends on)
    /// dependencies of the given table.
    pub fn get_dependencies(&self, table: &str, incoming: bool) -> Result<Vec<String>> {
        let mut dependent_tables = vec![];
        if !INTERNAL_TABLES.contains(&table) {
            let direct_deps = {
                if incoming {
                    self.table_dependencies_in
                        .get(table)
                        .ok_or(ValveError::InputError(format!(
                            "Undefined table '{}'",
                            table
                        )))?
                        .to_vec()
                } else {
                    self.table_dependencies_out
                        .get(table)
                        .ok_or(ValveError::InputError(format!(
                            "Undefined table '{}'",
                            table
                        )))?
                        .to_vec()
                }
            };
            for direct_dep in direct_deps {
                let mut indirect_deps = self.get_dependencies(&direct_dep, incoming)?;
                dependent_tables.append(&mut indirect_deps);
                dependent_tables.push(direct_dep);
            }
        }
        Ok(dependent_tables)
    }

    /// Given a list of tables, fill it in with any further tables that are dependent upon tables
    /// in the given list. If deletion_order is true, the tables are sorted as required for
    /// deleting them all sequentially, otherwise they are ordered in reverse.
    pub fn add_dependencies(
        &self,
        tables: &Vec<&str>,
        deletion_order: bool,
    ) -> Result<Vec<String>> {
        let mut with_dups = vec![];
        for table in tables {
            let dependent_tables = self.get_dependencies(table, true)?;
            for dep_table in dependent_tables {
                with_dups.push(dep_table.to_string());
            }
            with_dups.push(table.to_string());
        }
        // The algorithm above gives the tables in the order needed for deletion. But we want
        // this function to return the creation order by default so we reverse it unless
        // the deletion_order flag is set to true.
        if !deletion_order {
            with_dups.reverse();
        }

        // Remove the duplicates from the returned table list:
        let mut tables_in_order = vec![];
        for table in with_dups.iter().unique() {
            tables_in_order.push(table.to_string());
        }
        Ok(tables_in_order)
    }

    /// Returns an IndexMap, indexed by configured table, containing lists of their dependencies.
    /// If incoming is true, the lists are incoming dependencies, else they are outgoing.
    pub fn collect_dependencies(&self, incoming: bool) -> Result<IndexMap<String, Vec<String>>> {
        let tables = self.get_sorted_table_list();
        let mut dependencies = IndexMap::new();
        for table in tables {
            dependencies.insert(table.to_string(), self.get_dependencies(table, incoming)?);
        }
        Ok(dependencies)
    }

    /// Given the name of a table, determine whether its current instantiation in the database
    /// differs from the way it has been configured. The answer to this question is yes whenever
    /// (1) the number of columns or any of their names differs from their configured values, or
    /// the order of database columns differs from the configured order; (2) The SQL type of one or
    /// more columns does not match the configured SQL type for that column; (3) Some column with a
    /// 'unique', 'primary', or 'from(table, column)' in its column configuration fails to be
    /// associated, in the database, with a unique constraint, primary key, or foreign key,
    /// respectively; or vice versa; (4) The table does not exist in the database. When
    /// [Valve::interactive] is set to true, this function will inform the user, via the terminal,
    /// whenever the table needs to be changed with an explanation of the reason why, and will
    /// wait for explicit confirmation before proceeding.
    pub async fn table_has_changed(&self, table: &str) -> Result<bool> {
        // A clojure that, given a parsed structure condition, a table and column name, and an
        // unsigned integer representing whether the given column, in the case of a SQLite database,
        // is a primary key (in the case of PostgreSQL, the sqlite_pk parameter is ignored):
        // determine whether the structure of the column is properly reflected in the db. E.g., a
        // `from(table.column)` struct should be associated with a foreign key, `primary` with a
        // primary key, `unique` with a unique constraint.
        let structure_has_changed = |pstruct: &Expression,
                                     table: &str,
                                     column: &str,
                                     sqlite_pk: &u32|
         -> Result<bool> {
            // A clojure to determine whether the given column has the given constraint type, which
            // can be one of 'UNIQUE', 'PRIMARY KEY', 'FOREIGN KEY':
            let column_has_constraint_type = |constraint_type: &str| -> Result<bool> {
                if self.db_kind == DbKind::Postgres {
                    let sql = format!(
                        r#"SELECT 1
                       FROM information_schema.table_constraints tco
                       JOIN information_schema.key_column_usage kcu
                         ON kcu.constraint_name = tco.constraint_name
                            AND kcu.constraint_schema = tco.constraint_schema
                            AND kcu.table_name = $1
                       WHERE tco.constraint_type = $2
                         AND kcu.column_name = $3"#,
                    );
                    let query = sqlx_query(&sql)
                        .bind(table)
                        .bind(constraint_type)
                        .bind(column);
                    let rows = block_on(query.fetch_all(&self.pool))?;
                    if rows.len() > 1 {
                        unreachable!();
                    }
                    Ok(rows.len() == 1)
                } else {
                    if constraint_type == "PRIMARY KEY" {
                        return Ok(*sqlite_pk == 1);
                    } else if constraint_type == "UNIQUE" {
                        let sql = format!(r#"PRAGMA INDEX_LIST("{}")"#, table);
                        for row in block_on(sqlx_query(&sql).fetch_all(&self.pool))? {
                            let idx_name = row.get::<String, _>("name");
                            let unique = row.get::<i16, _>("unique") as u8;
                            if unique == 1 {
                                let sql = format!(r#"PRAGMA INDEX_INFO("{}")"#, idx_name);
                                let rows = block_on(sqlx_query(&sql).fetch_all(&self.pool))?;
                                if rows.len() == 1 {
                                    let cname = rows[0].get::<String, _>("name");
                                    if cname == column {
                                        return Ok(true);
                                    }
                                }
                            }
                        }
                        Ok(false)
                    } else if constraint_type == "FOREIGN KEY" {
                        let sql = format!(r#"PRAGMA FOREIGN_KEY_LIST("{}")"#, table);
                        for row in block_on(sqlx_query(&sql).fetch_all(&self.pool))? {
                            let cname = row.get::<String, _>("from");
                            if cname == column {
                                return Ok(true);
                            }
                        }
                        Ok(false)
                    } else {
                        return Err(ValveError::InputError(
                            format!("Unrecognized constraint type: '{}'", constraint_type).into(),
                        )
                        .into());
                    }
                }
            };

            // Check if there is a change to whether this column is a primary/unique key:
            let is_primary = match pstruct {
                Expression::Label(label) if label == "primary" => true,
                _ => false,
            };
            if is_primary != column_has_constraint_type("PRIMARY KEY")? {
                return Ok(true);
            } else if !is_primary {
                let is_unique = match pstruct {
                    Expression::Label(label) if label == "unique" => true,
                    _ => false,
                };
                let unique_in_db = column_has_constraint_type("UNIQUE")?;
                if is_unique != unique_in_db {
                    // A child of a tree constraint implies a unique db constraint, so if there is a
                    // unique constraint in the db that is not configured, that is the explanation,
                    // and in that case we do not count this as a change to the column.
                    if !unique_in_db {
                        return Ok(true);
                    } else {
                        let trees = self
                            .config
                            .constraint
                            .tree
                            .get(table)
                            .and_then(|v| Some(v.iter().map(|t| t.child.to_string())))
                            .ok_or(ValveError::ConfigError(format!(
                                "Could not determine trees for table '{}'",
                                table
                            )))?
                            .collect::<Vec<_>>();
                        if !trees.contains(&column.to_string()) {
                            return Ok(true);
                        }
                    }
                }
            }

            match pstruct {
                Expression::Function(name, args) if name == "from" => {
                    match &*args[0] {
                        Expression::Field(cfg_ftable, cfg_fcolumn) => {
                            if self.db_kind == DbKind::Sqlite {
                                let sql = format!(r#"PRAGMA FOREIGN_KEY_LIST("{}")"#, table);
                                for row in block_on(sqlx_query(&sql).fetch_all(&self.pool))? {
                                    let from = row.get::<String, _>("from");
                                    if from == column {
                                        let db_ftable = row.get::<String, _>("table");
                                        let db_fcolumn = row.get::<String, _>("to");
                                        if *cfg_ftable != db_ftable || *cfg_fcolumn != db_fcolumn {
                                            return Ok(true);
                                        }
                                    }
                                }
                            } else {
                                let sql = format!(
                                    r#"SELECT
                                       ccu.table_name AS foreign_table_name,
                                       ccu.column_name AS foreign_column_name
                                   FROM information_schema.table_constraints AS tc
                                   JOIN information_schema.key_column_usage AS kcu
                                     ON tc.constraint_name = kcu.constraint_name
                                        AND tc.table_schema = kcu.table_schema
                                   JOIN information_schema.constraint_column_usage AS ccu
                                     ON ccu.constraint_name = tc.constraint_name
                                   WHERE tc.constraint_type = 'FOREIGN KEY'
                                     AND tc.table_name = $1
                                     AND kcu.column_name = $2"#,
                                );
                                let query = sqlx_query(&sql).bind(table).bind(column);
                                let rows = block_on(query.fetch_all(&self.pool))?;
                                if rows.len() == 0 {
                                    // If the table doesn't even exist return true.
                                    return Ok(true);
                                } else if rows.len() > 1 {
                                    // This seems impossible given how PostgreSQL works:
                                    unreachable!();
                                } else {
                                    let row = &rows[0];
                                    let db_ftable = row.get::<String, _>("foreign_table_name");
                                    let db_fcolumn = row.get::<String, _>("foreign_column_name");
                                    if *cfg_ftable != db_ftable || *cfg_fcolumn != db_fcolumn {
                                        return Ok(true);
                                    }
                                }
                            }
                        }
                        _ => {
                            return Err(ValveError::InputError(
                                format!("Unrecognized structure: {:?}", pstruct).into(),
                            )
                            .into());
                        }
                    };
                }
                _ => (),
            };

            Ok(false)
        };

        let table_config = self.get_table_config(table)?;
        let (columns_config, configured_column_order) = {
            let columns_config = &table_config.column;
            let configured_column_order = {
                let mut configured_column_order = {
                    // These special identifier columns must go first:
                    if table == "message" {
                        vec!["message_id".to_string()]
                    } else if table == "history" {
                        vec!["history_id".to_string()]
                    } else {
                        vec!["row_number".to_string(), "row_order".to_string()]
                    }
                };
                configured_column_order.append(&mut table_config.column_order.clone());
                configured_column_order
            };

            (columns_config, configured_column_order)
        };

        let db_columns_in_order = {
            if self.db_kind == DbKind::Sqlite {
                let sql = format!(
                    r#"SELECT 1 FROM sqlite_master WHERE "type" = 'table' AND "name" = ?"#,
                );
                let rows = sqlx_query(&sql).bind(table).fetch_all(&self.pool).await?;
                if rows.len() == 0 {
                    if self.verbose {
                        println!(
                            "The table '{}' will be created as it does not exist in the \
                             database.",
                            table
                        );
                    }
                    return Ok(true);
                } else if rows.len() == 1 {
                    // Otherwise send another query to the db to get the column info:
                    let sql = format!(r#"PRAGMA TABLE_INFO("{}")"#, table);
                    let rows = block_on(sqlx_query(&sql).fetch_all(&self.pool))?;
                    rows.iter()
                        .map(|r| {
                            (
                                r.get::<String, _>("name"),
                                r.get::<String, _>("type"),
                                r.get::<i64, _>("pk") as u32,
                            )
                        })
                        .collect::<Vec<_>>()
                } else {
                    unreachable!();
                }
            } else {
                let sql = format!(
                    r#"SELECT "column_name", "data_type"
                         FROM "information_schema"."columns"
                        WHERE "table_name" = $1
                       ORDER BY "ordinal_position""#,
                );
                let rows = sqlx_query(&sql).bind(table).fetch_all(&self.pool).await?;
                if rows.len() == 0 {
                    if self.verbose {
                        println!(
                            "The table '{}' will be created as it does not exist in the \
                             database.",
                            table
                        );
                    }
                    return Ok(true);
                }
                // Otherwise we get the column name:
                rows.iter()
                    .map(|r| {
                        (
                            r.get::<String, _>("column_name"),
                            r.get::<String, _>("data_type"),
                            // The third entry is just a dummy so that the datatypes in the two
                            // wings of this if/else block match.
                            0,
                        )
                    })
                    .collect::<Vec<_>>()
            }
        };

        // Check if the order of the configured columns matches the order of the columns in the
        // database:
        let db_column_order = db_columns_in_order
            .iter()
            .map(|c| c.0.clone())
            .collect::<Vec<_>>();
        if db_column_order != configured_column_order {
            if self.verbose || self.interactive {
                print!(
                    "The table '{}' needs to be recreated because the database columns: {:?} \
                     and/or their order do not match the configured columns: {:?}. ",
                    table, db_column_order, configured_column_order
                );
                if self.interactive {
                    print!("Do you want to continue? [y/N] ");
                    if !proceed::proceed() {
                        return Err(
                            ValveError::UserError("Execution aborted by user".to_string()).into(),
                        );
                    }
                } else {
                    println!();
                }
            }
            return Ok(true);
        }

        // Check, for all tables, whether their column configuration matches the contents of the
        // database:
        for (cname, ctype, pk) in &db_columns_in_order {
            // Do not consider these special columns:
            if (table == "message" && cname == "message_id")
                || (table == "message" && cname == "row")
                || (table == "history" && cname == "history_id")
                || (table == "history" && cname == "timestamp")
                || (table == "history" && cname == "row")
                || cname == "row_number"
                || cname == "row_order"
            {
                continue;
            }
            let column_config =
                columns_config
                    .get(cname)
                    .ok_or(ValveError::ConfigError(format!(
                        "Undefined column '{}'",
                        cname
                    )))?;
            let sql_type =
                get_sql_type_from_global_config(&self.config, table, &cname, &self.db_kind);

            // Check the column's SQL type:
            if sql_type.to_lowercase() != ctype.to_lowercase() {
                let s = sql_type.to_lowercase();
                let c = ctype.to_lowercase();
                // CHARACTER VARYING and VARCHAR are synonyms so we ignore this difference.
                if !((s.starts_with("varchar") || s.starts_with("character varying"))
                    && (c.starts_with("varchar") || c.starts_with("character varying")))
                {
                    if self.verbose || self.interactive {
                        print!(
                            "The table '{}' needs to be recreated because the SQL type of column \
                             '{}', {}, does not match the configured value: {}. ",
                            table, cname, ctype, sql_type
                        );
                        if self.interactive {
                            print!("Do you want to continue? [y/N] ");
                            if !proceed::proceed() {
                                return Err(ValveError::UserError(
                                    "Execution aborted by user".to_string(),
                                )
                                .into());
                            }
                        } else {
                            println!();
                        }
                    }
                    return Ok(true);
                }
            }

            // Check the column's structure:
            let structure = &column_config.structure;
            if structure != "" {
                let parsed_structure = self
                    .structure_conditions
                    .get(structure)
                    .and_then(|p| Some(p.parsed.clone()))
                    .ok_or(ValveError::ConfigError(format!(
                        "Undefined structure '{}'",
                        structure
                    )))?;

                // If the column's datatype is a list datatype, or if it has a from() constraint
                // that references a view, these do not entail constraints and can be ignored:
                match self.get_value_type(table, cname) {
                    ValueType::List(_, _) => continue,
                    _ => match &parsed_structure {
                        Expression::Function(name, args) if name == "from" => {
                            match &*args[0] {
                                Expression::Field(ftable, _) => {
                                    let ftable_config = self.config.table.get(ftable).ok_or(
                                        ValveError::ConfigError(format!(
                                            "Undefined table '{}'",
                                            ftable
                                        )),
                                    )?;
                                    if ftable_config.options.contains("db_view") {
                                        continue;
                                    }
                                }
                                _ => (),
                            };
                        }
                        _ => (),
                    },
                };

                if structure_has_changed(&parsed_structure, table, &cname, &pk)? {
                    if self.verbose || self.interactive {
                        print!(
                            "The table '{}' needs to be recreated because the database \
                             constraints for column '{}' do not match the configured \
                             structure, '{}'. ",
                            table, cname, structure
                        );
                        if self.interactive {
                            print!("Do you want to continue? [y/N] ");
                            if !proceed::proceed() {
                                return Err(ValveError::UserError(
                                    "Execution aborted by user".to_string(),
                                )
                                .into());
                            }
                        } else {
                            println!();
                        }
                    }
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Given the name of a table, returns its table configuration.
    pub fn get_table_config(&self, table: &str) -> Result<&ValveTableConfig> {
        self.config
            .table
            .get(table)
            .ok_or(ValveError::ConfigError(format!("Undefined table '{}'", table)).into())
    }

    /// Given a table configuration struct, a datatype configuration struct, a parser, a table name,
    /// and a database connection pool, return a list of DDL statements that can be used to create
    /// the database tables.
    pub fn get_table_ddl(&self, table_name: &String, kind: &DbKind) -> Result<Vec<String>> {
        let tables_config = &self.config.table;
        let datatypes_config = &self.config.datatype;

        let mut statements = vec![];
        let mut create_lines = vec![
            format!(r#"CREATE TABLE "{}" ("#, table_name),
            String::from(r#"  "row_number" BIGINT,"#),
            String::from(r#"  "row_order" BIGINT,"#),
        ];

        let normal_table_name;
        if let Some(s) = table_name.strip_suffix("_conflict") {
            normal_table_name = String::from(s);
        } else {
            normal_table_name = table_name.to_string();
        }

        let column_configs = {
            let column_order = &tables_config
                .get(&normal_table_name)
                .expect(&format!("Undefined table '{}'", normal_table_name))
                .column_order;
            let columns = &tables_config
                .get(&normal_table_name)
                .expect(&format!("Undefined table '{}'", normal_table_name))
                .column;

            column_order
                .iter()
                .map(|column_name| columns.get(column_name).unwrap())
                .collect::<Vec<_>>()
        };

        let (primaries, uniques, foreigns, _trees) = {
            // Conflict tables have no database constraints:
            if table_name.ends_with("_conflict") {
                (vec![], vec![], vec![], vec![])
            } else {
                let cons = &self.config.constraint;
                let primaries = cons.primary.get(table_name).cloned().unwrap_or(vec![]);
                let uniques = cons.unique.get(table_name).cloned().unwrap_or(vec![]);
                let foreigns = cons.foreign.get(table_name).cloned().unwrap_or(vec![]);
                let trees = cons.tree.get(table_name).cloned().unwrap_or(vec![]);
                (primaries, uniques, foreigns, trees)
            }
        };

        let applicable_foreigns = foreigns
            .iter()
            .filter(|fkey| {
                let vtype = self.get_value_type(&normal_table_name, &fkey.column);
                let table_config = &tables_config
                    .get(&fkey.ftable)
                    .expect(&format!("Undefined table '{}'", fkey.ftable));
                vtype == ValueType::Single && !table_config.options.contains("db_view")
            })
            .collect::<Vec<_>>();

        let c = column_configs.len();
        let mut r = 0;
        for column_config in column_configs {
            r += 1;
            let sql_type = get_sql_type(&datatypes_config, &column_config.datatype, kind);

            let short_sql_type = {
                if sql_type.to_lowercase().as_str().starts_with("varchar(") {
                    "VARCHAR"
                } else {
                    &sql_type
                }
            };

            if !SQL_TYPES.contains(&short_sql_type.to_lowercase().as_str()) {
                panic!(
                    "Unrecognized SQL type '{}' for datatype: '{}'. Accepted SQL types are: {}",
                    sql_type,
                    column_config.datatype,
                    SQL_TYPES.join(", ")
                );
            }

            let column_name = &column_config.column;
            let mut line = format!(r#"  "{}" {}"#, column_name, sql_type);

            // Check if the column is a primary key and indicate this in the DDL if so:
            if primaries.contains(&column_name) {
                line.push_str(" PRIMARY KEY");
            }

            // Check if the column has a unique constraint and indicate this in the DDL if so:
            if uniques.contains(&column_name) {
                line.push_str(" UNIQUE");
            }

            // Check if the column has a default value and indicate this in the DDL if it does:
            let default_value = match &column_config.default {
                SerdeValue::String(s) if s == "" => "".to_string(),
                SerdeValue::String(s) => format!("'{}'", s),
                SerdeValue::Number(n) => n.to_string(),
                SerdeValue::Null => "".to_string(),
                _ => panic!(
                    "Configured default value, {:?}, of column '{}' is neither \
                     a number nor a string.",
                    column_config.default, column_name
                ),
            };
            if default_value != "" {
                line.push_str(&format!(" DEFAULT {}", default_value));
            }

            // If there are foreign constraints add a column to the end of the statement which we will
            // finish after this for loop is done (but don't do this for views):
            if !(r >= c && applicable_foreigns.is_empty()) {
                line.push_str(",");
            }
            create_lines.push(line);
        }

        // Add the SQL to indicate any foreign constraints:
        let num_fkeys = applicable_foreigns.len();
        let mut foreigns_added = 0;
        for fkey in foreigns.iter() {
            if self.get_value_type(&normal_table_name, &fkey.column) == ValueType::Single {
                let ftable_options = {
                    let table_config = &tables_config
                        .get(&fkey.ftable)
                        .expect(&format!("Undefined table '{}'", fkey.ftable));
                    table_config.options.clone()
                };
                if !ftable_options.contains("db_view") {
                    create_lines.push(format!(
                        r#"  FOREIGN KEY ("{}") REFERENCES "{}"("{}"){}"#,
                        fkey.column,
                        fkey.ftable,
                        fkey.fcolumn,
                        if foreigns_added < (num_fkeys - 1) {
                            ","
                        } else {
                            ""
                        }
                    ));
                    foreigns_added += 1;
                }
            }
        }
        create_lines.push(String::from(");"));
        // We are done generating the lines for the 'create table' statement. Join them and add the
        // result to the statements to return:
        statements.push(String::from(create_lines.join("\n")));

        // Finally, create further unique indexes on row_number and row_order:
        statements.push(format!(
            r#"DROP INDEX IF EXISTS "{}_row_number_idx";"#,
            table_name,
        ));
        statements.push(format!(
            r#"DROP INDEX IF EXISTS "{}_row_order_idx";"#,
            table_name,
        ));
        statements.push(format!(
            r#"CREATE UNIQUE INDEX "{}_row_number_idx" ON "{}"("row_number");"#,
            table_name, table_name
        ));
        statements.push(format!(
            r#"CREATE UNIQUE INDEX "{}_row_order_idx" ON "{}"("row_order");"#,
            table_name, table_name
        ));

        Ok(statements)
    }

    /// Generates and returns the DDL required to setup the database.
    pub async fn get_setup_statements(&self) -> Result<HashMap<String, Vec<String>>> {
        let tables_config = &self.config.table;
        let datatypes_config = &self.config.datatype;

        // Begin by reading in the TSV files corresponding to the tables defined in tables_config,
        // and use that information to create the associated database tables, while saving
        // constraint information to constrains_config.
        let mut setup_statements = HashMap::new();
        for (table, table_config) in tables_config.iter() {
            // Generate DDL for the table and its corresponding conflict table:
            let mut table_statements = vec![];
            let mut statements = self.get_table_ddl(&table, &self.db_kind)?;
            table_statements.append(&mut statements);
            if table_config.options.contains("conflict") {
                let cable = format!("{}_conflict", table);
                let mut statements = self.get_table_ddl(&cable, &self.db_kind)?;
                table_statements.append(&mut statements);

                let create_view_sql = get_sql_for_standard_view(&table, &self.db_kind);
                let create_text_view_sql =
                    get_sql_for_text_view(tables_config, &table, None, None, &self.db_kind);
                table_statements.push(create_view_sql);
                table_statements.push(create_text_view_sql);
            }
            setup_statements.insert(table.to_string(), table_statements);
        }

        let text_type = get_sql_type(datatypes_config, &"text".to_string(), &self.db_kind);

        // Generate DDL for the history and message tables:
        let history_statements = generate_internal_table_ddl("history", &self.db_kind, &text_type);
        setup_statements.insert("history".to_string(), history_statements);
        let message_statements = generate_internal_table_ddl("message", &self.db_kind, &text_type);
        setup_statements.insert("message".to_string(), message_statements);

        return Ok(setup_statements);
    }

    /// Writes the database schema to stdout.
    pub async fn dump_schema(&self) -> Result<String> {
        let setup_statements = self.get_setup_statements().await?;
        let mut output = String::from("");
        for table in self.get_sorted_table_list_with_option("edit", false) {
            let table_statements =
                setup_statements
                    .get(table)
                    .ok_or(ValveError::ConfigError(format!(
                        "Could not find setup statements for {}",
                        table
                    )))?;
            let table_output = String::from(table_statements.join("\n"));
            output.push_str(&table_output);
            output.push_str("\n\n");
        }
        Ok(output)
    }

    /// Create all configured database tables and views if they do not already exist as configured.
    /// If a table needs to be (re)created, ask the user for confirmation before proceeding unless
    /// the table is in the `no_prompt` list.
    pub async fn ensure_all_tables_created(&mut self, no_prompt: &Vec<&str>) -> Result<&Self> {
        let setup_statements = self.get_setup_statements().await?;
        let sorted_table_list = self
            .get_sorted_table_list()
            .iter()
            .map(|tbl| tbl.to_string())
            .collect::<Vec<_>>();
        for table in &sorted_table_list {
            let table_config = self.get_table_config(table)?;
            if table_config.options.contains("db_view") {
                // If the path points to a .sql file or an executable file, execute the statements
                // that are contained in it against the database, trusting that they are correct.
                // Note that the SQL script (or executable file), not Valve, is responsible for
                // deciding whether the view actually needs to be recreated. In any case, once any
                // specified scripts have been run (if the path is empty then Valve assumes that the
                // view has already been set up in the database), Valve checks to make sure that the
                // view now exists and returns an error if it does not.
                if table_config.path.to_lowercase().ends_with(".sql") {
                    self.execute_sql_file(&table_config.path).await?;
                } else if table_config.path != "" {
                    self.execute_script(&table_config.path, &vec![&self.db_path, table])?;
                }
                // Check to make sure that the view now exists:
                if !self.view_exists(table).await? {
                    return Err(ValveError::DataError(format!(
                        "No view named '{}' exists in the database",
                        table
                    ))
                    .into());
                }
            } else {
                let saved_interactive = self.interactive;
                self.set_interactive(!no_prompt.contains(&table.as_str()));
                if self.table_has_changed(table).await? {
                    self.drop_tables(&vec![table]).await?;
                    let table_statements =
                        setup_statements
                            .get(table)
                            .ok_or(ValveError::ConfigError(format!(
                                "Could not find setup statements for {}",
                                table
                            )))?;
                    for stmt in table_statements {
                        self.execute_sql(stmt).await?;
                    }
                }
                self.set_interactive(saved_interactive);
            }
        }

        Ok(self)
    }

    /// Checks whether the given table exists in the database.
    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        let sql = {
            if self.db_kind == DbKind::Sqlite {
                format!(
                    r#"SELECT 1
                       FROM "sqlite_master"
                       WHERE "type" = 'table' AND name = ?
                       LIMIT 1"#,
                )
            } else {
                format!(
                    r#"SELECT 1
                       FROM "information_schema"."tables"
                       WHERE "table_schema" = 'public'
                         AND "table_name" = $1
                         AND "table_type" LIKE '%TABLE'"#,
                )
            }
        };
        let query = sqlx_query(&sql).bind(table);
        let rows = query.fetch_all(&self.pool).await?;
        return Ok(rows.len() > 0);
    }

    /// Checks whether the given view exists in the database.
    pub async fn view_exists(&self, view: &str) -> Result<bool> {
        let sql = {
            if self.db_kind == DbKind::Sqlite {
                format!(
                    r#"SELECT 1
                       FROM "sqlite_master"
                       WHERE "type" = 'view' AND name = ?
                       LIMIT 1"#,
                )
            } else {
                format!(
                    r#"SELECT 1
                       FROM "information_schema"."tables"
                       WHERE "table_schema" = 'public'
                         AND "table_name" = $1
                         AND "table_type" = 'VIEW'"#,
                )
            }
        };
        let query = sqlx_query(&sql).bind(view);
        let rows = query.fetch_all(&self.pool).await?;
        return Ok(rows.len() > 0);
    }

    /// Given a table name, returns the options of the table.
    pub fn get_table_options_from_config(&self, table: &str) -> Result<HashSet<String>> {
        toolkit::get_table_options_from_config(&self.config, table)
    }

    /// Drop all configured tables, in reverse dependency order.
    pub async fn drop_all_tables(&mut self) -> Result<&Self> {
        // We turn off interactive mode for all "all" operations:
        let saved_interactive = self.interactive;
        self.set_interactive(false);

        // Drop all of the editable database tables in the reverse of their sorted order:
        self.drop_tables(&self.get_reverse_sorted_table_list())
            .await?;

        // Set interactive mode back to what it was before:
        self.set_interactive(saved_interactive);
        Ok(self)
    }

    /// Given a vector of table names, drop those tables, in the given order, including any tables
    /// that must be dropped implicitly because of a dependency relationship. Note that if
    /// [Valve::interactive] is set, and if more tables need to be dropped than were requested,
    /// Valve will prompt the user for confirmation via the terminal.
    pub async fn drop_tables(&self, tables: &Vec<&str>) -> Result<&Self> {
        let drop_list = self.add_dependencies(tables, true)?;
        if self.interactive {
            let drop_set = drop_list.iter().collect::<HashSet<_>>();
            let table_set = tables.iter().map(|t| t.to_string()).collect::<HashSet<_>>();
            let table_set = table_set.iter().collect::<HashSet<_>>();
            let extra_tables = drop_set.difference(&table_set).collect::<Vec<_>>();
            if !extra_tables.is_empty() {
                print!(
                    "In order to drop: {tables:?}, Valve must also drop the following tables: \
                     {extra_tables:?}. ",
                );
                print!("Do you want to continue? [y/N] ");
                if !proceed::proceed() {
                    return Err(
                        ValveError::UserError("Execution aborted by user".to_string()).into(),
                    );
                }
            }
        }
        for table in &drop_list {
            let table_config = self.get_table_config(table)?;
            if table_config.path != "" {
                if table_config.options.contains("conflict") {
                    let sql = format!(r#"DROP VIEW IF EXISTS "{}_text_view""#, table);
                    self.execute_sql(&sql).await?;
                    let sql = format!(r#"DROP VIEW IF EXISTS "{}_view""#, table);
                    self.execute_sql(&sql).await?;
                    let sql = format!(r#"DROP TABLE IF EXISTS "{}_conflict""#, table);
                    self.execute_sql(&sql).await?;
                }
                let type_to_drop = match table_config.options.contains("db_view") {
                    true => "VIEW",
                    false => "TABLE",
                };
                let sql = format!(r#"DROP {} IF EXISTS "{}""#, type_to_drop, table);
                self.execute_sql(&sql).await?;
            }
        }

        Ok(self)
    }

    /// Truncate all configured tables, in reverse dependency order.
    pub async fn truncate_all_tables(&mut self) -> Result<&Self> {
        // We turn off interactive mode for all "all" operations:
        let saved_interactive = self.interactive;
        self.set_interactive(false);

        // This makes Rust's borrow checker happy. The issue is that get_sorted_table_list()
        // is defined for an immutable self, but in the rest of the current function, self is
        // borrowed as mutable. The solution is to clone the table list, which we do in two steps:
        let sorted_editable_tables = self
            .get_sorted_table_list_with_option("edit", true)
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>();
        let sorted_editable_tables = sorted_editable_tables
            .iter()
            .map(|t| t.as_str())
            .collect::<Vec<_>>();

        self.truncate_tables(&sorted_editable_tables).await?;

        // Set the interactive move back to what it was before:
        self.set_interactive(saved_interactive);
        Ok(self)
    }

    /// Given a vector of table names, truncate those tables, in the given order, including any
    /// tables that must be truncated implicitly because of a dependency relationship. Note that if
    /// [Valve::interactive] is set, and if more tables need to be truncated than were requested,
    /// Valve will prompt the user for confirmation via the terminal.
    pub async fn truncate_tables(&mut self, tables: &Vec<&str>) -> Result<&Self> {
        self.ensure_all_tables_created(tables).await?;
        let truncate_list = self.add_dependencies(tables, true)?;
        if self.interactive {
            let truncate_set = truncate_list.iter().collect::<HashSet<_>>();
            let table_set = tables.iter().map(|t| t.to_string()).collect::<HashSet<_>>();
            let table_set = table_set.iter().collect::<HashSet<_>>();
            let extra_tables = truncate_set.difference(&table_set).collect::<Vec<_>>();
            if !extra_tables.is_empty() {
                print!(
                    "The tables: {tables:?} need to be truncated, but in order to do so, \
                     Valve must also truncate the following tables: {extra_tables:?}. ",
                );
                print!("Do you want to continue? [y/N] ");
                if !proceed::proceed() {
                    return Err(
                        ValveError::UserError("Execution aborted by user".to_string()).into(),
                    );
                }
            }
        }

        // We must use CASCADE in the case of PostgreSQL since we cannot truncate a table, T, that
        // depends on another table, T', even in the case where we have previously truncated T'.
        // SQLite does not need this. However SQLite does require that the tables be truncated in
        // deletion order (which means that it must be checking that T' is empty).
        let truncate_sql = |table: &str| -> String {
            if self.db_kind == DbKind::Postgres {
                format!(r#"TRUNCATE TABLE "{}" RESTART IDENTITY CASCADE"#, table)
            } else {
                format!(r#"DELETE FROM "{}""#, table)
            }
        };

        for table in &truncate_list {
            let table_options = self.get_table_options_from_config(table)?;
            if table_options.contains("truncate") {
                let sql = truncate_sql(&table);
                self.execute_sql(&sql).await?;
                if table_options.contains("conflict") {
                    let sql = truncate_sql(&format!("{}_conflict", table));
                    self.execute_sql(&sql).await?;
                }
            }
        }

        Ok(self)
    }

    /// Load all configured tables in dependency order. If `validate` is false, just try to insert
    /// all rows, irrespective of whether they are valid or not or will possibly trigger a db error.
    pub async fn load_all_tables(&mut self, validate: bool) -> Result<&Self> {
        // This makes Rust's borrow checker happy. The issue is that get_sorted_table_list()
        // is defined for an immutable self, but in the rest of the current function, self is
        // borrowed as mutable. The solution is to clone the table list, which we do in two steps:
        let table_list = self
            .get_sorted_table_list()
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>();
        let table_list = table_list.iter().map(|t| t.as_str()).collect::<Vec<_>>();

        if self.verbose {
            println!("Processing {} tables.", table_list.len());
        }

        // We turn off interactive mode for all "all" operations:
        let saved_interactive = self.interactive;
        self.set_interactive(false);

        self.load_tables(&table_list, validate).await?;

        // Clear the startup messages, which will have been inserted during loading:
        self.startup_table_messages.clear();

        // Set interactive mode back to what it was before:
        self.set_interactive(saved_interactive);
        Ok(self)
    }

    /// Given a vector of table names, truncate each table, as well as any that depend on a given
    /// table via a foreign key dependency, then load the tables in `table_list` in the given order.
    /// If `validate` is false, just try to insert all rows, irrespective of whether they are valid
    /// or not or whether they may trigger a db error, otherwise all rows are validated as they are
    /// inserted to the database. Note that if other tables need to be truncated in order to load
    /// any tables on the list---for instance if tableA is to be (re)loaded, and there is a table
    /// called tableB which has a foreign key referencing tableA, then before (re)loading tableA
    /// we need to truncate tableB---those tables will not automatically be reloaded as well.
    pub async fn load_tables(&mut self, table_list: &Vec<&str>, validate: bool) -> Result<&Self> {
        let list_for_truncation = self.reverse_sort_tables(table_list)?;
        self.truncate_tables(
            &list_for_truncation
                .iter()
                .map(|i| i.as_str())
                .collect::<Vec<_>>(),
        )
        .await?;

        // Insert any 'startup' messages into the message table. These are messages generated
        // during the configuration stage.
        for (row, messages) in self.startup_table_messages.iter() {
            for msg in messages {
                let msg_sql = format!(
                    r#"INSERT INTO "message"
                       ("table", "row", "column", "value", "level", "rule", "message")
                       VALUES ('table', {}, '{}', '{}', '{}', '{}', '{}')"#,
                    row, msg.column, msg.value, msg.level, msg.rule, msg.message
                );
                self.execute_sql(&msg_sql).await?;
            }
        }

        let num_tables = table_list.len();
        let mut total_errors = 0;
        let mut total_warnings = 0;
        let mut total_infos = 0;
        let mut table_num = 1;

        let mut load_normal_table = |table_name: &str| -> Result<()> {
            let table_name = table_name.to_string();
            let path = String::from(
                &self
                    .config
                    .table
                    .get(&table_name)
                    .ok_or(ValveError::InputError(format!(
                        "Undefined table '{}'",
                        table_name
                    )))?
                    .path,
            );
            let mut rdr = {
                match File::open(path.clone()) {
                    Err(e) => {
                        log::warn!("Unable to open '{}': {}", path.clone(), e);
                        return Ok(());
                    }
                    Ok(table_file) => ReaderBuilder::new()
                        .has_headers(false)
                        .delimiter(b'\t')
                        .from_reader(table_file),
                }
            };
            if self.verbose {
                println!("Loading table {}/{}: {}", table_num, num_tables, table_name);
            }
            table_num += 1;

            // Extract the headers, which we will need later:
            let mut records = rdr.records();
            let headers = {
                let labels = {
                    if let Some(result) = records.next() {
                        result.unwrap()
                    } else {
                        return Err(ValveError::DataError(format!("'{}' is empty", path)).into());
                    }
                };
                let column_config = &self.get_table_config(&table_name)?.column;
                let mut headers = vec![];
                for label in &labels {
                    headers.push(get_column_for_label(&column_config, label, &table_name)?);
                }
                headers
            };
            for header in headers.iter() {
                if header.trim().is_empty() {
                    return Err(ValveError::DataError(format!(
                        "One or more of the header fields is empty for table '{}'",
                        table_name
                    ))
                    .into());
                }
            }

            // HashMap used to report info about the number of error/warning/info messages for this
            // table when the verbose flag is set to true:
            let mut messages_stats = HashMap::new();
            messages_stats.insert("error".to_string(), 0);
            messages_stats.insert("warning".to_string(), 0);
            messages_stats.insert("info".to_string(), 0);

            // Split the data into chunks of size CHUNK_SIZE before passing them to the validation
            // logic:
            let chunks = records.chunks(CHUNK_SIZE);
            block_on(insert_chunks(
                &self.config,
                &self.pool,
                &self.datatype_conditions,
                &self.rule_conditions,
                &table_name,
                &chunks,
                &headers,
                &mut messages_stats,
                self.verbose,
                validate,
            ))?;

            if validate {
                // We need to wait until all of the rows for a table have been loaded before
                // validating the "foreign" constraints on a table's trees, since this checks if
                // the values of one column (the tree's parent) are all contained in another column
                // (the tree's child).
                let recs_to_update = block_on(validate_tree_foreign_keys(
                    &self.config,
                    &self.pool,
                    None,
                    &table_name,
                    None,
                ))?;

                for record in recs_to_update {
                    let row_number = record.get("row_number").unwrap();
                    let column_name = record.get("column").and_then(|s| s.as_str()).unwrap();
                    let value = record.get("value").and_then(|s| s.as_str()).unwrap();
                    let level = record.get("level").and_then(|s| s.as_str()).unwrap();
                    let rule = record.get("rule").and_then(|s| s.as_str()).unwrap();
                    let message = record.get("message").and_then(|s| s.as_str()).unwrap();

                    let sql = local_sql_syntax(
                        &self.db_kind,
                        &format!(
                            r#"INSERT INTO "message"
                               ("table", "row", "column", "value", "level", "rule", "message")
                               VALUES ({}, {}, {}, {}, {}, {}, {})"#,
                            SQL_PARAM,
                            row_number,
                            SQL_PARAM,
                            SQL_PARAM,
                            SQL_PARAM,
                            SQL_PARAM,
                            SQL_PARAM
                        ),
                    );
                    let mut query = sqlx_query(&sql);
                    query = query.bind(&table_name);
                    query = query.bind(&column_name);
                    query = query.bind(&value);
                    query = query.bind(&level);
                    query = query.bind(&rule);
                    query = query.bind(&message);
                    block_on(query.execute(&self.pool))?;

                    if self.verbose {
                        // Add the generated message to messages_stats:
                        let messages = vec![ValveCellMessage {
                            message: message.to_string(),
                            level: level.to_string(),
                            ..Default::default()
                        }];
                        add_message_counts(&messages, &mut messages_stats);
                    }
                }
            }

            if self.verbose {
                // Output a report on the messages generated to stderr:
                let errors = messages_stats.get("error").unwrap();
                let warnings = messages_stats.get("warning").unwrap();
                let infos = messages_stats.get("info").unwrap();
                let status_message = format!(
                    "{} errors, {} warnings, and {} information messages generated for {}",
                    errors, warnings, infos, table_name
                );
                println!("{}", status_message);
                total_errors += errors;
                total_warnings += warnings;
                total_infos += infos;
            }

            Ok(())
        };

        for table in table_list {
            let table_config = self.get_table_config(&table)?;

            if !table_config.options.contains("load") {
                continue;
            }
            // For all others, how they are loaded depends on the path, such that an empty
            // path implies that the table in question is not loaded by Valve at all.
            if table_config.path.to_lowercase().ends_with(".sql") {
                // SQL files:
                self.execute_sql_file(&table_config.path).await?;
            } else if table_config.path.to_lowercase().ends_with(".tsv") {
                // TSV files:
                load_normal_table(table)?;
            } else if table_config.path != "" {
                // Other types of scripts:
                self.execute_script(&table_config.path, &vec![&self.db_path, table])?;
            }
        }

        if self.verbose {
            println!(
                "Loading complete with {} errors, {} warnings, and {} information messages",
                total_errors, total_warnings, total_infos
            );
        }
        Ok(self)
    }

    /// Returns true if the Valve instance has the given optional column enabled,
    /// according to the database.
    pub async fn column_enabled_in_db(&self, table: &str, column: &str) -> Result<bool> {
        if self.db_kind == DbKind::Postgres {
            let sql = format!(
                r#"SELECT 1
                     FROM "information_schema"."columns"
                    WHERE "table_name" = $1
                      AND "column_name" = $2
                    LIMIT 1"#,
            );
            let query = sqlx_query(&sql).bind(table).bind(column);
            Ok(!block_on(query.fetch_all(&self.pool))?.is_empty())
        } else {
            let sql = format!(r#"PRAGMA TABLE_INFO("{table}")"#);
            Ok(!block_on(sqlx_query(&sql).fetch_all(&self.pool))?
                .iter()
                .filter(|r| r.get::<&str, &str>("name") == column)
                .collect::<Vec<_>>()
                .is_empty())
        }
    }

    /// Save all configured savable tables to their configured paths, unless save_dir is specified,
    /// in which case save them all there instead.
    pub async fn save_all_tables(&self, save_dir: &Option<String>) -> Result<&Self> {
        let table_table = &self.config.special.table;
        let options_enabled = self.column_enabled_in_db(table_table, "options").await?;

        // Get the list of potential tables to save by querying the table table for all tables
        // whose paths are TSV files:
        let mut tables = vec![];
        let sql = {
            let select_from_table = String::from({
                if !options_enabled {
                    format!(r#"SELECT "table" FROM "{table_table}""#)
                } else {
                    format!(r#"SELECT "table", "options" FROM "{table_table}""#)
                }
            });
            if self.db_kind == DbKind::Postgres {
                format!(r#"{select_from_table} WHERE "path" ILIKE '%.tsv'"#)
            } else {
                format!(r#"{select_from_table} WHERE LOWER("path") LIKE '%.tsv'"#)
            }
        };
        let mut stream = sqlx_query(&sql).fetch(&self.pool);
        while let Some(row) = stream.try_next().await? {
            let table = row
                .try_get::<&str, &str>("table")
                .ok()
                .ok_or(ValveError::InputError(
                    "No column \"table\" found in row".to_string(),
                ))?;
            if options_enabled {
                let options = row
                    .try_get::<&str, &str>("options")
                    .unwrap_or_default()
                    .split(" ")
                    .collect::<Vec<_>>();
                let (options, _) = normalize_options(&options, 0)?;
                if options.contains("save") {
                    tables.push(table.to_string());
                }
            } else {
                // If the options column is missing from the table table, then save is enabled
                // by default if the table has a .tsv path (which we have already verified above).
                tables.push(table.to_string());
            }
        }

        let tables = tables.iter().map(|s| s.as_str()).collect_vec();
        self.save_tables(&tables, save_dir).await?;
        Ok(self)
    }

    /// Given a vector of table names, save those tables to their configured path's, unless
    /// save_dir is specified, in which case save them there instead.
    pub async fn save_tables(
        &self,
        tables: &Vec<&str>,
        save_dir: &Option<String>,
    ) -> Result<&Self> {
        if self.verbose {
            println!("Saving tables: {} ...", tables.join(", "));
        }
        // Build the query to get the path and possibly the options info from the table table for
        // all of the tables that were requested to be saved:
        let options_enabled = self
            .column_enabled_in_db(&self.config.special.table, "options")
            .await?;
        let mut params = vec![];
        let sql_param_str = tables
            .iter()
            .map(|table| {
                params.push(table);
                SQL_PARAM.to_string()
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = {
            let table_table = &self.config.special.table;
            if options_enabled {
                format!(
                    r#"SELECT "table", "path", "options" FROM "{table_table}" WHERE "table" IN ({})"#,
                    sql_param_str
                )
            } else {
                format!(
                    r#"SELECT "table", "path" FROM "{table_table}" WHERE "table" IN ({})"#,
                    sql_param_str
                )
            }
        };
        let sql = local_sql_syntax(&self.db_kind, &sql);
        let mut query = sqlx_query(&sql);
        for param in &params {
            query = query.bind(param);
        }

        // Query the db:
        let mut stream = query.fetch(&self.pool);
        while let Some(row) = stream.try_next().await? {
            let table = row
                .try_get::<&str, &str>("table")
                .ok()
                .ok_or(ValveError::InputError(
                    "No column \"table\" found in row".to_string(),
                ))?;

            let options = row
                .try_get::<&str, &str>("options")
                .unwrap_or_default()
                .split(" ")
                .collect::<Vec<_>>();
            let (options, _) = normalize_options(&options, 0)?;

            let path = row.try_get::<&str, &str>("path").ok().unwrap_or_default();
            let path = match save_dir {
                Some(save_dir) => {
                    // If the table is not saveable it can still be saved to the saved_dir if one
                    // has been specified and if it is not identical to the table's configured path:
                    if !options.contains("save") {
                        let path_dir = Path::new(path)
                            .parent()
                            .and_then(|n| n.to_str())
                            .unwrap_or("");
                        if path_dir == save_dir {
                            return Err(ValveError::InputError(format!(
                                "Option 'save' is not set for table '{}'. Cannot overwrite '{}'",
                                table, path
                            ))
                            .into());
                        }
                    }
                    format!(
                        "{}/{}",
                        save_dir,
                        Path::new(path).file_name().and_then(|n| n.to_str()).ok_or(
                            ValveError::InputError(format!("Unable to save to '{}'", path))
                        )?
                    )
                }
                None => {
                    if !options.contains("save") {
                        return Err(ValveError::InputError(format!(
                            "Saving '{}' is not supported",
                            table
                        ))
                        .into());
                    } else if !path.to_lowercase().ends_with(".tsv") {
                        return Err(ValveError::InputError(format!(
                            "Refusing to save to non-tsv file '{}'",
                            path
                        ))
                        .into());
                    }
                    path.to_string()
                }
            };
            self.save_table(table, path.as_str()).await?;
        }

        Ok(self)
    }

    /// Save the given table with the given columns at the given path as a TSV file.
    pub async fn save_table(&self, table: &str, save_path: &str) -> Result<&Self> {
        // Uses the given (unverified) printf-style format string and the given compiled regular
        // expression (which is used to verify the given format) to format the given cell.
        fn format_cell(colformat: &str, format_regex: &Regex, cell: &str) -> String {
            let conversion_spec = match format_regex.captures(colformat) {
                Some(c) => c[1].to_lowercase(),
                None => {
                    log::warn!("Illegal format: '{}'", colformat);
                    "s".to_string()
                }
            };
            let generic_error = format!("Error applying format '{}' to '{}':", colformat, cell);
            match conversion_spec.as_str() {
                "d" | "i" | "c" => match cell.parse::<isize>() {
                    Ok(cell) => match sprintf!(&colformat, cell) {
                        Ok(cell) => {
                            // For some reason sprintf converts signed ints to unsigned ints before
                            // converting them to a string. So we have to workaround this here:
                            let cell = cell.parse::<usize>().unwrap();
                            let cell = cell as isize;
                            cell.to_string()
                        }
                        Err(e) => {
                            log::warn!("{}: {}", generic_error, e);
                            cell.to_string()
                        }
                    },
                    Err(e) => {
                        log::warn!("{}: {}", generic_error, e);
                        cell.to_string()
                    }
                },
                "o" | "u" | "x" => match cell.parse::<usize>() {
                    Ok(cell) => sprintf!(&colformat, cell).unwrap_or(cell.to_string()),
                    Err(e) => {
                        log::warn!("{}: {}", generic_error, e);
                        cell.to_string()
                    }
                },
                "e" | "f" | "g" | "a" => match cell.parse::<f64>() {
                    Ok(cell) => sprintf!(&colformat, cell).unwrap_or(cell.to_string()),
                    Err(e) => {
                        log::warn!("{}: {}", generic_error, e);
                        cell.to_string()
                    }
                },
                "s" => sprintf!(&colformat, cell).unwrap_or(cell.to_string()),
                _ => {
                    log::warn!(
                        "Unsupported conversion specifier '{}' in column format '{}'",
                        conversion_spec,
                        colformat
                    );
                    cell.to_string()
                }
            }
        }

        // Check if we are allowed to save the table:
        if !save_path.to_lowercase().ends_with(".tsv") {
            return Err(ValveError::InputError(format!(
                "Refusing to save to non-tsv file '{}'",
                save_path
            ))
            .into());
        } else if self
            .column_enabled_in_db(&self.config.special.table, "options")
            .await?
        {
            let table_table = &self.config.special.table;
            let sql = local_sql_syntax(
                &self.db_kind,
                &format!(
                    r#"SELECT "path", "options" FROM "{table_table}" WHERE "table" = {}"#,
                    SQL_PARAM
                ),
            );
            let row = sqlx_query(&sql).bind(table).fetch_one(&self.pool).await?;
            let configured_path = row.try_get::<&str, &str>("path").unwrap_or_default();
            let options = row
                .try_get::<&str, &str>("options")
                .unwrap_or_default()
                .split(" ")
                .collect::<Vec<_>>();
            let (options, _) = normalize_options(&options, 0)?;
            if configured_path == save_path && !options.contains("save") {
                return Err(ValveError::ConfigError(format!(
                    "Saving '{}' to '{}' is not supported by table options: {}",
                    table,
                    save_path,
                    options.iter().join(", ")
                ))
                .into());
            };
        }

        // Begin by constructing a map from column names to their associated labels and formats, by
        // querying the column table joined with information from the table and datatype tables.
        let mut columns: IndexMap<String, (String, String)> = IndexMap::new();
        let sql = {
            let table_table = &self.config.special.table;
            let column_table = &self.config.special.column;
            let datatype_table = &self.config.special.datatype;
            let mut sql_columns = r#"c."column", c."label", t."type" AS "table_type""#.to_string();
            if self.column_enabled_in_db(datatype_table, "format").await? {
                sql_columns.push_str(r#", d."format""#);
            }
            local_sql_syntax(
                &self.db_kind,
                &format!(
                    r#"SELECT {sql_columns}
                         FROM "{column_table}" c
                         JOIN "{table_table}" t
                           ON c."table" = t."table"
                    LEFT JOIN "{datatype_table}" d
                           ON c."datatype" = d."datatype"
                        WHERE c."table" = t."table"
                          AND c."table" = {SQL_PARAM}
                    ORDER BY c."row_order""#,
                ),
            )
        };
        let mut stream = sqlx_query(&sql).bind(table).fetch(&self.pool);
        while let Some(row) = stream.try_next().await? {
            let column = row
                .try_get::<&str, &str>("column")
                .ok()
                .ok_or(ValveError::DataError(
                    "No column \"column\" found in row".to_string(),
                ))?;
            let label = row.try_get::<&str, &str>("label").ok().unwrap_or_default();
            let table_type = row
                .try_get::<&str, &str>("table_type")
                .ok()
                .unwrap_or_default();
            let format = row.try_get::<&str, &str>("format").ok().unwrap_or_default();
            // If the table type is not empty then it is either a special configuration table or an
            // internal table and, in either case, we want to ignore the label in the same way that
            // we ignore it when initially reading the configuration files. Possibly we will want to
            // allow labels for configuration table columns in the future, but for now we need to
            // make sure that they are treated consistently at load-time and at save-time.
            if label == "" || table_type != "" {
                columns.insert(column.into(), (column.into(), format.into()));
            } else {
                columns.insert(column.into(), (label.into(), format.into()));
            }
        }

        if columns.is_empty() {
            return Err(ValveError::DataError(format!("No table: '{table}' to save")).into());
        }

        // Query the database and use the results to construct the records that will be written
        // to the TSV file:
        let format_regex = Regex::new(PRINTF_RE)?;
        let mut writer = WriterBuilder::new()
            .delimiter(b'\t')
            .quote_style(QuoteStyle::Never)
            .from_path(save_path)?;
        let tsv_header_row = columns
            .iter()
            .map(|(_, (label, _))| label)
            .collect::<Vec<_>>();
        writer.write_record(tsv_header_row)?;

        let existing_columns = {
            let mut existing_columns = vec![];
            for column in columns.keys() {
                if self.column_enabled_in_db(table, column).await? {
                    existing_columns.push(column);
                }
            }
            existing_columns
        };

        // Construct the query to use to retrieve the data:
        let query_table = format!("\"{}_text_view\"", table);
        let sql = format!(
            r#"SELECT "row_number", {} FROM {} ORDER BY "row_order""#,
            existing_columns
                .iter()
                .map(|c| format!(r#""{}""#, c))
                .collect::<Vec<_>>()
                .join(", "),
            query_table
        );

        let mut stream = sqlx_query(&sql).fetch(&self.pool);
        while let Some(row) = stream.try_next().await? {
            let mut record: Vec<String> = vec![];
            for (column, (_, colformat)) in &columns {
                let cell = {
                    if existing_columns.contains(&column) {
                        row.try_get::<&str, &str>(column).ok().unwrap_or_default()
                    } else {
                        ""
                    }
                };
                if *colformat != "" {
                    let formatted_cell = format_cell(&colformat, &format_regex, &cell);
                    record.push(formatted_cell.to_string());
                } else {
                    record.push(cell.to_string());
                }
            }
            writer.write_record(record)?;
        }
        writer.flush()?;

        Ok(self)
    }

    /// Given a table name and a column name, get the value type of the column.
    pub fn get_value_type(&self, table: &str, column: &str) -> ValueType {
        toolkit::get_value_type(&self.config, &self.datatype_conditions, table, column)
    }

    /// Given the name of a datatype, find (in the database) the value of the optional configuration
    /// parameter called 'format' corresponding to that datatype and return it, or an empty string
    /// if no format parameter has been configured for that datatype or if the format parameter is
    /// not defined. The format parameter indicates how values of the given column are to be
    /// formatted when saving a table to an external file.
    pub async fn get_datatype_format(&self, datatype: &str) -> Result<String> {
        if !self
            .config
            .table
            .get(&self.config.special.datatype)
            .and_then(|t| Some(&t.column))
            .and_then(|c| Some(c.keys().collect::<Vec<_>>()))
            .and_then(|v| Some(v.contains(&&"format".to_string())))
            .expect("Could not find column configrations for the datatype table")
        {
            Ok("".to_string())
        } else {
            let datatype_table = &self.config.special.datatype;
            let sql = local_sql_syntax(
                &self.db_kind,
                &format!(
                    r#"SELECT "format"
                       FROM "{datatype_table}"
                       WHERE "datatype" = {SQL_PARAM}"#
                ),
            );
            let rows = sqlx_query(&sql)
                .bind(datatype)
                .fetch_all(&self.pool)
                .await?;
            if rows.len() > 0 {
                let format_string = rows[0]
                    .try_get::<&str, &str>("format")
                    .ok()
                    .expect("No column 'format' in row.");
                Ok(format_string.to_string())
            } else {
                Ok("".to_string())
            }
        }
    }

    /// Given a table name and the name of a column in that table, find (in the database) the value
    /// of the optional configuration parameter called 'format' and return it, or an empty string
    /// if no format parameter has been configured or if none has been defined for that column.
    /// The format parameter indicates how values of the given column are to be formatted when
    /// saving a table to an external file.
    pub async fn get_column_format(&self, table: &str, column: &str) -> Result<String> {
        if !self
            .config
            .table
            .get(&self.config.special.datatype)
            .and_then(|t| Some(t.column.clone()))
            .and_then(|c| Some(c.keys().cloned().collect::<Vec<_>>()))
            .and_then(|v| Some(v.contains(&"format".to_string())))
            .expect("Could not find column configrations for the datatype table")
        {
            Ok("".to_string())
        } else {
            let column_table = &self.config.special.column;
            let datatype_table = &self.config.special.datatype;
            let sql = local_sql_syntax(
                &self.db_kind,
                &format!(
                    r#"SELECT d."format"
                         FROM "{column_table}" c, "{datatype_table}" d
                        WHERE c."table" = {SQL_PARAM}
                          AND c."column" = {SQL_PARAM}
                          AND c."datatype" = d."datatype""#,
                ),
            );
            let rows = sqlx_query(&sql)
                .bind(table)
                .bind(column)
                .fetch_all(&self.pool)
                .await?;
            if rows.len() > 1 {
                panic!(
                    "Multiple entries corresponding to '{}' in datatype table",
                    column
                );
            }
            let format_string = rows[0]
                .try_get::<&str, &str>("format")
                .ok()
                .unwrap_or_default();
            Ok(format_string.to_string())
        }
    }

    /// Get the row number and row order of the next new row to be inserted.
    pub async fn get_next_new_row(&self, table: &str) -> Result<(u32, u32)> {
        let mut tx = self.pool.begin().await?;
        get_next_new_row_tx(&self.db_kind, &mut tx, table).await
    }

    /// Given a table name and a row number, return a [ValveRow] representing that row.
    pub async fn get_row_from_db(&self, table: &str, row_number: &u32) -> Result<ValveRow> {
        let mut tx = self.pool.begin().await?;
        let row = get_text_row_from_db_tx(&self.config, &self.db_kind, &mut tx, table, row_number)
            .await?;
        // The get_text_row_from_db_tx() function returns the row with all values as strings. The
        // reason is that that function uses the text view to retrieve all of the values even when
        // values have the wrong sql type. That means that in this function we need to correct
        // the incorrect SerdeValues in the row before we return it:
        let corrected_row = correct_row_datatypes(&self.config, &self.db_kind, table, &row)?;
        ValveRow::from_rich_json(Some(*row_number), &corrected_row)
    }

    /// Given a table name, a row number, and a column name, return a [ValveCell] representing
    /// the value of that column in that row.
    pub async fn get_cell_from_db(
        &self,
        table: &str,
        row_number: &u32,
        column: &str,
    ) -> Result<ValveCell> {
        let row = self.get_row_from_db(table, row_number).await?;
        row.contents
            .get(column)
            .and_then(|c| Some(c.clone()))
            .ok_or(ValveError::InputError(format!("No column named '{column}' in row")).into())
    }

    /// Given a table name and a row number, search for and return the row number of the row that
    /// is marked as previous to the given row.
    pub async fn get_previous_row(&self, table: &str, row: &u32) -> Result<u32> {
        let mut tx = self.pool.begin().await?;
        get_previous_row_tx(table, row, &mut tx).await
    }

    /// Given a table name and a row, represented as a JSON object in the following ('simple')
    /// format:
    /// ```
    /// {
    ///     "column_1": value1,
    ///     "column_2": value2,
    ///     ...
    /// },
    /// ```
    /// validate the row and return the results in the form of a [ValveRow].
    pub async fn validate_row(
        &self,
        table_name: &str,
        row: &JsonRow,
        row_number: Option<u32>,
    ) -> Result<ValveRow> {
        let row = ValveRow::from_simple_json(row, row_number)?;
        validate_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            None,
            table_name,
            &row,
            None,
        )
        .await
    }

    /// Given a table name and a row, represented as a JSON object in the following ('simple')
    /// format:
    /// ```
    /// {
    ///     "column_1": value1,
    ///     "column_2": value2,
    ///     ...
    /// },
    /// ```
    /// validate and insert the row to the table and return the row number of the inserted row
    /// and the row itself in the form of a [ValveRow].
    pub async fn insert_row(&self, table_name: &str, row: &JsonRow) -> Result<(u32, ValveRow)> {
        let table_options = &self.get_table_options_from_config(table_name)?;
        if !table_options.contains("edit") {
            return Err(ValveError::InputError(format!(
                "Inserting to table '{table_name}' is not allowed"
            ))
            .into());
        }

        let mut tx = self.pool.begin().await?;
        let row = ValveRow::from_simple_json(row, None)?;
        let mut row = validate_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            Some(&mut tx),
            table_name,
            &row,
            None,
        )
        .await?;

        let rn = insert_new_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            &mut tx,
            table_name,
            &row,
            true,
            false,
        )
        .await?;

        row.row_number = Some(rn);
        let serde_row = row.contents_to_rich_json()?;
        record_row_change_tx(
            &self.db_kind,
            &mut tx,
            table_name,
            &rn,
            None,
            Some(&serde_row),
            &self.user,
        )
        .await?;

        tx.commit().await?;
        Ok((rn, row))
    }

    /// Given a table name, a row number, and a row, represented as a JSON object in the following
    /// ('simple') format:
    /// ```
    /// {
    ///     "column_1": value1,
    ///     "column_2": value2,
    ///     ...
    /// },
    /// ```
    /// validate and update the row in the database, and return it in the form of a [ValveRow].
    pub async fn update_row(
        &self,
        table_name: &str,
        row_number: &u32,
        row: &JsonRow,
    ) -> Result<ValveRow> {
        let table_options = &self.get_table_options_from_config(table_name)?;
        if !table_options.contains("edit") {
            return Err(ValveError::InputError(format!(
                "Updating table '{}' is not allowed",
                table_name
            ))
            .into());
        }

        let mut tx = self.pool.begin().await?;

        // Get the old version of the row from the database so that we can later record it to the
        // history table:
        let old_row = get_text_row_from_db_tx(
            &self.config,
            &self.db_kind,
            &mut tx,
            table_name,
            &row_number,
        )
        .await?;

        let row = ValveRow::from_simple_json(row, Some(*row_number))?;
        let row = validate_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            Some(&mut tx),
            table_name,
            &row,
            None,
        )
        .await?;

        update_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            &mut tx,
            table_name,
            &row,
            true,
            false,
        )
        .await?;

        // Record the row update in the history table:
        let serde_row = row.contents_to_rich_json()?;
        record_row_change_tx(
            &self.db_kind,
            &mut tx,
            table_name,
            row_number,
            Some(&old_row),
            Some(&serde_row),
            &self.user,
        )
        .await?;

        tx.commit().await?;
        Ok(row)
    }

    /// Given a table name and a row number, delete that row from the table.
    pub async fn delete_row(&self, table_name: &str, row_number: &u32) -> Result<()> {
        let table_options = &self.get_table_options_from_config(table_name)?;
        if !table_options.contains("edit") {
            return Err(ValveError::InputError(format!(
                "Deleting from table '{}' is not allowed",
                table_name
            ))
            .into());
        }

        let mut tx = self.pool.begin().await?;

        let mut row = get_text_row_from_db_tx(
            &self.config,
            &self.db_kind,
            &mut tx,
            &table_name,
            row_number,
        )
        .await?;

        let previous_row = get_previous_row_tx(table_name, row_number, &mut tx).await?;
        row.insert("previous_row".into(), json!(previous_row));
        record_row_change_tx(
            &self.db_kind,
            &mut tx,
            &table_name,
            row_number,
            Some(&row),
            None,
            &self.user,
        )
        .await?;

        delete_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            &mut tx,
            table_name,
            row_number,
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Given a table name, `table`, a row number, `row`, and the number of the row, `previous_row`,
    /// representing the row number that will come immediately before `row` in the ordering of rows
    /// after the move has been completed: Set the `row_order` field corresponding to `row` in the
    /// database so that `row` comes immediately after `previous_row` in the ordering of rows.
    pub async fn move_row(&self, table: &str, row: &u32, previous_row: &u32) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        // Get the previous row, i.e., the row after which one will find the current row, which we
        // will use later to record this change in the history table:
        let old_previous_row = get_previous_row_tx(table, row, &mut tx).await?;

        move_row_tx(&mut tx, table, row, previous_row).await?;

        // Record the move in the history table unless we have been explicitly told not to:
        record_row_move_tx(
            &self.config,
            &self.db_kind,
            &mut tx,
            table,
            row,
            &old_previous_row,
            &previous_row,
            &self.user,
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Add the message with the given fields to the message table and return the message_id of the
    /// newly added message.    
    pub async fn insert_message(
        &self,
        table: &str,
        row: u32,
        column: &str,
        value: &str,
        level: &str,
        rule: &str,
        message: &str,
    ) -> Result<u16> {
        let sql = format!(
            r#"
            INSERT INTO "message"
              ("table", "row", "column", "value", "level", "rule", "message")
            VALUES
              ({SQL_PARAM}, {row}, {SQL_PARAM}, {SQL_PARAM}, {SQL_PARAM}, {SQL_PARAM}, {SQL_PARAM})
            RETURNING "message_id" AS "message_id""#
        );
        let sql = local_sql_syntax(&self.db_kind, &sql);
        let mut query = sqlx_query(&sql);
        for param in vec![table, column, value, level, rule, message] {
            query = query.bind(param);
        }
        let row = query.fetch_one(&self.pool).await?;
        let mid: i32 = row.try_get::<i32, &str>("message_id")?;
        Ok(mid as u16)
    }

    /// Update the message represented by the given message_id in the message table with the given
    /// fields.
    pub async fn update_message(
        &self,
        message_id: u16,
        table: &str,
        row: u32,
        column: &str,
        value: &str,
        level: &str,
        rule: &str,
        message: &str,
    ) -> Result<()> {
        let sql = format!(
            r#"
            UPDATE "message"
               SET "table" = {SQL_PARAM},
                     "row" = {row},
                     "column" = {SQL_PARAM},
                      "value" = {SQL_PARAM},
                      "level" = {SQL_PARAM},
                       "rule" = {SQL_PARAM},
                    "message" = {SQL_PARAM}
             WHERE "message_id" = {message_id}"#
        );
        let sql = local_sql_syntax(&self.db_kind, &sql);
        let mut query = sqlx_query(&sql);
        for param in vec![table, column, value, level, rule, message] {
            query = query.bind(param);
        }
        query.execute(&self.pool).await?;
        Ok(())
    }

    /// Delete the given message in the message table.
    pub async fn delete_message(&self, message_id: u16) -> Result<()> {
        let sql = format!(r#"DELETE FROM "message" WHERE "message_id" = {message_id}"#);
        self.execute_sql(&sql).await
    }

    /// Given a string representing a literal argument to a SQL "LIKE" clause, delete any
    /// messages in the message table whose rule matches the LIKE clause.
    pub async fn delete_messages_like(&self, like: &str) -> Result<()> {
        let sql = format!(r#"DELETE FROM "message" WHERE "rule" LIKE {SQL_PARAM}"#);
        let sql = local_sql_syntax(&self.db_kind, &sql);
        let query = sqlx_query(&sql).bind(like);
        query.execute(&self.pool).await?;
        Ok(())
    }

    /// Return an ordered list of the changes that can be undone. If `limit` is nonzero, return no
    /// more than that many change records.
    pub async fn get_changes_to_undo(&self, limit: usize) -> Result<Vec<ValveRowChange>> {
        let records = get_db_records_to_undo(&self.pool, limit).await?;
        let mut changes = vec![];
        for record in &records {
            match ValveRowChange::from_undo_or_redo_record(record) {
                Err(e) => return Err(e.into()),
                Ok(change) => changes.push(change),
            };
        }
        Ok(changes)
    }

    /// Return an ordered list of the changes that can be undone. If `limit` is nonzero, return no
    /// more than that many change records.
    pub async fn get_changes_to_redo(&self, limit: usize) -> Result<Vec<ValveRowChange>> {
        let records = get_db_records_to_redo(&self.pool, limit).await?;
        let mut changes = vec![];
        for record in &records {
            match ValveRowChange::from_undo_or_redo_record(record) {
                Err(e) => return Err(e.into()),
                Ok(change) => changes.push(change),
            };
        }
        Ok(changes)
    }

    /// Undo one change and return the change record or None if there was no change to undo.
    pub async fn undo(&mut self) -> Result<Option<ValveRow>> {
        let records = get_db_records_to_undo(&self.pool, 1).await?;
        let last_change = match records.len() {
            0 => {
                log::warn!("Nothing to undo.");
                return Ok(None);
            }
            1 => &records[0],
            _ => {
                return Err(ValveError::DataError(format!(
                    "Too many records to undo: {}",
                    records.len()
                ))
                .into())
            }
        };

        let history_id: i32 = last_change.get("history_id");
        let history_id = history_id as u16;
        let table: &str = last_change.get("table");
        let row_number: i64 = last_change.get("row");
        let row_number = row_number as u32;
        let from = get_json_object_from_column(&last_change, "from");
        let to = get_json_object_from_column(&last_change, "to");

        let make_err = |err_str: &str| -> ValveError { ValveError::DataError(err_str.to_string()) };

        // Handle metadata changes and row moves:
        let raw_summary = last_change.try_get_raw("summary")?;
        if !raw_summary.is_null() {
            let summary: &str = last_change.get("summary");
            match summary {
                // Metadata changes:
                "add column" => match &to {
                    None => return Err(make_err("No 'to' found").into()),
                    Some(to) => {
                        let table = to
                            .get("table")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table' found"))?;
                        let column = to
                            .get("column")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'column' found"))?;

                        let mut tx = self.pool.begin().await?;
                        delete_column_tx(self, table, column, &mut tx).await?;
                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            true,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;
                        tx.commit().await?;

                        // Save the column table and the data table and then reconfigure valve:
                        self.save_tables(&vec![&self.config.special.column, table], &None)
                            .await?;
                        self.reconfigure().await?;

                        // Refresh the database:
                        self.ensure_all_tables_created(&vec![table]).await?;

                        return Ok(None);
                    }
                },
                "delete column" => match &from {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(from) => {
                        let table = from
                            .get("table")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table' found"))?;
                        let column = from
                            .get("column")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'column' found"))?;

                        // Add a new column to the column table, then update its row number to
                        // the saved row number of the column that had been previously deleted:
                        let mut tx = self.pool.begin().await?;
                        add_column_tx(self, table, column, from, &mut tx).await?;
                        let sql = {
                            let column_table = &self.config.special.column;
                            let rn: i64 = last_change.get("row");
                            let rn = rn as u32;
                            let ro = from
                                .get("row_order")
                                .ok_or(make_err("No 'row_order' found"))?;
                            local_sql_syntax(
                                &self.db_kind,
                                &format!(
                                    r#"UPDATE "{column_table}"
                                       SET "row_number" = {rn}, "row_order" = {ro}
                                       WHERE "table" = {SQL_PARAM} AND "column" = {SQL_PARAM}"#
                                ),
                            )
                        };
                        let query = sqlx_query(&sql).bind(table).bind(column);
                        query.execute(tx.acquire().await?).await?;

                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            true,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;
                        tx.commit().await?;

                        // Save the column table and the data table and then reconfigure valve:
                        self.save_tables(&vec![&self.config.special.column, table], &None)
                            .await?;
                        self.reconfigure().await?;

                        // Refresh the database:
                        self.ensure_all_tables_created(&vec![table]).await?;

                        return Ok(None);
                    }
                },
                "rename column" => match &from {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(from) => {
                        let table = from
                            .get("table")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table' found"))?;
                        let old_name = from
                            .get("column")
                            .and_then(|c| c.as_str())
                            .ok_or(make_err("No string 'column' found"))?;
                        let old_label = match from.get("label") {
                            None => None,
                            Some(label) => Some(
                                label
                                    .as_str()
                                    .ok_or(make_err("Label is not a string"))?
                                    .to_string(),
                            ),
                        };
                        match &to {
                            None => return Err(make_err("No 'to' found").into()),
                            Some(to) => {
                                let new_name = to
                                    .get("column")
                                    .and_then(|c| c.as_str())
                                    .ok_or(make_err("No string 'column' found"))?;

                                let mut tx = self.pool.begin().await?;
                                rename_column_tx(
                                    &self, &mut tx, table, new_name, old_name, &old_label,
                                )
                                .await?;
                                switch_undone_state_tx(
                                    &self.user,
                                    history_id,
                                    true,
                                    &mut tx,
                                    &self.db_kind,
                                )
                                .await?;
                                tx.commit().await?;

                                // Save the column and rule tables, and the data table, and
                                // then reconfigure valve:
                                self.save_tables(
                                    &vec![
                                        &self.config.special.column,
                                        &self.config.special.rule,
                                        table,
                                    ],
                                    &None,
                                )
                                .await?;
                                self.reconfigure().await?;

                                // Refresh the database but do not load any data:
                                self.ensure_all_tables_created(&vec![table]).await?;

                                return Ok(None);
                            }
                        }
                    }
                },
                "add datatype" => match &to {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(to) => {
                        let datatype = to
                            .get("datatype")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'datatype' found"))?;

                        let mut tx = self.pool.begin().await?;
                        delete_datatype_tx(self, datatype, &mut tx).await?;
                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            true,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;
                        tx.commit().await?;

                        // Save the datatype table and then reconfigure valve:
                        self.save_tables(&vec![&self.config.special.datatype], &None)
                            .await?;
                        self.reconfigure().await?;

                        return Ok(None);
                    }
                },
                "delete datatype" => match &from {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(from) => {
                        let mut tx = self.pool.begin().await?;

                        // Add a new datatype to the datatype table, then update its row number
                        // and row order to match the saved values of the datatype row that had
                        // been previously deleted:
                        add_datatype_tx(self, from, &mut tx).await?;
                        let datatype = from
                            .get("datatype")
                            .and_then(|d| d.as_str())
                            .ok_or(make_err("No string 'datatype' found"))?;
                        let sql = {
                            let rn: i64 = last_change.get("row");
                            let rn = rn as u32;
                            let ro = from
                                .get("row_order")
                                .ok_or(make_err("No 'row_order' found"))?;
                            let datatype_table = &self.config.special.datatype;
                            local_sql_syntax(
                                &self.db_kind,
                                &format!(
                                    r#"UPDATE "{datatype_table}"
                                       SET "row_number" = {rn}, "row_order" = {ro}
                                       WHERE "datatype" = {SQL_PARAM}"#
                                ),
                            )
                        };
                        let query = sqlx_query(&sql).bind(datatype);
                        query.execute(tx.acquire().await?).await?;

                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            true,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;

                        // Commit the transaction:
                        tx.commit().await?;

                        // Save the datatype table and then reconfigure valve:
                        self.save_tables(&vec![&self.config.special.datatype], &None)
                            .await?;
                        self.reconfigure().await?;
                        return Ok(None);
                    }
                },
                "rename datatype" => match &from {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(from) => {
                        let old_name = from
                            .get("datatype")
                            .and_then(|d| d.as_str())
                            .ok_or(make_err("No string 'datatype' found in 'from'"))?;
                        match &to {
                            None => return Err(make_err("No 'to' found").into()),
                            Some(to) => {
                                let new_name = to
                                    .get("datatype")
                                    .and_then(|d| d.as_str())
                                    .ok_or(make_err("No string 'datatype' found in 'to'"))?;

                                let mut tx = self.pool.begin().await?;
                                rename_datatype_tx(&self, &mut tx, new_name, old_name).await?;
                                switch_undone_state_tx(
                                    &self.user,
                                    history_id,
                                    true,
                                    &mut tx,
                                    &self.db_kind,
                                )
                                .await?;
                                tx.commit().await?;

                                // Save the column table and the data table and then reconfigure
                                // valve:
                                self.save_tables(
                                    &vec![
                                        &self.config.special.datatype,
                                        &self.config.special.column,
                                        &self.config.special.rule,
                                    ],
                                    &None,
                                )
                                .await?;
                                self.reconfigure().await?;

                                return Ok(None);
                            }
                        };
                    }
                },
                "add table" => match &to {
                    None => return Err(make_err("No 'to' found").into()),
                    Some(to) => {
                        let table = to
                            .get("table")
                            .and_then(|t| t.as_object())
                            .and_then(|t| t.get("table"))
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No table found"))?;

                        let mut tx = self.pool.begin().await?;
                        delete_table_tx(&self, table, &mut tx, &self.db_kind).await?;
                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            true,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;
                        tx.commit().await?;

                        // Save the column and table tables and reconfigure valve:
                        self.save_tables(
                            &vec![&self.config.special.table, &self.config.special.column],
                            &None,
                        )
                        .await?;
                        self.reconfigure().await?;

                        return Ok(None);
                    }
                },
                "delete table" => match &from {
                    None => return Err(make_err("No 'to' found").into()),
                    Some(from) => {
                        let mut values = vec![];
                        let mut params = vec![];

                        // Use `from['table']` to build a query for inserting a new table to the
                        // table table:
                        let from_table = from
                            .get("table")
                            .and_then(|t| t.as_object())
                            .ok_or(make_err("No object 'table' found"))?;

                        let table = from_table
                            .get("table")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table' found"))?;
                        if table == "" {
                            values.push("NULL");
                        } else {
                            values.push(SQL_PARAM);
                            params.push(table);
                        }

                        let table_type = from_table
                            .get("table_type")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table_type' found"))?;
                        if table_type == "" {
                            values.push("NULL");
                        } else {
                            values.push(SQL_PARAM);
                            params.push(table_type);
                        }

                        let options = from_table
                            .get("options")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'options' found"))?;
                        if options == "" {
                            values.push("NULL");
                        } else {
                            values.push(SQL_PARAM);
                            params.push(options);
                        }

                        let description = from_table
                            .get("description")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'description' found"))?;
                        if description == "" {
                            values.push("NULL");
                        } else {
                            values.push(SQL_PARAM);
                            params.push(description);
                        }

                        let path = from_table
                            .get("path")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'path' found"))?;
                        if path == "" {
                            values.push("NULL");
                        } else {
                            values.push(SQL_PARAM);
                            params.push(path);
                        }

                        let rn = from_table
                            .get("row_number")
                            .and_then(|r| r.as_i64())
                            .ok_or(make_err("No integer 'row_number' found"))?;
                        let rn = rn as u32;
                        let ro = from_table
                            .get("row_order")
                            .and_then(|r| r.as_i64())
                            .ok_or(make_err("No integer 'row_number' found"))?;
                        let ro = ro as u32;

                        // Begin a database transaction and execute the SQL:
                        let mut tx = self.pool.begin().await?;

                        let sql = {
                            let table_table = &self.config.special.table;
                            local_sql_syntax(
                                &self.db_kind,
                                &format!(
                                    r#"INSERT INTO "{table_table}"
                                       ("row_number", "row_order", "table", "type", "options",
                                        "description", "path")
                                       VALUES ({rn}, {ro}, {remaining_values})"#,
                                    remaining_values = values.join(", ")
                                ),
                            )
                        };
                        let mut query = sqlx_query(&sql);
                        for param in params {
                            query = query.bind(param);
                        }
                        query.execute(tx.acquire().await?).await?;

                        // Use `from['column']` to build a query for inserting new columns to the
                        // column table:
                        let from_columns = from
                            .get("column")
                            .and_then(|t| t.as_array())
                            .ok_or(make_err("No array 'column' found"))?;

                        for cconfig in from_columns.iter() {
                            let mut values = vec![];
                            let mut params = vec![];

                            let rn = cconfig
                                .get("row_number")
                                .and_then(|r| r.as_i64())
                                .ok_or(make_err("No integer 'row_number' found"))?;
                            let rn = rn as u32;
                            let ro = cconfig
                                .get("row_order")
                                .and_then(|r| r.as_i64())
                                .ok_or(make_err("No integer 'row_order' found"))?;
                            let ro = ro as u32;

                            let table = cconfig
                                .get("table")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'table' found"))?;
                            if table == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(table);
                            }

                            let column = cconfig
                                .get("column")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'column' found"))?;
                            if column == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(column);
                            }

                            let datatype = cconfig
                                .get("datatype")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'datatype' found"))?;
                            if datatype == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(datatype);
                            }

                            let description = cconfig
                                .get("description")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'description' found"))?;
                            if description == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(description);
                            }

                            let label = cconfig
                                .get("label")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'label' found"))?;
                            if label == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(label);
                            }

                            let structure = cconfig
                                .get("structure")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'structure' found"))?;
                            if structure == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(structure);
                            }

                            let nulltype = cconfig
                                .get("nulltype")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'nulltype' found"))?;
                            if nulltype == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(nulltype);
                            }

                            let default = cconfig
                                .get("default")
                                .ok_or(make_err("No 'default' found"))?;
                            // The 'default' field need not be a string, so it needs to be handled
                            // with more care:
                            let default_str = default.to_string();
                            match default {
                                SerdeValue::Number(_) => {
                                    values.push(&default_str);
                                }
                                SerdeValue::String(default) => {
                                    if default == "" {
                                        values.push("NULL");
                                    } else {
                                        values.push(SQL_PARAM);
                                        params.push(default);
                                    }
                                }
                                _ => return Err(make_err("Field 'default' has wrong type").into()),
                            };

                            // Execute the SQL to add this column:
                            let sql = format!(
                                r#"INSERT INTO "{column_table}"
                                   ("row_number", "row_order", "table", "column",
                                    "datatype", "description", "label", "structure",
                                    "nulltype", "default")
                                   VALUES
                                   ({rn}, {ro}, {remaining_values})"#,
                                column_table = &self.config.special.column,
                                remaining_values = values.join(", "),
                            );
                            let sql = local_sql_syntax(&self.db_kind, &sql.to_string());
                            let mut query = sqlx_query(&sql);
                            for param in params {
                                query = query.bind(param);
                            }
                            query.execute(tx.acquire().await?).await?;
                        }

                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            true,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;

                        // Commit the transaction:
                        tx.commit().await?;

                        // Save and reconfigure:
                        self.save_tables(
                            &vec![&self.config.special.table, &self.config.special.column],
                            &None,
                        )
                        .await?;
                        self.reconfigure().await?;
                        self.ensure_all_tables_created(&vec![table]).await?;
                        return Ok(None);
                    }
                },
                "rename table" => match &from {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(from) => {
                        let old_name = from
                            .get("table")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table' found in 'from'"))?;
                        match &to {
                            None => return Err(make_err("No 'to' found").into()),
                            Some(to) => {
                                let new_name = to
                                    .get("table")
                                    .and_then(|t| t.as_str())
                                    .ok_or(make_err("No string 'table' found in 'to'"))?;

                                let mut tx = self.pool.begin().await?;
                                rename_table_tx(&self, &mut tx, new_name, old_name).await?;
                                switch_undone_state_tx(
                                    &self.user,
                                    history_id,
                                    true,
                                    &mut tx,
                                    &self.db_kind,
                                )
                                .await?;
                                tx.commit().await?;

                                // Save the column table and the data table and then reconfigure
                                // valve:
                                self.save_tables(
                                    &vec![
                                        &self.config.special.table,
                                        &self.config.special.column,
                                        &self.config.special.rule,
                                    ],
                                    &None,
                                )
                                .await?;
                                self.reconfigure().await?;

                                return Ok(None);
                            }
                        };
                    }
                },
                // Moves:
                _ => {
                    let summary = get_json_array_from_column(&last_change, "summary");
                    if let Some(summary) = summary {
                        let num_moves = summary
                            .iter()
                            .filter(|o| {
                                o.get("column").expect("No 'column' in summary") == "previous_row"
                                    && o.get("level").expect("No 'level' in summary") == "move"
                            })
                            .collect::<Vec<_>>()
                            .len();
                        if num_moves == 1 {
                            // Undo a move:
                            let mut tx = self.pool.begin().await?;

                            undo_or_redo_move_tx(
                                table,
                                &last_change,
                                history_id,
                                &row_number,
                                &mut tx,
                                true,
                            )
                            .await?;
                            switch_undone_state_tx(
                                &self.user,
                                history_id,
                                true,
                                &mut tx,
                                &self.db_kind,
                            )
                            .await?;

                            tx.commit().await?;
                            return Ok(None);
                        } else if num_moves > 1 {
                            return Err(ValveError::DataError(format!(
                                "Invalid history record #{} indicated multiple moves",
                                history_id
                            ))
                            .into());
                        }
                    }
                }
            };
        }

        // Handle other kinds of changes to rows:
        match (from, to) {
            (None, None) => Err(ValveError::DataError(
                "Cannot undo unknown operation from None to None".into(),
            )
            .into()),
            (None, Some(_)) => {
                // Undo an insert:
                let mut tx = self.pool.begin().await?;

                delete_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &row_number,
                )
                .await?;

                switch_undone_state_tx(&self.user, history_id, true, &mut tx, &self.db_kind)
                    .await?;
                tx.commit().await?;
                Ok(None)
            }
            (Some(mut from), None) => {
                // Undo a delete:
                let mut tx = self.pool.begin().await?;

                let previous_row = match from.get("previous_row") {
                    Some(SerdeValue::Number(n)) => {
                        let number: u32 = n.to_string().parse()?;
                        number
                    }
                    Some(v) => {
                        return Err(ValveError::DataError(format!(
                            "Unexpected value: {:?} for previous_row in history record",
                            v
                        ))
                        .into())
                    }
                    None => {
                        return Err(ValveError::DataError(
                            "No previous_row found in history record".into(),
                        )
                        .into())
                    }
                };

                // The previous_row field is no longer needed so we remove it here:
                from.remove("previous_row");

                let from = ValveRow::from_rich_json(Some(row_number), &from)?;
                let rn = insert_new_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &from,
                    false,
                    false,
                )
                .await?;

                // Move the row back to the position after `previous_row`, which is the position
                // it was in before it was deleted:
                move_row_tx(&mut tx, table, &rn, &previous_row).await?;

                switch_undone_state_tx(&self.user, history_id, true, &mut tx, &self.db_kind)
                    .await?;
                tx.commit().await?;
                Ok(Some(from))
            }
            (Some(mut from), Some(_)) => {
                // Undo an an update:
                let mut tx = self.pool.begin().await?;

                // The previous_row field is not needed so we remove it here:
                from.remove("previous_row");

                let from = ValveRow::from_rich_json(Some(row_number), &from)?;
                update_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &from,
                    false,
                    false,
                )
                .await?;

                switch_undone_state_tx(&self.user, history_id, true, &mut tx, &self.db_kind)
                    .await?;
                tx.commit().await?;
                Ok(Some(from))
            }
        }
    }

    /// Redo one change and return the change record or None if there was no change to redo.
    pub async fn redo(&mut self) -> Result<Option<ValveRow>> {
        let records = get_db_records_to_redo(&self.pool, 1).await?;
        let next_undo_id = get_next_undo_id(&self.pool).await?;
        let next_redo = match records.len() {
            0 => {
                log::warn!("Nothing to redo.");
                return Ok(None);
            }
            1 => {
                let next_redo = &records[0];
                let undone_by = next_redo.try_get_raw("undone_by")?;
                if undone_by.is_null() {
                    log::warn!("Nothing to redo.");
                    return Ok(None);
                }
                next_redo
            }
            _ => {
                return Err(ValveError::DataError(format!(
                    "Too many records to redo: {}",
                    records.len()
                ))
                .into())
            }
        };

        let history_id: i32 = next_redo.get("history_id");
        let history_id = history_id as u16;
        // We do not redo orphaned changes:
        if history_id < next_undo_id {
            log::warn!("Nothing to redo.");
            return Ok(None);
        }

        let table: &str = next_redo.get("table");
        let row_number: i64 = next_redo.get("row");
        let row_number = row_number as u32;
        let from = get_json_object_from_column(&next_redo, "from");
        let to = get_json_object_from_column(&next_redo, "to");

        let make_err = |err_str: &str| -> ValveError { ValveError::DataError(err_str.to_string()) };

        // Handle metadata changes and row moves:
        let raw_summary = next_redo.try_get_raw("summary")?;
        if !raw_summary.is_null() {
            let summary: &str = next_redo.get("summary");
            match summary {
                // Metadata changes:
                "add column" => match &to {
                    None => return Err(make_err("No 'to' found").into()),
                    Some(to) => {
                        let table = to
                            .get("table")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table' found"))?;
                        let column = to
                            .get("column")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'column' found"))?;

                        // Begin a transaction:
                        let mut tx = self.pool.begin().await?;

                        // Add a column to the table and then update its row number and row order
                        // to the saved values for the previously deleted row:
                        add_column_tx(self, table, column, to, &mut tx).await?;
                        let sql = {
                            let rn: i64 = next_redo.get("row");
                            let rn = rn as u32;
                            let ro = to
                                .get("row_order")
                                .ok_or(make_err("No 'row_order' found"))?;
                            let column_table = &self.config.special.column;
                            local_sql_syntax(
                                &self.db_kind,
                                &format!(
                                    r#"UPDATE "{column_table}"
                                       SET "row_number" = {rn}, "row_order" = {ro}
                                       WHERE "table" = {SQL_PARAM} AND "column" = {SQL_PARAM}"#
                                ),
                            )
                        };
                        let query = sqlx_query(&sql).bind(table).bind(column);
                        query.execute(tx.acquire().await?).await?;

                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            false,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;

                        // Commit the transaction:
                        tx.commit().await?;

                        // Save the column table and the data table and then reconfigure valve:
                        self.save_tables(&vec![&self.config.special.column, table], &None)
                            .await?;
                        self.reconfigure().await?;

                        // Refresh the database:
                        self.ensure_all_tables_created(&vec![table]).await?;

                        return Ok(None);
                    }
                },
                "delete column" => match &from {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(from) => {
                        let table = from
                            .get("table")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table' found"))?;
                        let column = from
                            .get("column")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'column' found"))?;

                        let mut tx = self.pool.begin().await?;
                        delete_column_tx(self, table, column, &mut tx).await?;
                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            false,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;
                        tx.commit().await?;

                        // Save the column table and the data table and then reconfigure valve:
                        self.save_tables(&vec![&self.config.special.column, table], &None)
                            .await?;
                        self.reconfigure().await?;

                        // Refresh the database:
                        self.ensure_all_tables_created(&vec![table]).await?;

                        return Ok(None);
                    }
                },
                "rename column" => match &to {
                    None => return Err(make_err("No 'to' found").into()),
                    Some(to) => {
                        let table = to
                            .get("table")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table' in 'to'"))?;
                        let new_name = to
                            .get("column")
                            .and_then(|c| c.as_str())
                            .ok_or(make_err("No string 'column' in 'to'"))?;
                        let new_label = match to.get("label") {
                            None => None,
                            Some(label) => Some(
                                label
                                    .as_str()
                                    .ok_or(make_err("Label is not a string"))?
                                    .to_string(),
                            ),
                        };
                        match &from {
                            None => return Err(make_err("No 'from' found").into()),
                            Some(from) => {
                                let old_name = from
                                    .get("column")
                                    .and_then(|c| c.as_str())
                                    .ok_or(make_err("No string 'column' in 'from'"))?;

                                let mut tx = self.pool.begin().await?;
                                rename_column_tx(
                                    &self, &mut tx, table, old_name, new_name, &new_label,
                                )
                                .await?;
                                switch_undone_state_tx(
                                    &self.user,
                                    history_id,
                                    false,
                                    &mut tx,
                                    &self.db_kind,
                                )
                                .await?;
                                tx.commit().await?;

                                // Save the column and rule tables, and the data table, and
                                // then reconfigure valve:
                                self.save_tables(
                                    &vec![
                                        &self.config.special.column,
                                        &self.config.special.rule,
                                        table,
                                    ],
                                    &None,
                                )
                                .await?;
                                self.reconfigure().await?;

                                // Refresh the database but do not load any data:
                                self.ensure_all_tables_created(&vec![table]).await?;

                                return Ok(None);
                            }
                        };
                    }
                },
                "add datatype" => match &to {
                    None => return Err(make_err("No 'to' found").into()),
                    Some(to) => {
                        // Begin a transaction:
                        let mut tx = self.pool.begin().await?;

                        // Add the datatype and then update its row number to the save value of the
                        // previously deleted datatype:
                        add_datatype_tx(self, to, &mut tx).await?;
                        let datatype = to
                            .get("datatype")
                            .and_then(|d| d.as_str())
                            .ok_or(make_err("No string 'datatype' found"))?;
                        let sql = {
                            let rn: i64 = next_redo.get("row");
                            let rn = rn as u32;
                            let ro = to
                                .get("row_order")
                                .ok_or(make_err("No 'row_order' found"))?;
                            let datatype_table = &self.config.special.datatype;
                            local_sql_syntax(
                                &self.db_kind,
                                &format!(
                                    r#"UPDATE "{datatype_table}"
                                       SET "row_number" = {rn}, "row_order" = {ro}
                                       WHERE "datatype" = {SQL_PARAM}"#
                                ),
                            )
                        };
                        let query = sqlx_query(&sql).bind(datatype);
                        query.execute(tx.acquire().await?).await?;

                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            false,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;

                        // Commit the transaction:
                        tx.commit().await?;

                        // Save the datatype table and then reconfigure valve:
                        self.save_tables(&vec![&self.config.special.datatype], &None)
                            .await?;
                        self.reconfigure().await?;
                        return Ok(None);
                    }
                },
                "delete datatype" => match &from {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(from) => {
                        let datatype = from
                            .get("datatype")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'datatype' found"))?;

                        let mut tx = self.pool.begin().await?;
                        delete_datatype_tx(self, datatype, &mut tx).await?;
                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            false,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;
                        tx.commit().await?;

                        // Save the datatype table and then reconfigure valve:
                        self.save_tables(&vec![&self.config.special.datatype], &None)
                            .await?;
                        self.reconfigure().await?;

                        return Ok(None);
                    }
                },
                "rename datatype" => match &from {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(from) => {
                        let old_name = from
                            .get("datatype")
                            .and_then(|d| d.as_str())
                            .ok_or(make_err("No string 'datatype' in 'from'"))?;
                        match &to {
                            None => return Err(make_err("No 'to' found").into()),
                            Some(to) => {
                                let new_name = to
                                    .get("datatype")
                                    .and_then(|d| d.as_str())
                                    .ok_or(make_err("No string 'datatype' in 'to'"))?;

                                let mut tx = self.pool.begin().await?;
                                rename_datatype_tx(&self, &mut tx, old_name, new_name).await?;
                                switch_undone_state_tx(
                                    &self.user,
                                    history_id,
                                    false,
                                    &mut tx,
                                    &self.db_kind,
                                )
                                .await?;
                                tx.commit().await?;

                                // Save the column table and the data table and then reconfigure
                                // valve:
                                self.save_tables(
                                    &vec![
                                        &self.config.special.datatype,
                                        &self.config.special.column,
                                        &self.config.special.rule,
                                    ],
                                    &None,
                                )
                                .await?;
                                self.reconfigure().await?;

                                return Ok(None);
                            }
                        };
                    }
                },
                "add table" => match &to {
                    None => return Err(make_err("No 'to' found").into()),
                    Some(to) => {
                        let mut values = vec![];
                        let mut params = vec![];

                        // Use `to['table']` to build a query for inserting to the table table:
                        let to_table = to
                            .get("table")
                            .and_then(|t| t.as_object())
                            .ok_or(make_err("No object 'table' found"))?;

                        let table = to_table
                            .get("table")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table' found"))?;
                        if table == "" {
                            values.push("NULL");
                        } else {
                            values.push(SQL_PARAM);
                            params.push(table);
                        }

                        let table_type = to_table
                            .get("table_type")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table_type' found"))?;
                        if table_type == "" {
                            values.push("NULL");
                        } else {
                            values.push(SQL_PARAM);
                            params.push(table_type);
                        }

                        let options = to_table
                            .get("options")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'options' found"))?;
                        if options == "" {
                            values.push("NULL");
                        } else {
                            values.push(SQL_PARAM);
                            params.push(options);
                        }

                        let description = to_table
                            .get("description")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'description' found"))?;
                        if description == "" {
                            values.push("NULL");
                        } else {
                            values.push(SQL_PARAM);
                            params.push(description);
                        }

                        let path = to_table
                            .get("path")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'path' found"))?;
                        if path == "" {
                            values.push("NULL");
                        } else {
                            values.push(SQL_PARAM);
                            params.push(path);
                        }

                        let rn = to_table
                            .get("row_number")
                            .and_then(|r| r.as_i64())
                            .ok_or(make_err("No integer 'row_number' found"))?;
                        let rn = rn as u32;
                        let ro = to_table
                            .get("row_order")
                            .and_then(|r| r.as_i64())
                            .ok_or(make_err("No integer 'row_order' found"))?;
                        let ro = ro as u32;

                        // Begin a transaction and execute the insert statement:
                        let mut tx = self.pool.begin().await?;

                        let sql = {
                            let table_table = &self.config.special.table;
                            local_sql_syntax(
                                &self.db_kind,
                                &format!(
                                    r#"INSERT INTO "{table_table}"
                                       ("row_number", "row_order", "table", "type", "options",
                                        "description", "path")
                                       VALUES ({rn}, {ro}, {remaining_values})"#,
                                    remaining_values = values.join(", ")
                                ),
                            )
                        };
                        let mut query = sqlx_query(&sql);
                        for param in params {
                            query = query.bind(param);
                        }
                        query.execute(tx.acquire().await?).await?;

                        // Use `to['column']` to build a query to add columns to the column table:
                        let to_columns = to
                            .get("column")
                            .and_then(|t| t.as_array())
                            .ok_or(make_err("No array 'column' found"))?;

                        for cconfig in to_columns.iter() {
                            let mut values = vec![];
                            let mut params = vec![];

                            let rn = cconfig
                                .get("row_number")
                                .and_then(|r| r.as_i64())
                                .ok_or(make_err("No integer 'row_number' found"))?;
                            let rn = rn as u32;
                            let ro = cconfig
                                .get("row_order")
                                .and_then(|r| r.as_i64())
                                .ok_or(make_err("No integer 'row_number' found"))?;
                            let ro = ro as u32;

                            let table = cconfig
                                .get("table")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'table' found"))?;
                            if table == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(table);
                            }

                            let column = cconfig
                                .get("column")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'column' found"))?;
                            if column == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(column);
                            }

                            let datatype = cconfig
                                .get("datatype")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'datatype' found"))?;
                            if datatype == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(datatype);
                            }

                            let description = cconfig
                                .get("description")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'description' found"))?;
                            if description == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(description);
                            }

                            let label = cconfig
                                .get("label")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'label' found"))?;
                            if label == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(label);
                            }

                            let structure = cconfig
                                .get("structure")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'structure' found"))?;
                            if structure == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(structure);
                            }

                            let nulltype = cconfig
                                .get("nulltype")
                                .and_then(|t| t.as_str())
                                .ok_or(make_err("No string 'nulltype' found"))?;
                            if nulltype == "" {
                                values.push("NULL");
                            } else {
                                values.push(SQL_PARAM);
                                params.push(nulltype);
                            }

                            // The field 'default' need not be a string:
                            let default = cconfig
                                .get("default")
                                .ok_or(make_err("No 'default' found"))?;
                            let default_str = default.to_string();
                            match default {
                                SerdeValue::Number(_) => {
                                    values.push(&default_str);
                                }
                                SerdeValue::String(default) => {
                                    if default == "" {
                                        values.push("NULL");
                                    } else {
                                        values.push(SQL_PARAM);
                                        params.push(default);
                                    }
                                }
                                _ => return Err(make_err("Field 'default' has wrong type").into()),
                            };

                            let sql = format!(
                                r#"INSERT INTO "{column_table}"
                                   ("row_number", "row_order", "table", "column",
                                    "datatype", "description", "label", "structure",
                                    "nulltype", "default")
                                   VALUES
                                   ({rn}, {ro}, {remaining_values})"#,
                                column_table = &self.config.special.column,
                                remaining_values = values.join(", "),
                            );
                            let sql = local_sql_syntax(&self.db_kind, &sql.to_string());
                            let mut query = sqlx_query(&sql);
                            for param in params {
                                query = query.bind(param);
                            }
                            query.execute(tx.acquire().await?).await?;
                        }

                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            false,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;

                        // Commit the transaction:
                        tx.commit().await?;

                        self.save_tables(
                            &vec![&self.config.special.table, &self.config.special.column],
                            &None,
                        )
                        .await?;
                        self.reconfigure().await?;
                        self.ensure_all_tables_created(&vec![table]).await?;
                        return Ok(None);
                    }
                },
                "delete table" => match &from {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(from) => {
                        let table = from
                            .get("table")
                            .and_then(|t| t.as_object())
                            .and_then(|t| t.get("table"))
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No table found"))?;

                        let mut tx = self.pool.begin().await?;
                        delete_table_tx(&self, table, &mut tx, &self.db_kind).await?;
                        switch_undone_state_tx(
                            &self.user,
                            history_id,
                            false,
                            &mut tx,
                            &self.db_kind,
                        )
                        .await?;
                        tx.commit().await?;

                        // Save the column and table tables and reconfigure valve:
                        self.save_tables(
                            &vec![&self.config.special.table, &self.config.special.column],
                            &None,
                        )
                        .await?;
                        self.reconfigure().await?;

                        return Ok(None);
                    }
                },
                "rename table" => match &from {
                    None => return Err(make_err("No 'from' found").into()),
                    Some(from) => {
                        let old_name = from
                            .get("table")
                            .and_then(|t| t.as_str())
                            .ok_or(make_err("No string 'table' in 'from'"))?;
                        match &to {
                            None => return Err(make_err("No 'to' found").into()),
                            Some(to) => {
                                let new_name = to
                                    .get("table")
                                    .and_then(|t| t.as_str())
                                    .ok_or(make_err("No string 'table' in 'to'"))?;

                                let mut tx = self.pool.begin().await?;
                                rename_table_tx(&self, &mut tx, old_name, new_name).await?;
                                switch_undone_state_tx(
                                    &self.user,
                                    history_id,
                                    false,
                                    &mut tx,
                                    &self.db_kind,
                                )
                                .await?;
                                tx.commit().await?;

                                // Save the column table and the data table and then reconfigure
                                // valve:
                                self.save_tables(
                                    &vec![
                                        &self.config.special.table,
                                        &self.config.special.column,
                                        &self.config.special.rule,
                                    ],
                                    &None,
                                )
                                .await?;
                                self.reconfigure().await?;

                                return Ok(None);
                            }
                        };
                    }
                },
                // Moves
                _ => {
                    let summary = get_json_array_from_column(&next_redo, "summary");
                    if let Some(summary) = summary {
                        let num_moves = summary
                            .iter()
                            .filter(|o| {
                                o.get("column").expect("No 'column' in summary") == "previous_row"
                                    && o.get("level").expect("No 'level' in summary") == "move"
                            })
                            .collect::<Vec<_>>()
                            .len();
                        if num_moves == 1 {
                            // Redo a move:
                            let mut tx = self.pool.begin().await?;

                            undo_or_redo_move_tx(
                                table,
                                &next_redo,
                                history_id,
                                &row_number,
                                &mut tx,
                                false,
                            )
                            .await?;
                            switch_undone_state_tx(
                                &self.user,
                                history_id,
                                false,
                                &mut tx,
                                &self.db_kind,
                            )
                            .await?;

                            tx.commit().await?;
                            return Ok(None);
                        } else if num_moves > 1 {
                            return Err(ValveError::DataError(format!(
                                "Invalid history record #{} indicated multiple moves",
                                history_id
                            ))
                            .into());
                        }
                    }
                }
            };
        }

        // Handle other kinds of changes to rows:
        match (from, to) {
            (None, None) => {
                return Err(ValveError::DataError(
                    "Cannot redo unknown operation from None to None".into(),
                )
                .into());
            }
            (None, Some(mut to)) => {
                // Redo an insert:
                let mut tx = self.pool.begin().await?;

                // The previous_row field is not needed so we remove it here:
                to.remove("previous_row");

                let to = ValveRow::from_rich_json(Some(row_number), &to)?;
                insert_new_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &to,
                    false,
                    false,
                )
                .await?;

                switch_undone_state_tx(&self.user, history_id, false, &mut tx, &self.db_kind)
                    .await?;
                tx.commit().await?;
                Ok(Some(to))
            }
            (Some(_), None) => {
                // Redo a delete:
                let mut tx = self.pool.begin().await?;

                delete_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &row_number,
                )
                .await?;

                switch_undone_state_tx(&self.user, history_id, false, &mut tx, &self.db_kind)
                    .await?;
                tx.commit().await?;
                Ok(None)
            }
            (Some(_), Some(mut to)) => {
                // Redo an an update:
                let mut tx = self.pool.begin().await?;

                // The previous_row field is not needed so we remove it here:
                to.remove("previous_row");

                let to = ValveRow::from_rich_json(Some(row_number), &to)?;
                update_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &to,
                    false,
                    false,
                )
                .await?;

                switch_undone_state_tx(&self.user, history_id, false, &mut tx, &self.db_kind)
                    .await?;
                tx.commit().await?;
                Ok(Some(to))
            }
        }
    }

    /// Given the name of a datatype, returns the configuration information for all of its
    /// ancestors.
    pub fn get_datatype_ancestors(&self, datatype: &str) -> Vec<ValveDatatypeConfig> {
        toolkit::get_datatype_ancestors(&self.config, &self.datatype_conditions, datatype, false)
    }

    /// Given the name of a datatype, returns the names of all of its ancestors.
    pub fn get_datatype_ancestor_names(&self, datatype: &str) -> Vec<String> {
        self.get_datatype_ancestors(datatype)
            .iter()
            .map(|d| d.datatype.to_string())
            .collect::<Vec<_>>()
    }

    /// Given a table name, a column name, and (optionally) a string to match, return a JSON array
    /// of possible valid values for the given column which contain the matching string as a
    /// substring (or all of them if no matching string is given). The JSON array returned is
    /// formatted for Typeahead, i.e., it takes the form:
    /// `[{"id": id, "label": label, "order": order}, ...]`.
    pub async fn get_matching_values(
        &self,
        table_name: &str,
        column_name: &str,
        matching_string: Option<&str>,
    ) -> Result<SerdeValue> {
        let config = &self.config;
        let datatype_conditions = &self.datatype_conditions;
        let structure_conditions = &self.structure_conditions;
        let pool = &self.pool;
        let db_kind = &self.db_kind;
        let dt_name = &config
            .table
            .get(table_name)
            .ok_or(ValveError::InputError(format!(
                "Undefined table '{}'",
                table_name
            )))?
            .column
            .get(column_name)
            .ok_or(ValveError::InputError(format!(
                "Undefined column '{}.{}'",
                table_name, column_name
            )))?
            .datatype;

        // If the condition is a list, then use the condition of the datatype indicated by
        // its first argument. Otherwise use the condition as is.
        let dt_condition = match datatype_conditions
            .get(dt_name)
            .and_then(|d| Some(&d.parsed))
        {
            Some(Expression::Function(name, args)) if name == "list" => match &*args[0] {
                Expression::Label(arg) => {
                    let label = unquote(arg).unwrap_or_else(|_| arg.to_string());
                    match datatype_conditions.get(&label) {
                        Some(c) => Some(c.parsed.clone()),
                        None => None,
                    }
                }
                _ => None,
            },
            Some(dt_condition) => Some(dt_condition.clone()),
            None => None,
        };

        let mut values = vec![];
        match dt_condition {
            Some(Expression::Function(name, args)) if name == "in" => {
                for arg in args {
                    if let Expression::Label(arg) = *arg {
                        // Remove the enclosing quotes from the values being returned:
                        let label = unquote(&arg).unwrap_or_else(|_| arg);
                        if let Some(s) = matching_string {
                            if label.contains(s) {
                                values.push(label);
                            }
                        }
                    }
                }
            }
            _ => {
                // If the datatype for the column does not correspond to an `in(...)` function, then
                // we check the column's structure constraints. If they include a
                // `from(foreign_table.foreign_column)` condition, then the values are taken from
                // the foreign column.
                let structure = structure_conditions.get(
                    &config
                        .table
                        .get(table_name)
                        .ok_or(ValveError::InputError(format!(
                            "Undefined table '{}'",
                            table_name
                        )))?
                        .column
                        .get(column_name)
                        .ok_or(ValveError::InputError(format!(
                            "Undefined column '{}.{}'",
                            table_name, column_name
                        )))?
                        .structure,
                );

                let sql_type =
                    get_sql_type_from_global_config(&config, table_name, &column_name, db_kind);

                match structure {
                    Some(ParsedStructure { original, parsed }) => {
                        let matching_string = {
                            match matching_string {
                                None => "%".to_string(),
                                Some(s) => format!("%{}%", s),
                            }
                        };

                        match parsed {
                            Expression::Function(name, args) if name == "from" => {
                                let foreign_key = &args[0];
                                if let Expression::Field(ftable, fcolumn) = &**foreign_key {
                                    let fcolumn_text = cast_column_sql_to_text(&fcolumn, &sql_type);
                                    let sql = local_sql_syntax(
                                        db_kind,
                                        &format!(
                                            r#"SELECT "{}" FROM "{}" WHERE {} LIKE {}"#,
                                            fcolumn, ftable, fcolumn_text, SQL_PARAM
                                        ),
                                    );
                                    let rows = sqlx_query(&sql)
                                        .bind(&matching_string)
                                        .fetch_all(pool)
                                        .await?;
                                    for row in rows.iter() {
                                        values.push(get_column_value_as_string(
                                            &row, &fcolumn, &sql_type,
                                        ));
                                    }
                                }
                            }
                            Expression::Function(name, args) if name == "tree" => {
                                let mut tree_col = "not set";
                                let tree_key = &args[0];
                                if let Expression::Label(label) = &**tree_key {
                                    tree_col = label;
                                }

                                let tree = config
                                    .constraint
                                    .tree
                                    .get(table_name)
                                    .ok_or(ValveError::ConfigError(format!(
                                        "No tree config found for table '{}' found",
                                        table_name
                                    )))?
                                    .iter()
                                    .find(|t| t.child == *tree_col)
                                    .ok_or(ValveError::ConfigError(format!(
                                        "No tree: '{}.{}' found",
                                        table_name, tree_col
                                    )))?;
                                let child_column = &tree.child;

                                let tree_sql = with_tree_sql(&tree, &table_name.to_string(), None);
                                let child_column_text =
                                    cast_column_sql_to_text(&child_column, &sql_type);
                                let sql = local_sql_syntax(
                                    db_kind,
                                    &format!(
                                        r#"{} SELECT "{}" FROM "tree" WHERE {} LIKE {}"#,
                                        tree_sql, child_column, child_column_text, SQL_PARAM
                                    ),
                                );

                                let rows = sqlx_query(&sql)
                                    .bind(matching_string)
                                    .fetch_all(pool)
                                    .await?;
                                for row in rows.iter() {
                                    values.push(get_column_value_as_string(
                                        &row,
                                        &child_column,
                                        &sql_type,
                                    ));
                                }
                            }
                            _ => {
                                return Err(ValveError::DataError(format!(
                                    "Unrecognised structure: {}",
                                    original
                                ))
                                .into())
                            }
                        };
                    }
                    None => (),
                };
            }
        };

        let mut typeahead_values = vec![];
        for (i, v) in values.iter().enumerate() {
            // enumerate() begins at 0 but we need to begin at 1:
            let i = i + 1;
            typeahead_values.push(json!({
                "id": v,
                "label": v,
                "order": i,
            }));
        }

        Ok(json!(typeahead_values))
    }
}
