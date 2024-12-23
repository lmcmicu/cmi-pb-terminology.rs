//! Valve internal table definitions

use crate::{
    toolkit::DbKind,
    valve::{ValveColumnConfig, ValveTableConfig},
    INTERNAL_TABLES,
};
use indoc::indoc;
use std::collections::{HashMap, HashSet};

pub fn generate_internal_table_config(table_name: &str) -> ValveTableConfig {
    if !INTERNAL_TABLES.contains(&table_name) {
        panic!("Not an internal table: '{}'", table_name);
    }
    match table_name {
        "message" => ValveTableConfig {
            table: "message".to_string(),
            table_type: "message".to_string(),
            options: HashSet::from(["internal".to_string(), "truncate".to_string()]),
            description: "Validation messages for all of the tables and columns".to_string(),
            column_order: vec![
                "table".to_string(),
                "row".to_string(),
                "column".to_string(),
                "value".to_string(),
                "level".to_string(),
                "rule".to_string(),
                "message".to_string(),
            ],
            column: {
                let mut column_configs = HashMap::new();
                column_configs.insert(
                    "table".to_string(),
                    ValveColumnConfig {
                        table: "message".to_string(),
                        column: "table".to_string(),
                        description: "The table referred to by the message".to_string(),
                        datatype: "table_name".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "row".to_string(),
                    ValveColumnConfig {
                        table: "message".to_string(),
                        column: "row".to_string(),
                        description: "The row number of the table referred to by the message"
                            .to_string(),
                        datatype: "natural_number".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "column".to_string(),
                    ValveColumnConfig {
                        table: "message".to_string(),
                        column: "column".to_string(),
                        description: "The column of the table referred to by the message"
                            .to_string(),
                        datatype: "column_name".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "value".to_string(),
                    ValveColumnConfig {
                        table: "message".to_string(),
                        column: "value".to_string(),
                        description: "The value that is the reason for the message".to_string(),
                        datatype: "text".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "level".to_string(),
                    ValveColumnConfig {
                        table: "message".to_string(),
                        column: "level".to_string(),
                        description: "The severity of the violation".to_string(),
                        datatype: "word".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "rule".to_string(),
                    ValveColumnConfig {
                        table: "message".to_string(),
                        column: "rule".to_string(),
                        description: "The rule violated by the value".to_string(),
                        datatype: "CURIE".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "message".to_string(),
                    ValveColumnConfig {
                        table: "message".to_string(),
                        column: "message".to_string(),
                        description: "The message".to_string(),
                        datatype: "line".to_string(),
                        ..Default::default()
                    },
                );
                column_configs
            },
            ..Default::default()
        },
        "history" => ValveTableConfig {
            table: "history".to_string(),
            table_type: "history".to_string(),
            options: HashSet::from(["internal".to_string(), "truncate".to_string()]),
            description: "History of changes to the VALVE database".to_string(),
            column_order: vec![
                "table".to_string(),
                "row".to_string(),
                "from".to_string(),
                "to".to_string(),
                "summary".to_string(),
                "user".to_string(),
                "undone_by".to_string(),
                "timestamp".to_string(),
            ],
            column: {
                let mut column_configs = HashMap::new();
                column_configs.insert(
                    "table".to_string(),
                    ValveColumnConfig {
                        table: "history".to_string(),
                        column: "table".to_string(),
                        description: "The table referred to by the history entry".to_string(),
                        datatype: "table_name".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "row".to_string(),
                    ValveColumnConfig {
                        table: "history".to_string(),
                        column: "row".to_string(),
                        description: "The row number of the table referred to by the history entry"
                            .to_string(),
                        datatype: "natural_number".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "from".to_string(),
                    ValveColumnConfig {
                        table: "history".to_string(),
                        column: "from".to_string(),
                        description: "The initial value of the row".to_string(),
                        datatype: "text".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "to".to_string(),
                    ValveColumnConfig {
                        table: "history".to_string(),
                        column: "to".to_string(),
                        description: "The final value of the row".to_string(),
                        datatype: "text".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "summary".to_string(),
                    ValveColumnConfig {
                        table: "history".to_string(),
                        column: "summary".to_string(),
                        description: "Summarizes the changes to each column of the row".to_string(),
                        datatype: "text".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "user".to_string(),
                    ValveColumnConfig {
                        table: "history".to_string(),
                        column: "user".to_string(),
                        description: "User responsible for the change".to_string(),
                        datatype: "line".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "undone_by".to_string(),
                    ValveColumnConfig {
                        table: "history".to_string(),
                        column: "undone_by".to_string(),
                        description:
                            "User who has undone the change. Null if it has not been undone"
                                .to_string(),
                        datatype: "line".to_string(),
                        ..Default::default()
                    },
                );
                column_configs.insert(
                    "timestamp".to_string(),
                    ValveColumnConfig {
                        table: "history".to_string(),
                        column: "timestamp".to_string(),
                        description: "The time of the change, or of the undo".to_string(),
                        datatype: "line".to_string(),
                        ..Default::default()
                    },
                );
                column_configs
            },
            ..Default::default()
        },
        _ => todo!(
            "Table configuration for table '{}' is not implemented.",
            table_name
        ),
    }
}

pub fn generate_internal_table_ddl(
    table_name: &str,
    db_kind: &DbKind,
    text_type: &str,
) -> Vec<String> {
    if !INTERNAL_TABLES.contains(&table_name) {
        panic!("Not an internal table: '{}'", table_name);
    }

    let mut statements = vec![];
    match table_name {
        "history" => {
            statements.push(format!(
                indoc! {r#"
                    CREATE TABLE "history" (
                      {history_id}
                      "table" {text_type},
                      "row" BIGINT,
                      "from" {text_type},
                      "to" {text_type},
                      "summary" {text_type},
                      "user" {text_type},
                      "undone_by" {text_type},
                      {timestamp}
                    );
                  "#},
                history_id = {
                    if *db_kind == DbKind::Sqlite {
                        "\"history_id\" INTEGER PRIMARY KEY,"
                    } else {
                        "\"history_id\" SERIAL PRIMARY KEY,"
                    }
                },
                text_type = text_type,
                timestamp = {
                    if *db_kind == DbKind::Sqlite {
                        "\"timestamp\" TIMESTAMP DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))"
                    } else {
                        "\"timestamp\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                    }
                },
            ));
            statements
                .push(r#"CREATE INDEX "history_tr_idx" ON "history"("table", "row");"#.to_string());
            statements
        }
        "message" => {
            statements.push(format!(
                indoc! {r#"
                    CREATE TABLE "message" (
                      {message_id}
                      "table" {text_type},
                      "row" BIGINT,
                      "column" {text_type},
                      "value" {text_type},
                      "level" {text_type},
                      "rule" {text_type},
                      "message" {text_type}
                    );
                  "#},
                message_id = {
                    if *db_kind == DbKind::Sqlite {
                        "\"message_id\" INTEGER PRIMARY KEY,"
                    } else {
                        "\"message_id\" SERIAL PRIMARY KEY,"
                    }
                },
                text_type = text_type,
            ));
            statements.push(
                r#"CREATE INDEX "message_trc_idx" ON "message"("table", "row", "column");"#
                    .to_string(),
            );
            statements
        }
        _ => todo!("Table DDL for table '{}' is not implemented.", table_name),
    }
}
