//! Command line interface

use crate::{
    tests::{run_api_tests, run_dt_hierarchy_tests},
    toolkit::{generic_select_with_message_values, local_sql_syntax, DbKind},
    valve::{JsonRow, Valve, ValveCell, ValveRow},
    REQUIRED_DATATYPE_COLUMNS, SQL_PARAM,
};
use ansi_term::Style;
use clap::{ArgAction, Parser, Subcommand};
use futures::TryStreamExt;
use promptly::prompt_default;
use serde_json::{json, Value as SerdeValue};
use sqlx::{query as sqlx_query, Row};
use std::{collections::HashMap, io};

// Help strings that are used in more than one subcommand:
static BUILD_ERROR: &str = "Error building Valve";
static COLUMN_HELP: &str = "A column name or label";
static DATATYPE_HELP: &str = "A datatype name";
static ROW_HELP: &str = "A row number";
static SAVE_DIR_HELP: &str = "Save tables to DIR instead of to their configured paths";
static TABLE_HELP: &str = "A table name";

#[derive(Parser)]
#[command(version,
          about = "Valve: A lightweight validation engine -- command line interface",
          long_about = None)]
pub struct Cli {
    /// Read the contents of the table table from the given TSV file. If unspecified, Valve
    /// will read the table table location from the environment variable VALVE_SOURCE or exit
    /// with an error if it is undefined.
    #[arg(long, action = ArgAction::Set, env = "VALVE_SOURCE")]
    pub source: String,

    /// Can be one of (A) A URL of the form `postgresql://...` or `sqlite://...` (B) The filename
    /// (including path) of a sqlite database. If not specified, Valve will read the database
    /// location from the environment variable VALVE_DATABASE, or exit with an error if it is
    /// undefined.
    #[arg(long, action = ArgAction::Set, env = "VALVE_DATABASE")]
    pub database: String,

    /// Can be one of: JSON (that's it for now). If unspecified Valve will attempt to read the
    /// environment variable VALVE_INPUT. If that is also unset, the user will be presented with
    /// questions whenever input is required.
    #[arg(long, action = ArgAction::Set, env = "VALVE_INPUT")]
    pub input: Option<String>,

    /// Use this option with caution. When set, Valve will not not ask the user for confirmation
    /// before executing potentially destructive operations on the database and/or table files.
    #[arg(long, action = ArgAction::SetTrue)]
    pub assume_yes: bool,

    /// Print more information about progress and results to the terminal
    #[arg(long, action = ArgAction::SetTrue)]
    pub verbose: bool,

    // Subcommands:
    #[command(subcommand)]
    pub command: Commands,
}

// Note that the subcommands are declared below in the order in which we want them to appear
// in the usage statement that is printed when valve is run with the option `--help`.
#[derive(Subcommand)]
pub enum Commands {
    /// Load all of the tables
    LoadAll {},

    /// Load a particular table
    Load {
        #[arg(long,
              action = ArgAction::SetTrue,
              help = "(SQLite only) When this flag is set, the database \
                      settings will be tuned for initial loading. Note that \
                      these settings are unsafe and should be used for \
                      initial loading of a table only, as data integrity will not be \
                      guaranteed in the case of an interrupted transaction.")]
        initial_load: bool,

        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// Ensure that all of the tables have been created as configured and are empty.
    CreateAll {},

    /// Drop all of the tables
    DropAll {},

    /// Drop a particular table without deleting it from the Valve configuration.
    Drop {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// Truncate all of the tables
    TruncateAll {},

    /// Truncate a particular table
    Truncate {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// Save (a given list of) tables as TSV files. If LIST is empty, save all of the saveable
    /// tables. Otherwise save the tables in LIST. If the --save_dir option is given, tables are
    /// saved, with their current filename, under DIR instead of their default locations.
    Save {
        #[arg(value_name = "LIST",
              action = ArgAction::Set,
              value_delimiter = ' ',
              help = "A space-separated list of tables to save. Note that table names with spaces \
                      must be enclosed within quotes.")]
        tables: Option<Vec<String>>,

        #[arg(long, value_name = "DIR", action = ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
    },

    /// Save a table under a different filename.
    SaveAs {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "PATH", action = ArgAction::Set,
              help = "The new location of the table's source file")]
        path: String,
    },

    /// Get data from the database
    Get {
        #[command(subcommand)]
        subcommand: GetSubcommands,
    },

    /// Validate rows
    Validate {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: Option<u32>,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: Option<String>,

        #[arg(value_name = "VALUE", action = ArgAction::Set,
              help = "The value, of the given column, to validate")]
        value: Option<String>,
    },

    /// Add tables, rows, and messages to a given database
    Add {
        #[command(subcommand)]
        subcommand: AddSubcommands,
    },

    /// Update rows, messages, and values in the database
    Update {
        #[command(subcommand)]
        subcommand: UpdateSubcommands,
    },

    /// Delete rows and messages from the database
    Delete {
        #[command(subcommand)]
        subcommand: DeleteSubcommands,
    },

    /// Reorder rows in database tables
    Move {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set,
              help = "The number of the row from TABLE to be moved")]
        row: u32,

        #[arg(value_name = "AFTER", action = ArgAction::Set,
              help = "The number of the row coming immediately before ROW in TABLE in the new row \
                      order. If this is 0, the row will be moved to the first position.")]
        after: u32,
    },

    /// Undo changes to the database
    Undo {},

    /// Redo changes to the database that have been undone
    Redo {},

    /// Show recent changes to the database
    History {
        #[arg(long, value_name = "CONTEXT", action = ArgAction::Set,
              help = "Number of lines of redo / undo context (0 = infinite)",
              default_value_t = 5)]
        context: usize,
    },

    /// Rename table, rows, columns, and datatypes
    Rename {
        #[command(subcommand)]
        subcommand: RenameSubcommands,
    },

    /// Run test suites designed to validate that Valve is installed and running correctly
    Test {
        #[command(subcommand)]
        subcommand: TestSubcommands,
    },
}

#[derive(Subcommand)]
pub enum GetSubcommands {
    /// Get the rows from a given table.
    Table {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// Get a row having a given row number from a given table.
    Row {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: u32,
    },

    /// Get a cell representing the value of a given column of a given row from a given table.
    Cell {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: u32,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,
    },

    /// Get the value of a given column of a given row from a given table.
    Value {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: u32,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,
    },

    /// Get validation messages from the message table.
    Messages {
        #[arg(long, action = ArgAction::Set, help = "Get the message with this specific ID",
              required = false)]
        message_id: Option<u16>,

        #[arg(long, action = ArgAction::Set, required = false,
              help = "Only get messages whose rule matches the SQL LIKE-clause given by RULE")]
        rule: Option<String>,

        #[arg(value_name = "TABLE", action = ArgAction::Set,
              help = "Only get messages for TABLE")]
        table: Option<String>,

        #[arg(value_name = "ROW", action = ArgAction::Set,
              help = "Only get messages for row ROW of table TABLE")]
        row: Option<u32>,

        #[arg(value_name = "COLUMN", action = ArgAction::Set,
              help = "Only get messages for column COLUMN of row ROW of table TABLE")]
        column: Option<String>,
    },

    /// View a table configuration
    TableConfig {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// View a column configuration
    ColumnConfig {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,
    },

    /// View a datatype configuration
    DatatypeConfig {
        #[arg(value_name = "DATATYPE", action = ArgAction::Set, help = DATATYPE_HELP)]
        datatype: String,
    },

    /// View the ancestors of a given datatype
    Ancestors {
        #[arg(value_name = "DATATYPE", action = ArgAction::Set, help = DATATYPE_HELP)]
        datatype: String,
    },

    /// View the full Valve configuration
    ValveConfig {},

    /// View the configured constraints for a given table
    Constraints {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// View the rules for a given table
    Rules {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: Option<String>,
    },

    /// View special configuration table names
    Special {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: Option<String>,
    },

    /// Print the SQL that is used to instantiate Valve-managed tables in a given database.
    Schema {},

    /// View the order in which Valve-managed tables will be created, as determined by their
    /// dependency relations.
    TableOrder {},

    /// View the incoming dependencies for each Valve-managed table.
    IncomingDeps {},

    /// View the outgoing dependencies for each Valve-managed table.
    OutgoingDeps {},
}

#[derive(Subcommand)]
pub enum AddSubcommands {
    /// Read a JSON-formatted string representing a row (of the form: { "column_1": value1,
    /// "column_2": value2, ...}) from STDIN and add it to a given table, optionally printing
    /// (when the global --verbose flag has been set) a JSON representation of the row, including
    /// validation information and its assigned row_number, to the terminal before exiting.
    Row {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// Read a JSON-formatted string representing a row (of the form: { "table": TABLE_NAME,
    /// "row": ROW_NUMBER, "column": COLUMN_NAME, "value": VALUE, "level": LEVEL,
    /// "rule": RULE, "message": MESSAGE}) from STDIN and add it to the message table. Note
    /// that if any of the "table", "row", or "column" fields are ommitted from the input JSON
    /// then they must be specified as positional parameters.
    Message {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: Option<String>,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: Option<u32>,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: Option<String>,
    },

    /// Add a table to the database
    Table {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "PATH", action = ArgAction::Set,
              help = "The TSV file representing the table to be added.")]
        path: String,

        #[arg(long, value_name = "SIZE", action = ArgAction::Set,
              help = "Sample size to use when guessing the table configuration",
              default_value_t = 10000)]
        sample_size: usize,

        #[arg(long, value_name = "RATE", action = ArgAction::Set,
              help = "A number between 0 and 1 (inclusive) representing the proportion of data \
                      errors (incorrect datatypes, unsatisfied data dependencies, etc.) that are \
                      expected to be in the data.",
              default_value_t = 0.1)]
        error_rate: f32,

        #[arg(long, value_name = "SEED", action = ArgAction::Set,
              help = "Use SEED to generate pseudo-random samples.")]
        seed: Option<u64>,

        #[arg(long, action = ArgAction::SetTrue, help = "Do not load the table after creating it")]
        no_load: bool,
    },

    /// Add a column to a database table
    Column {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: Option<String>,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: Option<String>,

        #[arg(long, action = ArgAction::SetTrue,
              help = "Do not load the table after adding COLUMN")]
        no_load: bool,
    },

    /// Add a datatype to the datatype table
    Datatype {
        #[arg(value_name = "DATATYPE", action = ArgAction::Set, help = DATATYPE_HELP)]
        datatype: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum UpdateSubcommands {
    /// Read a JSON-formatted string representing a row (of the form: { "column_1": value1,
    /// "column_2": value2, ...}) from STDIN and use it as a replacement for the row
    /// currently assigned the row number ROW in the given database table. If ROW is not given,
    /// then Valve expects there to be a field called "row_number" with an integer value in the
    /// input JSON.
    Row {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: Option<u32>,
    },

    /// Update the current value of the given column of the given row of the given table with
    /// VALUE.
    Value {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: u32,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,

        #[arg(value_name = "VALUE", action = ArgAction::Set,
              help = "The value, of the given column, to update")]
        value: String,
    },

    /// Read a JSON-formatted string representing a row (of the form: { "table": TABLE_NAME,
    /// "row": ROW_NUMBER, "column": COLUMN_NAME, "value": VALUE, "level": LEVEL,
    /// "rule": RULE, "message": MESSAGE}) from STDIN and update the row given identified by
    /// JSON_ID with the given values. Note that if any of the "table", "row", or "column" fields
    /// are ommitted from the input JSON then they must be specified as positional parameters.
    Message {
        #[arg(value_name = "MESSAGE_ID", action = ArgAction::Set,
              help = "The ID of the message to update")]
        message_id: u16,

        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: Option<String>,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: Option<u32>,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum DeleteSubcommands {
    /// Delete rows from a given table.
    Row {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, value_delimiter = ' ', num_args = 1..,
              help = ROW_HELP)]
        rows: Vec<u32>,
    },

    /// Delete messages from the message table.
    Messages {
        #[arg(long, action = ArgAction::Set, help = "The specific ID of the message to delete",
              required = false)]
        message_id: Option<u16>,

        #[arg(long, action = ArgAction::Set, required = false,
              help = "Delete all messages whose rule matches the SQL LIKE-clause given by RULE")]
        rule: Option<String>,
    },

    /// Delete a table
    Table {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// Delete a column from a given table
    Column {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,

        #[arg(long, action = ArgAction::SetTrue,
              help = "Do not load the table after deleting COLUMN")]
        no_load: bool,
    },

    /// Remove a datatype
    Datatype {
        #[arg(value_name = "DATATYPE", action = ArgAction::Set, help = DATATYPE_HELP)]
        datatype: String,
    },
}

#[derive(Subcommand)]
pub enum RenameSubcommands {
    /// Rename a table
    Table {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "NEW_NAME", action = ArgAction::Set,
              help = "The desired new name for the table")]
        new_name: String,
    },

    /// Rename a datatype
    Datatype {
        #[arg(value_name = "DATATYPE", action = ArgAction::Set, help = DATATYPE_HELP)]
        datatype: String,

        #[arg(value_name = "NEW_NAME", action = ArgAction::Set,
              help = "The desired new name for the column")]
        new_name: String,
    },

    /// Rename a column
    Column {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,

        #[arg(value_name = "NEW_NAME", action = ArgAction::Set,
              help = "The desired new name for the column")]
        new_name: String,

        #[arg(value_name = "NEW_LABEL", action = ArgAction::Set,
              help = "The desired new label for the column")]
        new_label: Option<String>,

        #[arg(long, action = ArgAction::SetTrue,
              help = "Do not load the table after deleting COLUMN")]
        no_load: bool,
    },
}

#[derive(Subcommand)]
pub enum TestSubcommands {
    /// Run a set of predefined tests, on a specified pre-loaded database, that will test Valve's
    /// Application Programmer Interface.
    Api {},

    /// Run a set of predefined tests, on a specified pre-loaded database, that will test the
    /// validity of the configured datatype hierarchy.
    DtHierarchy {},
}

/// Build a Valve instance in conformity with the given command-line options and parameters.
pub async fn build_valve(cli: &Cli) -> Valve {
    let mut valve = Valve::build(&cli.source, &cli.database)
        .await
        .expect(BUILD_ERROR);
    valve.set_verbose(cli.verbose);
    valve.set_interactive(!cli.assume_yes);
    valve
}

/// Use Valve, in conformity with the given command-line parameters, to add a column to the
/// given table in the database. The column details are read from STDIN. If `table` and `column`
/// are not given then the fields "table" and "column", respectively, must be present in the input.
/// If the `no_load` option is set, do not load the modified table.
pub async fn add_column(cli: &Cli, table: &Option<String>, column: &Option<String>, no_load: bool) {
    let mut valve = build_valve(&cli).await;
    let json_row = {
        match &cli.input {
            Some(s) if s == "JSON" => read_json_row_for_table(&valve, &valve.config.special.column),
            Some(s) => panic!("Unsupported input type: '{s}'"),
            None => prompt_for_column_columns(&valve, table, column),
        }
    };
    let column_json = extract_column_fields(&json_row, table, column);

    valve
        .add_column(table, column, &column_json)
        .await
        .expect("Error adding column");

    let column = column_json
        .get("column")
        .expect("No column given")
        .as_str()
        .expect("Not a string");
    let table = column_json
        .get("table")
        .expect("No table given")
        .as_str()
        .expect("Not a string");
    let load_table = {
        if no_load {
            false
        } else if !valve.interactive {
            true
        } else {
            // Ask the user if they would like to load now:
            print!(
                "Added column '{column}' to '{table}'. Do you want to load '{table}' \
                 now? [y/N] ",
            );
            proceed::proceed()
        }
    };
    if load_table {
        valve
            .load_tables(&vec![table], true)
            .await
            .expect("Error loading table");
    }
    if cli.verbose {
        // If the verbose flag has been set, write a JSON representation of the column
        // to STDOUT:
        println!("{}", json!(column_json));
    }
}

/// Use Valve, in conformity with the given command-line parameters, to add a datatype to the
/// database. The datatype details are read from STDIN. Note that if `datatype` is not given then
/// a field called "datatype" must be present in the input.
pub async fn add_datatype(cli: &Cli, datatype: &Option<String>) {
    let mut valve = build_valve(&cli).await;
    let json_row = match &cli.input {
        Some(s) if s == "JSON" => read_json_row_for_table(&valve, &valve.config.special.datatype),
        Some(s) => panic!("Unsupported input type: '{s}'"),
        None => prompt_for_datatype_columns(&valve, datatype),
    };
    let dt_fields = extract_datatype_fields(&valve, datatype, &json_row);
    valve
        .add_datatype(&dt_fields)
        .await
        .expect("Error adding datatype");
    if cli.verbose {
        // If the verbose flag has been set, write a JSON representation of the datatype
        // to STDOUT:
        println!("{}", json!(dt_fields));
    }
}

/// Use Valve, in conformity with the given command-line parameters, to add a message to the
/// message table. The details of the message are read from STDIN. Note that if any of the
/// parameters, `table`, `row`, or `column` have not been specified, they must be provided in the
/// input.
pub async fn add_message(
    cli: &Cli,
    table: &Option<String>,
    row: &Option<u32>,
    column: &Option<String>,
) {
    let valve = build_valve(&cli).await;
    let json_row = match &cli.input {
        Some(s) if s == "JSON" => read_json_row_for_table(&valve, "message"),
        Some(s) => panic!("Unsupported input type '{s}'"),
        None => prompt_for_message_columns(&valve, table, row, column),
    };
    let (table, row, column, value, level, rule, message) =
        extract_message_fields(table, row, column, &json_row);
    let message_id = valve
        .insert_message(&table, row, &column, &value, &level, &rule, &message)
        .await
        .expect("Error inserting message");
    if cli.verbose {
        println!(
            "{}",
            json!({
                "ID": message_id,
                "contents": {
                    "table": table,
                    "row": row,
                    "column": column,
                    "value": value,
                    "level": level,
                    "rule": rule,
                    "message": message,
                }
            })
        );
    } else {
        println!("{message_id}");
    }
}

/// Use Valve, in conformity with the given command-line parameters, to add a row to the given
/// table in the database. The row details are read from STDIN.
pub async fn add_row(cli: &Cli, table: &str) {
    let valve = build_valve(&cli).await;
    let json_row = match &cli.input {
        Some(s) if s == "JSON" => read_json_row_for_table(&valve, table),
        Some(s) => panic!("Unsupported input type '{s}'"),
        None => prompt_for_table_columns(&valve, table, &None, false),
    };
    let (rn, row) = valve
        .insert_row(table, &json_row)
        .await
        .expect("Error inserting row");
    if cli.verbose {
        println!(
            "{}",
            json!(row
                .to_rich_json()
                .expect("Error converting row to rich JSON"))
        );
    } else {
        println!("{rn}");
    }
}

/// Use Valve, in conformity with the given command-line parameters, to add a given `table`, located
/// at a given `path`, to the database. If `no_load` is set, do not load the newly created table.
/// The arguments `sample_size`, `error_rate`, and `seed` are passed to
/// [guess()](crate::guess::guess()), which is used to guess the new table's configuration.
pub async fn add_table(
    cli: &Cli,
    table: &str,
    path: &str,
    sample_size: &usize,
    error_rate: &f32,
    seed: &Option<u64>,
    no_load: bool,
) {
    let mut valve = build_valve(&cli).await;
    valve
        .add_table(table, path, sample_size, error_rate, seed)
        .await
        .expect("Error adding table");
    let load_table = {
        if no_load {
            false
        } else if !valve.interactive {
            true
        } else {
            // Ask the user if they would like to load now:
            print!("Added table '{table}'. Do you want to load '{table}' now? [y/N] ",);
            proceed::proceed()
        }
    };
    if load_table {
        valve
            .load_tables(&vec![table], true)
            .await
            .expect("Error loading table");
    }
}

/// Use Valve, in conformity with the given command-line parameters, to (re)create an empty
/// database.
pub async fn create_all(cli: &Cli) {
    let mut valve = build_valve(&cli).await;
    // We turn interactive mode off since this is an "all" operation:
    valve.set_interactive(false);
    valve
        .truncate_all_tables()
        .await
        .expect("Error truncating tables");
    valve
        .ensure_all_tables_created(&vec![])
        .await
        .expect("Error ensuring that all tables are created");
}

/// Use Valve, in conformity with the given command-line parameters, to delete the given datatype.
pub async fn delete_datatype(cli: &Cli, datatype: &str) {
    let mut valve = build_valve(&cli).await;
    valve
        .delete_datatype(datatype)
        .await
        .expect("Error deleting datatype");
}

/// Use Valve, in conformity with the given command-line parameters, to delete a given column
/// from a given table.
pub async fn delete_column(cli: &Cli, table: &str, column: &str, no_load: bool) {
    let mut valve = build_valve(&cli).await;
    valve
        .delete_column(table, column)
        .await
        .expect("Error deleting column");

    // Possibly load the data table:
    let load_table = {
        if no_load {
            false
        } else if !valve.interactive {
            true
        } else {
            // Ask the user if they would like to load now:
            print!(
                "Column '{column}' deleted from '{table}'. Do you want to load '{table}' \
                 now? [y/N] ",
            );
            proceed::proceed()
        }
    };
    if load_table {
        valve
            .load_tables(&vec![table], true)
            .await
            .expect("Error loading table");
    }
}

/// Use Valve, in conformity with the given command-line parameters, to either (i) in the case
/// where a `message_id` has been given, delete that particular message, or (ii) delete any
/// messages whose rule matches `rule`, where the latter is in the form of a SQL LIKE clause,
/// which may contain wildcards and can in principle match multiple messages.
pub async fn delete_messages_by_id_or_rule(
    cli: &Cli,
    message_id: &Option<u16>,
    rule: &Option<String>,
) {
    let valve = build_valve(&cli).await;
    if let Some(message_id) = message_id {
        valve
            .delete_message(*message_id)
            .await
            .expect("Error deleting message");
    } else {
        match rule {
            Some(rule) => valve
                .delete_messages_like(rule)
                .await
                .expect("Error deleting messages"),
            None => panic!(
                "Either a MESSAGE_ID or a RULE (possibly with wildcards) \
                 is required. To delete all messages use the option '--rule %'."
            ),
        }
    }
}

/// Use Valve, in conformity with the given command-line parameters, to delete the given table
/// from the column table. Also drop the table in the database unless the `no_drop` flag has been
/// set.
pub async fn delete_table(cli: &Cli, table: &str) {
    let mut valve = build_valve(&cli).await;
    valve
        .delete_table(table)
        .await
        .expect("Error deleting table");
}

/// Use Valve, in conformity with the given command-line parameters, to delete the rows with the
/// given row numbers from the given table.
pub async fn delete_rows(cli: &Cli, table: &str, rows: &Vec<u32>) {
    let valve = build_valve(&cli).await;
    for row in rows {
        valve
            .delete_row(table, row)
            .await
            .expect(&format!("Error deleting row {row}"));
    }
}

/// Use Valve, in conformity with the given command-line parameters, to drop all of the tables.
pub async fn drop_all_tables(cli: &Cli) {
    let mut valve = build_valve(&cli).await;
    valve
        .drop_all_tables()
        .await
        .expect("Error dropping all tables");
}

/// Use Valve, in conformity with the given command-line parameters, to drop the given table.
pub async fn drop_table(cli: &Cli, table: &str) {
    let valve = build_valve(&cli).await;
    valve
        .drop_tables(&vec![table])
        .await
        .expect("Error dropping tables");
}

/// Use Valve, in conformity with the given command-line parameters, to print the ancestor
/// datatypes of the given datatype.
pub async fn print_ancestors(cli: &Cli, datatype: &str) {
    let valve = build_valve(&cli).await;
    println!(
        "{}",
        valve
            .get_datatype_ancestor_names(datatype)
            .iter()
            .map(|name| {
                if name.contains(" ") {
                    format!("'{name}'")
                } else {
                    name.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    );
}

/// Use Valve, in conformity with the given command-line parameters, to print the [ValveCell]
/// corresponding to the given column of the given row of the given table.
pub async fn print_cell(cli: &Cli, table: &str, row: u32, column: &str) {
    let valve = build_valve(&cli).await;
    let cell = valve
        .get_cell_from_db(table, &row, column)
        .await
        .expect("Error getting cell");
    println!(
        "{}",
        json!(cell
            .to_rich_json()
            .expect("Error converting cell to rich JSON"))
    );
}

/// Use Valve, in conformity with the given command-line parameters, to print the column
/// configuration of the given column of the given table.
pub async fn print_column_config(cli: &Cli, table: &str, column: &str) {
    let valve = build_valve(&cli).await;
    let column_config = valve
        .config
        .table
        .get(table)
        .expect(&format!("Table '{table}' not found"))
        .column
        .get(column)
        .expect(&format!("Column '{column}' not found"));
    println!("{}", json!(column_config));
}

/// Use Valve, in conformity with the given command-line parameters, to print the constraints that
/// are associated with the given table.
pub async fn print_constraints(cli: &Cli, table: &str) {
    let valve = build_valve(&cli).await;
    let mut table_constraints = HashMap::new();
    table_constraints.insert(
        "primary",
        json!(valve
            .config
            .constraint
            .primary
            .get(table)
            .expect(&format!("No table '{table}'"))),
    );
    table_constraints.insert(
        "unique",
        json!(valve
            .config
            .constraint
            .unique
            .get(table)
            .expect(&format!("No table '{table}'"))),
    );
    table_constraints.insert(
        "foreign",
        json!(valve
            .config
            .constraint
            .foreign
            .get(table)
            .expect(&format!("No table '{table}'"))),
    );
    table_constraints.insert(
        "tree",
        json!(valve
            .config
            .constraint
            .tree
            .get(table)
            .expect(&format!("No table '{table}'"))),
    );
    println!("{}", json!(table_constraints));
}

/// Use Valve, in conformity with the given command-line parameters, to print the given datatype's
/// datatype configuration.
pub async fn print_datatype_config(cli: &Cli, datatype: &str) {
    let valve = build_valve(&cli).await;
    let dt_config = valve
        .config
        .datatype
        .get(datatype)
        .expect(&format!("Datatype '{datatype}' not found"));
    println!("{}", json!(dt_config));
}

/// Use Valve, in conformity with the given command-line parameters, to print messages from the
/// message table. If none of `message_id`, `table`, `row`, `column`, or `rule` have been given,
/// print all of the messages in the message table. Otherwise there are two mutually exclusive
/// options:
/// (i) If `message_id` is given, print the particular message and exit.
/// (ii), i.e.,
///   (iia) If `table` is given, only show the messages for that table. If, in addition, `row` is
///     given, show only messages for that row of the table. And if `column` is also given, show
///     only messages for that column in the row.
///   and/or
///   (iib) If `rule` is given, show only messages with that rule.
/// Note that (iia) and (iib) are compatible, i.e., one can filter by table/row/column and by rule.
pub async fn print_messages(
    cli: &Cli,
    table: &Option<String>,
    row: &Option<u32>,
    column: &Option<String>,
    rule: &Option<String>,
    message_id: &Option<u16>,
) {
    let valve = build_valve(&cli).await;
    let mut sql = format!(
        r#"SELECT "message_id", "table", "row", "column", "value", "level", "rule", "message"
           FROM "message""#
    );
    let mut sql_params = vec![];
    match message_id {
        Some(message_id) => {
            sql.push_str(&format!(r#" WHERE "message_id" = {message_id}"#));
        }
        None => {
            // The command line parser should have taken care of this but we add in a check
            // to be certain:
            if *row != None && *table == None {
                unreachable!("A table must be given when a row is given");
            }
            if *column != None && (*table == None || *row == None) {
                unreachable!("A table and row must be given when a column is given");
            }

            if let Some(table) = table {
                sql.push_str(&format!(r#"WHERE "table" = {SQL_PARAM}"#));
                sql_params.push(table);
            }
            if let Some(row) = row {
                sql.push_str(&format!(r#" AND "row" = {row}"#));
            }
            if let Some(column) = column {
                sql.push_str(&format!(r#" AND "column" = {SQL_PARAM}"#));
                sql_params.push(column);
            }
            if let Some(rule) = rule {
                sql.push_str(&format!(
                    r#" {connective} "rule" LIKE {SQL_PARAM}"#,
                    connective = match table {
                        None => "WHERE",
                        Some(_) => "AND",
                    }
                ));
                sql_params.push(rule);
            }
        }
    };
    let sql = local_sql_syntax(
        &valve.db_kind,
        &format!(r#"{sql} ORDER BY "table", "row", "column", "message_id""#,),
    );
    let mut query = sqlx_query(&sql);
    for param in &sql_params {
        query = query.bind(param);
    }

    let mut row_stream = query.fetch(&valve.pool);
    let mut is_first = true;
    print!("[");
    while let Some(row) = row_stream
        .try_next()
        .await
        .expect("Error fetching row from stream")
    {
        if !is_first {
            print!(",");
        } else {
            is_first = false;
        }
        let rn: i64 = row.get::<i64, _>("row");
        let rn = rn as u32;
        let mid: i32 = row.get::<i32, _>("message_id");
        let mid = mid as u16;
        println!(
            "{{\"message_id\":{},\"table\":{},\"row\":{},\"column\":{},\
             \"value\":{},\"level\":{},\"rule\":{},\"message\":{}}}",
            mid,
            json!(row.get::<&str, _>("table")),
            rn,
            json!(row.get::<&str, _>("column")),
            json!(row.get::<&str, _>("value")),
            json!(row.get::<&str, _>("level")),
            json!(row.get::<&str, _>("rule")),
            json!(row.get::<&str, _>("message")),
        );
    }
    println!("]");
}

/// Use Valve, in conformity with the given command-line parameters, to print the row with the
/// given row number from the given table.
pub async fn print_row(cli: &Cli, table: &str, row: u32) {
    let valve = build_valve(&cli).await;
    let row = valve
        .get_row_from_db(table, &row)
        .await
        .expect("Error getting row");
    println!(
        "{}",
        json!(row
            .to_rich_json()
            .expect("Error converting row to rich JSON"))
    );
}

/// Use Valve, in conformity with the given command-line parameters, to print the rules
/// associated with the given table, optionally filtered by the given column.
pub async fn print_rules(cli: &Cli, table: &str, column: &Option<String>) {
    let valve = build_valve(&cli).await;
    if !valve.config.table.contains_key(table) {
        panic!("No table config for '{table}'");
    }
    if let Some(table_rules) = valve.config.rule.get(table) {
        match column {
            Some(column) => {
                if let Some(column_rules) = table_rules.get(column) {
                    println!("{}", json!(column_rules));
                }
            }
            None => println!("{}", json!(table_rules)),
        };
    }
}

/// Use Valve, in conformity with the given command-line parameters, to print the database schema.
pub async fn print_schema(cli: &Cli) {
    let valve = build_valve(&cli).await;
    let schema = valve.dump_schema().await.expect("Error dumping schema");
    println!("{}", schema);
}

/// Use Valve, in conformity with the given command-line parameters, to print the names of the
/// special configuration tables.
pub async fn print_special(cli: &Cli, table: &Option<String>) {
    let valve = build_valve(&cli).await;

    match table {
        None => {
            println!("Table table name: '{}'", valve.config.special.table);
            println!("Column table name: '{}'", valve.config.special.column);
            println!("Datatype table name: '{}'", valve.config.special.datatype);
            println!("Rule table name: '{}'", valve.config.special.rule);
        }
        Some(table) => {
            let table = table.to_string();
            match table.as_str() {
                "table" => println!("{}", valve.config.special.table),
                "column" => println!("{}", valve.config.special.column),
                "datatype" => println!("{}", valve.config.special.datatype),
                "rule" => println!("{}", valve.config.special.rule),
                _ => panic!("Not a special table type: '{table}'"),
            };
        }
    };
}

/// Use Valve, in conformity with the given command-line parameters, to print all of the rows
/// in the given table.
pub async fn print_table(cli: &Cli, table: &str) {
    let valve = build_valve(&cli).await;

    let (sql, sql_params) =
        generic_select_with_message_values(table, &valve.config, &valve.db_kind);
    let sql = local_sql_syntax(&valve.db_kind, &sql);
    let mut query = sqlx_query(&sql);
    for param in &sql_params {
        query = query.bind(param);
    }

    let mut row_stream = query.fetch(&valve.pool);
    let mut is_first = true;
    print!("[");
    while let Some(row) = row_stream
        .try_next()
        .await
        .expect("Error fetching row from stream")
    {
        if !is_first {
            print!(",");
        } else {
            is_first = false;
        }
        let row = ValveRow::from_any_row(&valve.config, &valve.db_kind, table, &row, &None)
            .expect("Error converting to ValveRow");
        println!(
            "{}",
            json!(row
                .to_rich_json()
                .expect("Error converting row to rich JSON"))
        );
    }
    println!("]");
}

/// Use Valve, in conformity with the given command-line parameters, to print the table
/// configuration for the given table.
pub async fn print_table_config(cli: &Cli, table: &str) {
    let valve = build_valve(&cli).await;

    let table_config = valve
        .config
        .table
        .get(table)
        .expect(&format!("{table} not found"));
    println!("{}", json!(table_config));
}

/// Use Valve, in conformity with the given command-line parameters, to print the list of
/// configured tables in sorted order.
pub async fn print_table_order(cli: &Cli) {
    let valve = build_valve(&cli).await;
    let sorted_table_list = valve.get_sorted_table_list();
    println!("{}", sorted_table_list.join(", "));
}

/// Use Valve, in conformity with the given command-line parameters, to print the value of the
/// given column of the given row of the given table.
pub async fn print_value(cli: &Cli, table: &str, row: u32, column: &str) {
    let valve = build_valve(&cli).await;
    let cell = valve
        .get_cell_from_db(table, &row, column)
        .await
        .expect("Error getting cell");
    println!("{}", cell.strvalue());
}

/// Use Valve, in conformity with the given command-line parameters, to print the global Valve
/// configuration.
pub async fn print_valve_config(cli: &Cli) {
    let valve = build_valve(&cli).await;
    println!("{}", valve.config)
}

/// Use Valve, in conformity with the given command-line parameters, to print a list of operations
/// that have been previously executed and that can be undone, a list of such operations that
/// can be redone, and markers to indicate where the state of the Valve instance with respect to
/// those lists. When `context` is non-zero, limit the redo and undo lists to no more than that
/// many operations.
pub async fn print_history(cli: &Cli, context: usize) {
    let valve = build_valve(&cli).await;
    let mut undo_history = valve
        .get_changes_to_undo(context)
        .await
        .expect("Error getting changes to undo");
    let next_undo = match undo_history.len() {
        0 => 0,
        _ => undo_history[0].history_id,
    };
    undo_history.reverse();
    let id_width = next_undo.to_string().len();
    for undo in &undo_history {
        if undo.history_id == next_undo {
            let line = format!("▲ {:>id_width$} {}", undo.history_id, undo.message);
            println!("{}", Style::new().bold().paint(line));
        } else {
            println!("  {:>id_width$} {}", undo.history_id, undo.message);
        }
    }

    let redo_history = valve
        .get_changes_to_redo(context)
        .await
        .expect("Error getting changes to redo");
    let next_redo = match redo_history.len() {
        0 => 0,
        _ => redo_history[0].history_id,
    };
    let mut highest_encountered_id = 0;
    for redo in &redo_history {
        if redo.history_id > highest_encountered_id {
            highest_encountered_id = redo.history_id;
        }
        // We do not allow redoing changes that are older than the next record to undo.
        // If there are no such changes in the redo stack, then there will be no triangle,
        // which indicates that nothing can be redone even though there are entries in the
        // redo stack.
        if redo.history_id == next_redo && redo.history_id > next_undo {
            println!("▼ {:>id_width$} {}", redo.history_id, redo.message);
        } else {
            let line = format!("  {:>id_width$} {}", redo.history_id, redo.message);
            // If the history_id under consideration is lower than the next undo, or if
            // there is a redo operation appearing before this one in the returned results
            // that has a greater history_id, then this is an orphaned operation that cannot
            // be redone. We choose not to include orphaned ops in the history output:
            if redo.history_id >= next_undo && redo.history_id >= highest_encountered_id {
                println!("{line}");
            }
        }
    }
}

/// Use Valve, in conformity with the given command-line parameters, to load all of the tables in
/// the database.
pub async fn load_all(cli: &Cli) {
    let mut valve = build_valve(&cli).await;
    valve
        .configure_for_initial_load()
        .await
        .expect("Could not configure for initial load");
    valve
        .load_all_tables(true)
        .await
        .expect("Error loading tables");
}

/// Use Valve, in conformity with the given command-line parameters, to load a table. When
/// `initial_load` has been set, then (SQLite only) use unsafe parameters that are normally
/// only reserved for the initial loading of a freshly created database.
pub async fn load_table(cli: &Cli, table: &str, initial_load: bool) {
    let mut valve = build_valve(&cli).await;
    if initial_load {
        if valve.db_kind == DbKind::Sqlite && !cli.assume_yes {
            print!(
                "--initial-load enables options intended for use on an empty database. \
                 It should not normally be set when loading a single table as it is \
                 unsafe and could result in data corruption in the case of an \
                 interrupted transaction. Are you sure you want to continue? [y/N] "
            );
            if !proceed::proceed() {
                std::process::exit(1);
            }
        }
        valve
            .configure_for_initial_load()
            .await
            .expect("Could not configure for initial load");
    }
    valve
        .load_tables(&vec![table], true)
        .await
        .expect("Error loading table");
}

/// Use Valve, in conformity with the given command-line parameters, to move the given `row` from
/// the given `table` so that it comes after the row `after` in `table`.
pub async fn move_row(cli: &Cli, table: &str, row: u32, after: u32) {
    let valve = build_valve(&cli).await;
    valve
        .move_row(table, &row, &after)
        .await
        .expect("Error moving row");
}

/// Use Valve, in conformity with the given command-line parameters, to undo the last operation and
/// print the information, if any, that resulted from the undo.
pub async fn undo(cli: &Cli) {
    let mut valve = build_valve(&cli).await;
    let updated_row = valve.undo().await.expect("Error undoing");
    if let Some(valve_row) = updated_row {
        if cli.verbose {
            print!(
                "{}",
                json!(valve_row
                    .to_rich_json()
                    .expect("Error converting row to rich JSON"))
            );
        }
    }
}

/// Use Valve, in conformity with the given command-line parameters, to redo the last operation
/// that was undone and print the information, if any, that resulted from the redo.
pub async fn redo(cli: &Cli) {
    let mut valve = build_valve(&cli).await;
    let updated_row = valve.redo().await.expect("Error redoing");
    if let Some(valve_row) = updated_row {
        if cli.verbose {
            print!(
                "{}",
                json!(valve_row
                    .to_rich_json()
                    .expect("Error converting row to rich JSON"))
            );
        }
    }
}

/// Use Valve, in conformity with the given command-line parameters, to rename the given column
/// of the given table to the given new name, optionally with a given label. Note that if no
/// label is given it will be left unset. The column's old label will not be retained.
pub async fn rename_column(
    cli: &Cli,
    table: &str,
    column: &str,
    new_name: &str,
    label: &Option<String>,
    no_load: bool,
) {
    let mut valve = build_valve(&cli).await;
    valve
        .rename_column(table, column, new_name, label)
        .await
        .expect("Error renaming column");

    let load_table = {
        if no_load {
            false
        } else if !valve.interactive {
            true
        } else {
            // Ask the user if they would like to load now:
            print!(
                "Column '{column}' renamed in '{table}'. Do you want to load '{table}' \
                 now? [y/N] ",
            );
            proceed::proceed()
        }
    };
    if load_table {
        valve
            .load_tables(&vec![table], true)
            .await
            .expect("Error loading table");
    }
}

/// Use Valve, in conformity with the given command-line parameters, to rename the given datatype
/// to the given new datatype name.
pub async fn rename_datatype(cli: &Cli, datatype: &str, new_name: &str) {
    let mut valve = build_valve(&cli).await;
    valve
        .rename_datatype(datatype, new_name)
        .await
        .expect("Error renaming datatype");
}

/// Use Valve, in conformity with the given command-line parameters, to rename the given table
/// to the given new table name.
pub async fn rename_table(cli: &Cli, table: &str, new_name: &str) {
    let mut valve = build_valve(&cli).await;
    valve
        .rename_table(table, new_name)
        .await
        .expect("Error renaming table");
}

/// Use Valve, in conformity with the given command-line parameters, to save the given tables,
/// or all tables if no table list is given, to the given save directory, or to each table's
/// configured path if no save directory is given.
pub async fn save(cli: &Cli, tables: &Option<Vec<String>>, save_dir: &Option<String>) {
    let valve = build_valve(&cli).await;
    match tables {
        None => {
            valve
                .save_all_tables(&save_dir)
                .await
                .expect("Error saving tables");
        }
        Some(tables) => {
            let tables = tables.iter().map(|s| s.as_str()).collect::<Vec<_>>();
            valve
                .save_tables(&tables, &save_dir)
                .await
                .expect("Error saving tables");
        }
    };
}

/// Use Valve, in conformity with the given command-line parameters, to save the given table
/// to the given path.
pub async fn save_as(cli: &Cli, table: &str, path: &str) {
    let valve = build_valve(&cli).await;
    valve
        .save_table(table, path)
        .await
        .expect("Error saving table");
}

/// Use Valve, in conformity with the given command-line parameters, to run a set of functions
/// designed to test Valve's Application Programmer Interface.
pub async fn test_api(cli: &Cli) {
    let mut valve = build_valve(&cli).await;
    run_api_tests(&mut valve)
        .await
        .expect("Error running API tests");
}

/// Use Valve, in conformity with the given command-line parameters, to run a set of functions
/// designed to test how Valve handles datatype hierarchies.
pub async fn test_dt_hierarchy(cli: &Cli) {
    let valve = build_valve(&cli).await;
    run_dt_hierarchy_tests(&valve).expect("Error running datatype hierarchy tests");
}

/// Use Valve, in conformity with the given command-line parameters, to truncate all of the tables.
pub async fn truncate_all_tables(cli: &Cli) {
    let mut valve = build_valve(&cli).await;
    valve
        .truncate_all_tables()
        .await
        .expect("Error truncating tables");
}

/// Use Valve, in conformity with the given command-line parameters, to truncate the given table.
pub async fn truncate_table(cli: &Cli, table: &str) {
    let mut valve = build_valve(&cli).await;
    valve
        .truncate_tables(&vec![table])
        .await
        .expect("Error truncating table");
}

/// Use Valve, in conformity with the given command-line parameters, to update a message in the
/// message table. The details of the message are read from STDIN. Note that if any of `table`,
/// `row`, or `column` have not been specified, then they must be provided in the input.
pub async fn update_message(
    cli: &Cli,
    message_id: u16,
    table: &Option<String>,
    row: &Option<u32>,
    column: &Option<String>,
) {
    let valve = build_valve(&cli).await;
    let json_row = match &cli.input {
        Some(s) if s == "JSON" => read_json_row_for_table(&valve, "message"),
        Some(s) => panic!("Unsupported input type '{s}'"),
        None => prompt_for_message_columns(&valve, table, row, column),
    };
    let (table, row, column, value, level, rule, message) =
        extract_message_fields(table, row, column, &json_row);
    valve
        .update_message(
            message_id, &table, row, &column, &value, &level, &rule, &message,
        )
        .await
        .expect("Error updating message");
    if cli.verbose {
        println!(
            "{}",
            json!({
                "ID": message_id,
                "contents": {
                    "table": table,
                    "row": row,
                    "column": column,
                    "value": value,
                    "level": level,
                    "rule": rule,
                    "message": message,
                }
            })
        );
    }
}

/// Use Valve, in conformity with the given command-line parameters, to update a row from the given
/// table. The details of the message are read from STDIN using JSON format. Note that
/// if the row number, `row`, is not given it must be included using the field name "row" or
/// "row_number" in the input JSON.
pub async fn update_row(cli: &Cli, table: &str, row: &Option<u32>) {
    let valve = build_valve(&cli).await;
    let mut json_row = match &cli.input {
        Some(s) if s == "JSON" => read_json_row_for_table(&valve, table),
        Some(s) => panic!("Unsupported input type '{s}'"),
        None => prompt_for_table_columns(
            &valve,
            table,
            row,
            match row {
                Some(_) => false,
                None => true,
            },
        ),
    };
    let input_rn = extract_and_remove_rn(&mut json_row);
    let rn = match input_rn {
        Some(input_rn) => match row {
            Some(rn) if *rn != input_rn => {
                panic!("Mismatch between input row and positional parameter, ROW")
            }
            None | Some(_) => input_rn,
        },
        None => match row {
            Some(row) => *row,
            None => panic!("No row given"),
        },
    };
    let output_row = valve
        .update_row(table, &rn, &json_row)
        .await
        .expect("Error updating row");

    if cli.verbose {
        println!(
            "{}",
            json!(output_row
                .to_rich_json()
                .expect("Error converting updated row to rich JSON"))
        );
    }
}

/// Use Valve, in conformity with the given command-line parameters, to update the given column
/// of the given row of the given table with the given input value.
pub async fn update_value(cli: &Cli, table: &str, row: u32, column: &str, value: &str) {
    let valve = build_valve(&cli).await;
    let json_row = fetch_row_with_input_value(&valve, table, row, column, value).await;
    let output_row = valve
        .update_row(table, &row, &json_row)
        .await
        .expect("Error updating row");

    if cli.verbose {
        println!(
            "{}",
            json!(output_row
                .to_rich_json()
                .expect("Error converting updated row to rich JSON"))
        );
    }
}

/// Use Valve, in conformity with the given command-line parameters, to validate a row from a given
/// table. If `value` is given, then fetch the row with the given row number from the database,
/// and validate it using `value` as the value of the column `column` in that row instead of its
/// current value. If `value` is not given, then read in the details of the row to validate (which
/// need not include a row number) from STDIN and validate it.
pub async fn validate(
    cli: &Cli,
    table: &str,
    row: &Option<u32>,
    column: &Option<String>,
    value: &Option<String>,
) {
    let valve = build_valve(&cli).await;
    let (rn, input_row) = match value {
        Some(value) => {
            let rn = row.expect("No row given");
            let input_row = fetch_row_with_input_value(
                &valve,
                table,
                rn,
                match column {
                    Some(column) => column,
                    None => panic!("No column given"),
                },
                value,
            )
            .await;
            (Some(rn), input_row)
        }
        None => match &cli.input {
            Some(s) if s == "JSON" => {
                let mut input_row = read_json_row_for_table(&valve, table);
                let rn = extract_and_remove_rn(&mut input_row);
                // If now row was input, default to `row` (which could still be None)
                let rn = match rn {
                    Some(rn) => {
                        if let Some(row) = row {
                            if *row != rn {
                                panic!("Mismatch between input row and positional parameter, ROW")
                            }
                        }
                        Some(rn)
                    }
                    None => *row,
                };
                (rn, input_row)
            }
            Some(s) => panic!("Unsupported input type: '{s}'"),
            None => {
                let question_end = match row {
                    Some(rn) => format!(" for row {rn}"),
                    None => "".to_string(),
                };

                // This is the JSON that we will be sending back:
                let mut json_row = JsonRow::new();

                // A closure to get the value of a given column from the user and to validate that
                // it's of the correct type:
                let prompt_for_column_value = |column: &str| -> SerdeValue {
                    let question = format!("Enter the '{column}'{question_end}");
                    let value: String =
                        prompt_default(question, String::new()).expect("Error getting user input");
                    // If the valus is numeric, parse it as such. Otherwise parse it as a string:
                    match value.as_str().parse::<u32>() {
                        Ok(nvalue) => json!(nvalue),
                        Err(_) => json!(value),
                    }
                };

                match column {
                    Some(column) => {
                        let value = prompt_for_column_value(column);
                        json_row.insert(column.to_string(), json!(value));
                    }
                    None => {
                        let columns = &valve
                            .config
                            .table
                            .get(table)
                            .expect("Table not found in config")
                            .column_order;
                        for column in columns {
                            let value = prompt_for_column_value(column);
                            json_row.insert(column.to_string(), json!(value));
                        }
                    }
                };
                (row.clone(), json_row)
            }
        },
    };
    // Validate the input row:
    let output_row = valve
        .validate_row(table, &input_row, rn)
        .await
        .expect("Error validating row");

    // Print the results to STDOUT:
    println!(
        "{}",
        json!(output_row
            .to_rich_json()
            .expect("Error converting validated row to rich JSON"))
    );

    // Set the exit status:
    let exit_code = output_row.contents.iter().all(|(_, vcell)| vcell.valid);
    std::process::exit(match exit_code {
        true => 0,
        false => 1,
    });
}

/// Prints the table dependencies in either incoming or outgoing order.
pub async fn print_dependencies(cli: &Cli, incoming: bool) {
    let valve = build_valve(&cli).await;
    let dependencies = valve
        .collect_dependencies(incoming)
        .expect("Could not collect dependencies");
    for (table, deps) in dependencies.iter() {
        let deps = {
            let deps = deps.iter().map(|s| format!("'{}'", s)).collect::<Vec<_>>();
            if deps.is_empty() {
                "None".to_string()
            } else {
                deps.join(", ")
            }
        };
        let preamble = {
            if incoming {
                format!("Tables that depend on '{}'", table)
            } else {
                format!("Table '{}' depends on", table)
            }
        };
        println!("{}: {}", preamble, deps);
    }
}

/// Given a Valve instance, a table name, a row number, a column name, and an input value,
/// fetches the row from the given table with the given row number, such that the value of the
/// given column is replaced with the given input_value.
pub async fn fetch_row_with_input_value(
    valve: &Valve,
    table: &str,
    row: u32,
    column: &str,
    input_value: &str,
) -> JsonRow {
    let mut row = valve
        .get_row_from_db(table, &row)
        .await
        .expect("Error getting row");
    let value = match serde_json::from_str::<SerdeValue>(&input_value) {
        Ok(value) => value,
        Err(_) => json!(input_value),
    };
    let cell = ValveCell {
        value: value,
        valid: true,
        ..Default::default()
    };
    *row.contents
        .get_mut(column)
        .expect(&format!("No column '{column}' in row")) = cell;
    row.contents_to_simple_json()
        .expect("Can't convert to simple JSON")
}

/// Read a JSON-formatted string from STDIN.
pub fn read_json_row_from_stdin() -> JsonRow {
    let mut json_row = String::new();
    io::stdin()
        .read_line(&mut json_row)
        .expect("Error reading from STDIN");
    let json_row = serde_json::from_str::<SerdeValue>(&json_row)
        .expect(&format!("Invalid JSON: {json_row}"))
        .as_object()
        .expect(&format!("{json_row} is not a JSON object"))
        .clone();
    json_row
}

/// Given a list of parameters, prompt the user for the values of each of them. If the parameter
/// list is empty, then repeatedly ask the user if she would like to define a new parameter, and
/// if so also ask for the value.
pub fn prompt_for_parameter_values(columns: &Vec<String>) -> JsonRow {
    // The row to be returned:
    let mut json_row = JsonRow::new();

    let prompt_for_column_name = || -> String {
        let column: String = prompt_default(
            "Enter the name of a new parameter, or press enter to stop adding parameters:",
            "".to_string(),
        )
        .expect("Error getting user input");
        column
    };
    let prompt_for_column_value = |column: &str| -> SerdeValue {
        let value: String = prompt_default(format!("Enter the '{column}':"), "".to_string())
            .expect("Error getting user input");
        json!(value)
    };

    // If the list of columns passed to this function is not empty, then prompt for each one.
    // Otherwise get new column names interactively until a newline is received.
    if !columns.is_empty() {
        for column in columns {
            json_row.insert(column.to_string(), prompt_for_column_value(column));
        }
    } else {
        let mut column = prompt_for_column_name();
        while column != "" {
            json_row.insert(column.to_string(), prompt_for_column_value(&column));
            column = prompt_for_column_name();
        }
    }
    json_row
}

/// Given a [Valve] instance, a table name, and, optionally, a row number, return a [JsonRow]
/// mapping each column of the table to an input value. If `include_rn` is set, then include
/// the `row_number` column in the map that is returned. If it is not given via `row_number` then
/// it must be given as an input value.
pub fn prompt_for_table_columns(
    valve: &Valve,
    table: &str,
    row_number: &Option<u32>,
    include_rn: bool,
) -> JsonRow {
    let table_columns = {
        let mut table_columns = match include_rn {
            true => vec!["row_number".to_string()],
            false => vec![],
        };
        table_columns.append(
            &mut valve
                .config
                .table
                .get(table)
                .expect("Table configuration not found")
                .column_order
                .clone(),
        );
        table_columns
    };
    let mut json_row = prompt_for_parameter_values(&table_columns);
    if include_rn {
        // The row number will be formatted as a string but we need to convert it to a number:
        json_row.insert("row_number".to_string(), {
            let rn = json_row.get("row_number").expect("No row_number_found");
            let rn = rn
                .as_str()
                .expect("Not a string")
                .parse::<u32>()
                .expect("Not a number");
            if let Some(row_number) = row_number {
                if rn != *row_number {
                    panic!("Mismatch between input value for row_number and row_number parameter")
                }
            }
            json!(rn)
        });
    }
    json_row
}

/// Given a [Valve] instance, a table name and a column name, return a [JsonRow] describing
/// its column configuration, which maps each column configuration parameter to an input value.
/// If either `table` or `column` are not given they must be provided as input values.
pub fn prompt_for_column_columns(
    valve: &Valve,
    table: &Option<String>,
    column: &Option<String>,
) -> JsonRow {
    let column_columns = &valve
        .config
        .table
        .get(&valve.config.special.column)
        .expect("No column table configuration found")
        .column_order
        .iter()
        .filter(|col| {
            // Only include the `table` and `column` columns if they are not already
            // present as arguments to this function:
            (*col != "table" || *table == None) && (*col != "column" || *column == None)
        })
        .cloned()
        .collect::<Vec<_>>();
    prompt_for_parameter_values(&column_columns)
}

/// Given a [Valve] instance and, optionally, a datatype name, return a [JsonRow] describing
/// its datatype configuration, which maps each datatype configuration parameter to an input
/// value. If `datatype` is not specified it must be provided as an input value.
pub fn prompt_for_datatype_columns(valve: &Valve, datatype: &Option<String>) -> JsonRow {
    let datatype_columns = &valve
        .config
        .table
        .get(&valve.config.special.datatype)
        .expect("No datatype table configuration found")
        .column_order
        .iter()
        .filter(|col| {
            // Only include the `datatype` column if it is not already present as an
            // argument to this function:
            *col != "datatype" || *datatype == None
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut dt_columns = prompt_for_parameter_values(&datatype_columns);
    if let Some(datatype) = datatype {
        dt_columns.insert("datatype".to_string(), json!(datatype));
    }
    dt_columns
}

/// Given a [Valve] instance and optionally: a table name, row number, and column name (any of
/// which, if not given, must be provided as an input value), return a [JsonRow] describing a
/// validation message, mapping each message parameter to an input value.
pub fn prompt_for_message_columns(
    valve: &Valve,
    table: &Option<String>,
    row: &Option<u32>,
    column: &Option<String>,
) -> JsonRow {
    let message_columns = &valve
        .config
        .table
        .get("message")
        .expect("No message table configuration found")
        .column_order
        .iter()
        .filter(|col| {
            // Only include the `table`, `row`, and `column` columns if they have not
            // already been provided as arguments to this function:
            (*col != "table" || *table == None)
                && (*col != "row" || *row == None)
                && (*col != "column" || *column == None)
        })
        .cloned()
        .collect::<Vec<_>>();
    prompt_for_parameter_values(&message_columns)
}

/// Given a [Valve] instance, a JSON representation of a row, and a table name, verifies that all
/// of the row columns exist in the given table's column configuration and panics otherwise.
pub fn verify_json_columns_for_table(valve: &Valve, json_row: &JsonRow, table: &str) {
    let json_columns = json_row.keys().collect::<Vec<_>>();
    let ignored_columns = {
        if table == "message" {
            vec!["message_id"]
        } else if table == "history" {
            vec!["history_id"]
        } else {
            vec!["row_number", "row_order"]
        }
    };
    let configured_columns = valve
        .config
        .table
        .get(table)
        .expect(&format!("No configuration found for '{table}'"))
        .column
        .keys()
        .collect::<Vec<_>>();
    for column in &json_columns {
        if !ignored_columns.contains(&column.as_str()) && !configured_columns.contains(column) {
            panic!("No column '{column}' in '{table}'");
        }
    }
}

/// Read a JSON-formatted string from STDIN and verify, using the given Valve instance, that
/// the fields in the JSON correspond to the allowed fields in the given table, before returning
/// the string as a [JsonRow].
pub fn read_json_row_for_table(valve: &Valve, table: &str) -> JsonRow {
    let json_row = read_json_row_from_stdin();

    // Verify that all of the columns specified in the input JSON exist in the table:
    verify_json_columns_for_table(valve, &json_row, table);

    // Return the JSON row:
    json_row
}

/// Try to extract the row number from the given [JsonRow]. For a successful lookup the input
/// JSON should contain either the field "row" or "row_number".
pub fn extract_rn(json_row: &JsonRow) -> Option<u32> {
    let extract_rn_from_value = |value: &SerdeValue| -> Option<u32> {
        let rn = match value.as_i64() {
            Some(rn) => Some(rn as u32),
            None => match value.as_str() {
                Some(rn_str) => {
                    let rn = rn_str.parse::<u32>().expect("Not a number");
                    Some(rn)
                }
                None => panic!("Not a number or a string: '{value}'"),
            },
        };
        rn
    };
    match json_row.get("row") {
        None => match json_row.get("row_number") {
            None => None,
            Some(value) => extract_rn_from_value(value),
        },
        Some(value) => extract_rn_from_value(value),
    }
}

/// Try to extract the row number from the given JSON row. It shold be specified using the field
/// name "row" or "row_number". Once extracted, remove it from the JSON row.
pub fn extract_and_remove_rn(json_row: &mut JsonRow) -> Option<u32> {
    let rn = extract_rn(json_row);
    json_row.remove("row_number");
    json_row.remove("row");
    rn
}

/// Given a JSON representation of a row, which is assumed to be from the column table, and
/// optionally a table name and a column name, extract the column table fields for that column
/// from the row and return them all as a JSON object. Note that if table_name or column_name
/// are not given then json_row must contain them (i.e., it must contain a "table" and a "column"
/// field, respectively).
pub fn extract_column_fields(
    json_row: &JsonRow,
    table_param: &Option<String>,
    column_param: &Option<String>,
) -> JsonRow {
    // Mandatory fields that may optionally be provided as arguments to this function:
    let table = match json_row.get("table") {
        Some(input_table) => match table_param {
            Some(table_param) if table_param != input_table => {
                panic!("Mismatch between input table and positional parameter, TABLE")
            }
            None | Some(_) => input_table.clone(),
        },
        None => match table_param {
            Some(table_param) => json!(table_param),
            None => panic!("No table given"),
        },
    };
    let column = match json_row.get("column") {
        Some(input_column) => match column_param {
            Some(column_param) if column_param != input_column => {
                panic!("Mismatch between input column and positional parameter, COLUMN")
            }
            None | Some(_) => input_column.clone(),
        },
        None => match column_param {
            Some(column_param) => json!(column_param),
            None => panic!("No column given"),
        },
    };

    // Mandatory field:
    let datatype = match json_row.get("datatype") {
        None => panic!("No datatype given"),
        Some(datatype) => datatype.clone(),
    };

    // Optional fields:
    let label = match json_row.get("label") {
        None => SerdeValue::Null,
        Some(label) => match label {
            SerdeValue::String(_) => label.clone(),
            _ => panic!("Field 'label' is not a string"),
        },
    };
    let nulltype = match json_row.get("nulltype") {
        None => SerdeValue::Null,
        Some(nulltype) => match nulltype {
            SerdeValue::String(_) => nulltype.clone(),
            _ => panic!("Field 'nulltype' is not a string"),
        },
    };
    let structure = match json_row.get("structure") {
        None => SerdeValue::Null,
        Some(structure) => match structure {
            SerdeValue::String(_) => structure.clone(),
            _ => panic!("Field 'structure' is not a string"),
        },
    };
    let description = match json_row.get("description") {
        None => SerdeValue::Null,
        Some(description) => match description {
            SerdeValue::String(_) => description.clone(),
            _ => panic!("Field 'description' is not a string"),
        },
    };

    let mut column_config = JsonRow::new();
    column_config.insert("table".to_string(), table);
    column_config.insert("column".to_string(), column);
    column_config.insert("datatype".to_string(), datatype);
    if label != SerdeValue::Null {
        column_config.insert("label".to_string(), label);
    }
    if nulltype != SerdeValue::Null {
        column_config.insert("nulltype".to_string(), nulltype);
    }
    if structure != SerdeValue::Null {
        column_config.insert("structure".to_string(), structure);
    }
    if description != SerdeValue::Null {
        column_config.insert("description".to_string(), description);
    }

    column_config
}

/// Reads and parses a JSON-formatted string representing a validation message (for the expected
/// format see documentation for [AddSubcommands::Message]), and returns the tuple:
/// (table, row, column, value, level, rule, message). If any of table_param, column_param, or
/// row_param are not provided, then a "table", "column", and/or "row" field, respectively should
/// be present in json_row.
pub fn extract_message_fields(
    table_param: &Option<String>,
    row_param: &Option<u32>,
    column_param: &Option<String>,
    json_row: &JsonRow,
) -> (String, u32, String, String, String, String, String) {
    let table = match json_row.get("table") {
        Some(input_table) => match table_param {
            Some(table_param) if table_param != input_table => {
                panic!("Mismatch between input table and positional parameter, TABLE")
            }
            None | Some(_) => input_table.as_str().expect("Not a string").to_string(),
        },
        None => match table_param {
            Some(table_param) => table_param.to_string(),
            None => panic!("No table given"),
        },
    };
    let row = match extract_rn(json_row) {
        Some(input_row) => match row_param {
            Some(row_param) if *row_param != input_row => {
                panic!("Mismatch between input row and positional parameter, ROW")
            }
            None | Some(_) => input_row,
        },
        None => match row_param {
            Some(row_param) => *row_param,
            None => panic!("No row given"),
        },
    };
    let column = match json_row.get("column") {
        Some(input_column) => match column_param {
            Some(column_param) if column_param != input_column => {
                panic!("Mismatch between input column and positional parameter, COLUMN")
            }
            None | Some(_) => input_column.as_str().expect("Not a string").to_string(),
        },
        None => match column_param {
            Some(column_param) => column_param.to_string(),
            None => panic!("No column given"),
        },
    };

    let value = match json_row.get("value") {
        Some(value) => value.as_str().expect("Not a string").to_string(),
        None => panic!("No value given"),
    };
    let level = match json_row.get("level") {
        Some(level) => level.as_str().expect("Not a string").to_string(),
        None => panic!("No level given"),
    };
    let rule = match json_row.get("rule") {
        Some(rule) => rule.as_str().expect("Not a string").to_string(),
        None => panic!("No rule given"),
    };
    let message = match json_row.get("message") {
        Some(message) => message.as_str().expect("Not a string").to_string(),
        None => panic!("No message given"),
    };

    (table, row, column, value, level, rule, message)
}

/// Given a JSON representation of a row, which is assumed to be from the datatype table, and
/// optionally a datatype name, extract the datatype table fields for that datatype
/// from the row and return them all as a JsonRow. Note that if datatype_name
/// is not given then the json_row argument must contain it, i.e., it must have a "datatype" field.
pub fn extract_datatype_fields(
    valve: &Valve,
    datatype_param: &Option<String>,
    json_row: &JsonRow,
) -> JsonRow {
    let mut dt_fields = JsonRow::new();
    // Add the datatype field to the map first:
    let datatype = match json_row.get("datatype") {
        Some(input_datatype) => {
            let input_datatype = input_datatype.as_str().expect("Not a string").to_string();
            match datatype_param {
                Some(datatype_param) if *datatype_param != input_datatype => {
                    panic!("Mismatch between input datatype and positional parameter, DATATYPE")
                }
                None | Some(_) => input_datatype,
            }
        }
        None => match datatype_param {
            Some(datatype_param) => datatype_param.to_string(),
            None => panic!("No 'datatype' given"),
        },
    };
    dt_fields.insert("datatype".to_string(), json!(datatype));

    // Now add fields corresponding to the values of every datatype column:
    let mut json_row = json_row.clone();
    json_row.remove("datatype");
    let configured_columns = valve
        .config
        .table
        .get(&valve.config.special.datatype)
        .expect(&format!("No configuration found for 'datatype'"))
        .column
        .keys()
        .filter(|col| *col != "datatype")
        .collect::<Vec<_>>();
    for column in configured_columns {
        let value = match json_row.get(column) {
            Some(value) => value.clone(),
            None => {
                if REQUIRED_DATATYPE_COLUMNS.contains(&column.as_str()) {
                    panic!("No '{column}' given");
                }
                json!("")
            }
        };
        dt_fields.insert(column.to_string(), value);
        json_row.remove(column);
    }
    // There should be no leftover columns after we've gone through all of the
    // configured columns:
    if !json_row.is_empty() {
        panic!("Extra columns found in input row: {}", json!(json_row));
    }

    dt_fields
}

/// Process Valve commands and command-line options. Note that this function will panic if it
/// encouters an error.
pub async fn process_command() {
    let cli = Cli::parse();
    // Although Valve::build() will accept a non-TSV argument (in which case that argument is
    // ignored and a table called 'table' is looked up in the given database instead), we do not
    // allow non-TSV arguments on the command line:
    if !cli.source.to_lowercase().ends_with(".tsv") {
        println!("SOURCE must be a file ending (case-insensitively) with .tsv");
        std::process::exit(1);
    }

    match &cli.command {
        Commands::Add { subcommand } => {
            match subcommand {
                AddSubcommands::Column {
                    table,
                    column,
                    no_load,
                } => add_column(&cli, table, column, *no_load).await,
                AddSubcommands::Datatype { datatype } => add_datatype(&cli, datatype).await,
                AddSubcommands::Message { table, row, column } => {
                    add_message(&cli, table, row, column).await
                }
                AddSubcommands::Row { table } => add_row(&cli, table).await,
                AddSubcommands::Table {
                    table,
                    path,
                    sample_size,
                    error_rate,
                    seed,
                    no_load,
                } => add_table(&cli, table, path, sample_size, error_rate, seed, *no_load).await,
            };
        }
        Commands::CreateAll {} => create_all(&cli).await,
        Commands::Delete { subcommand } => {
            match subcommand {
                DeleteSubcommands::Column {
                    table,
                    column,
                    no_load,
                } => delete_column(&cli, table, column, *no_load).await,
                DeleteSubcommands::Datatype { datatype } => delete_datatype(&cli, datatype).await,
                DeleteSubcommands::Messages { message_id, rule } => {
                    delete_messages_by_id_or_rule(&cli, message_id, rule).await
                }
                DeleteSubcommands::Row { table, rows } => delete_rows(&cli, table, rows).await,
                DeleteSubcommands::Table { table } => delete_table(&cli, table).await,
            };
        }
        Commands::DropAll {} => drop_all_tables(&cli).await,
        Commands::Drop { table } => drop_table(&cli, table).await,
        Commands::Get { subcommand } => {
            match subcommand {
                GetSubcommands::Ancestors { datatype } => print_ancestors(&cli, datatype).await,
                GetSubcommands::Cell { table, row, column } => {
                    print_cell(&cli, table, *row, column).await
                }
                GetSubcommands::ColumnConfig { table, column } => {
                    print_column_config(&cli, table, column).await
                }
                GetSubcommands::Constraints { table } => print_constraints(&cli, table).await,
                GetSubcommands::DatatypeConfig { datatype } => {
                    print_datatype_config(&cli, datatype).await
                }
                GetSubcommands::IncomingDeps {} => print_dependencies(&cli, true).await,
                GetSubcommands::Messages {
                    table,
                    row,
                    column,
                    rule,
                    message_id,
                } => print_messages(&cli, table, row, column, rule, message_id).await,
                GetSubcommands::OutgoingDeps {} => print_dependencies(&cli, false).await,
                GetSubcommands::Row { table, row } => print_row(&cli, table, *row).await,
                GetSubcommands::Rules { table, column } => print_rules(&cli, table, column).await,
                GetSubcommands::Schema {} => print_schema(&cli).await,
                GetSubcommands::Special { table } => print_special(&cli, table).await,
                GetSubcommands::Table { table } => print_table(&cli, table).await,
                GetSubcommands::TableConfig { table } => print_table_config(&cli, table).await,
                GetSubcommands::TableOrder {} => print_table_order(&cli).await,
                GetSubcommands::Value { table, row, column } => {
                    print_value(&cli, table, *row, column).await
                }
                GetSubcommands::ValveConfig {} => print_valve_config(&cli).await,
            };
        }
        Commands::History { context } => print_history(&cli, *context).await,
        Commands::LoadAll {} => load_all(&cli).await,
        Commands::Load {
            initial_load,
            table,
        } => load_table(&cli, table, *initial_load).await,
        Commands::Move { table, row, after } => move_row(&cli, table, *row, *after).await,
        Commands::Redo {} => redo(&cli).await,
        Commands::Undo {} => undo(&cli).await,
        Commands::Rename { subcommand } => {
            match subcommand {
                RenameSubcommands::Column {
                    table,
                    column,
                    new_name,
                    new_label,
                    no_load,
                } => rename_column(&cli, table, column, new_name, new_label, *no_load).await,
                RenameSubcommands::Datatype { datatype, new_name } => {
                    rename_datatype(&cli, datatype, new_name).await
                }
                RenameSubcommands::Table { table, new_name } => {
                    rename_table(&cli, table, new_name).await
                }
            };
        }
        Commands::Save { save_dir, tables } => {
            save(&cli, tables, save_dir).await;
        }
        Commands::SaveAs { table, path } => {
            save_as(&cli, table, path).await;
        }
        Commands::Test { subcommand } => {
            match subcommand {
                TestSubcommands::Api {} => {
                    test_api(&cli).await;
                }
                TestSubcommands::DtHierarchy {} => {
                    test_dt_hierarchy(&cli).await;
                }
            };
        }
        Commands::TruncateAll {} => {
            truncate_all_tables(&cli).await;
        }
        Commands::Truncate { table } => {
            truncate_table(&cli, table).await;
        }
        Commands::Update { subcommand } => {
            match subcommand {
                UpdateSubcommands::Message {
                    message_id,
                    table,
                    row,
                    column,
                } => {
                    update_message(&cli, *message_id, table, row, column).await;
                }
                UpdateSubcommands::Row { table, row } => {
                    update_row(&cli, table, row).await;
                }
                UpdateSubcommands::Value {
                    table,
                    row,
                    column,
                    value,
                } => {
                    update_value(&cli, table, *row, column, value).await;
                }
            };
        }
        Commands::Validate {
            table,
            row,
            column,
            value,
        } => {
            validate(&cli, table, row, column, value).await;
        }
    }
}
