//! API tests

use crate::{
    ast::Expression,
    validate::validate_cell_datatype,
    valve::{JsonRow, Valve, ValveCell, ValveDatatypeConfig, ValveError},
    PRINTF_RE,
};
use anyhow::Result;
use futures::executor::block_on;
use indoc::indoc;
use rand::{
    distributions::{Alphanumeric, DistString, Distribution, Uniform},
    random,
    rngs::StdRng,
    Rng, SeedableRng,
};
use rand_regex::Regex as RandRegex;
use regex::Regex;
use serde_json::{json, Value as SerdeValue};
use sprintf::sprintf;
use sqlx::{any::AnyPool, query as sqlx_query, Row, ValueRef};
use std::sync::Arc;

async fn test_matching(valve: &Valve) -> Result<()> {
    eprint!("Running test_matching() ... ");

    // Test the get_matching_values() function:
    let matching_values = valve.get_matching_values("table2", "child", None).await?;
    assert_eq!(
        matching_values,
        json!([
            {"id":"a","label":"a","order":1},
            {"id":"b","label":"b","order":2},
            {"id":"c","label":"c","order":3},
            {"id":"d","label":"d","order":4},
            {"id":"e","label":"e","order":5},
            {"id":"f","label":"f","order":6},
            {"id":"g","label":"g","order":7},
            {"id":"h","label":"h","order":8},
            {"id":"k","label":"k","order":9}

        ])
    );

    let matching_values = valve
        .get_matching_values("table6", "child", Some("7"))
        .await?;
    assert_eq!(
        matching_values,
        json!([
            {"id":"7","label":"7","order":1}
        ])
    );

    eprintln!("done.");
    Ok(())
}

async fn test_update_1(valve: &Valve) -> Result<()> {
    eprint!("Running test_update_1() ... ");

    let row = json!({
        "child": "b",
        "parent": "f",
        "xyzzy": "w",
        "foo": 1,
        "bar": "B",
    });

    valve
        .update_row("table2", &1, &row.as_object().unwrap())
        .await?;

    eprintln!("done.");
    Ok(())
}

async fn test_insert_1(valve: &Valve) -> Result<()> {
    eprint!("Running test_insert_1() ... ");

    let row = json!({
        "id": "BFO:0000027",
        "label": "bazaar",
        "parent": "barrie",
        "source": "BFOBBER",
        "type": "owl:Class",
    });

    let (_new_row_num, _new_row) = valve.insert_row("table3", row.as_object().unwrap()).await?;

    eprintln!("done.");
    Ok(())
}

async fn test_update_2(valve: &Valve) -> Result<()> {
    eprint!("Running test_update_2() ... ");

    let row = json!({
        "child": 2,
        "parent": 6,
        "xyzzy": 23,
        "foo": 'a',
        "bar": 2,
    });

    valve
        .update_row("table6", &1, row.as_object().unwrap())
        .await?;

    eprintln!("done.");
    Ok(())
}

async fn test_insert_2(valve: &Valve) -> Result<()> {
    eprint!("Running test_insert_2() ... ");

    let row = json!({
        "child": 2,
        "parent": 6,
        "xyzzy": 23,
        "foo": 'a',
        "bar": 2,
    });

    let (_new_row_num, _new_row) = valve.insert_row("table6", row.as_object().unwrap()).await?;

    eprintln!("done.");
    Ok(())
}

async fn test_insert_3(valve: &Valve) -> Result<()> {
    eprint!("Running test_insert_3() ... ");

    let row = json!({
        "id": "BFO:0000099",
        "label": "jafar",
        "parent": "mar",
        "source": "COB",
        "type": "owl:Class",
    });
    let (_new_row_num, _new_row) = valve.insert_row("table3", row.as_object().unwrap()).await?;

    // The result of this insertion is that the tree:foreign error message will be resolved
    // for table3 row 5 column parent: "Value 'jafar' of column parent is not in column label"

    eprintln!("done.");
    Ok(())
}

async fn test_dependencies(valve: &Valve) -> Result<()> {
    eprint!("Running test_dependencies() ... ");

    // Test cases for updates/inserts/deletes with dependencies.
    let row = json!({
        "foreign_column": "w",
        "other_foreign_column": "z",
        "numeric_foreign_column": "",
    });

    valve
        .update_row("table10", &1, &row.as_object().unwrap())
        .await?;

    let row = json!({
        "child": "b",
        "parent": "c",
        "xyzzy": "d",
        "foo": "d",
        "bar": "f",
    });

    valve
        .update_row("table11", &2, &row.as_object().unwrap())
        .await?;

    valve.delete_row("table11", &4).await?;

    let row = json!({
        "foreign_column": "i",
        "other_foreign_column": "i",
        "numeric_foreign_column": "9",
    });

    let (_new_row_num, _new_row) = valve
        .insert_row("table10", &row.as_object().unwrap())
        .await?;

    eprintln!("done.");
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DbOperation {
    Insert,
    Delete,
    Update,
    Move,
    Undo,
    Redo,
}

async fn generate_operation_sequence(pool: &AnyPool) -> Result<Vec<DbOperation>> {
    /*
    Algorithm:
    ----------
    1. Determine the number of "modify" operations to randomly generate. Then for each operation do
       the following:
    2. Generate a modify/undo pair
    3. Do the modify
    4. Either add an undo immediately after the given modify, or defer the undo by adding it to an
       undo stack.
    5. Possibly generate a redo/undo pair such that the undo comes immediately after the undo, or
       is deferred to the undo stack.
    6. Once all of the modify operations have been processed, go through the undo stack:
       a. For each undo, once it's been processed, possibly generate a redo/undo pair such that the
          undo comes immediately after the undo, or is deferred to the undo stack.

    After this function returns, the database should be in the same logical state as it was before.
     */

    let list_len = {
        let between = Uniform::from(25..51);
        let mut rng = rand::thread_rng();
        between.sample(&mut rng)
    };

    let mut operations = vec![];
    let mut undo_stack = vec![];
    for _ in 0..list_len {
        let between = Uniform::from(0..4);
        let mut rng = rand::thread_rng();
        match between.sample(&mut rng) {
            0 => operations.push(DbOperation::Insert),
            1 => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let rows = query.fetch_all(pool).await?;
                if rows.len() != 0 {
                    operations.push(DbOperation::Delete)
                } else {
                    operations.push(DbOperation::Insert)
                }
            }
            2 => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let rows = query.fetch_all(pool).await?;
                if rows.len() != 0 {
                    operations.push(DbOperation::Update)
                } else {
                    operations.push(DbOperation::Insert)
                }
            }
            3 => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let rows = query.fetch_all(pool).await?;
                if rows.len() != 0 {
                    operations.push(DbOperation::Move)
                } else {
                    operations.push(DbOperation::Insert)
                }
            }
            _ => unreachable!(),
        };
        // Randomly either add an undo immediately after the modify, or add it to the undo_stack:
        if random::<bool>() == true {
            operations.push(DbOperation::Undo);
            // Randomly add a redo as well:
            if random::<bool>() == true {
                operations.push(DbOperation::Redo);
                // Randomly either add an undo either immediately after the redo, or to the
                // undo_stack:
                if random::<bool>() == true {
                    operations.push(DbOperation::Undo);
                } else {
                    undo_stack.push(DbOperation::Undo);
                }
            }
        } else {
            undo_stack.push(DbOperation::Undo);
        }
    }

    // Go through the items in the undo stack:
    let mut further_operations = vec![];
    let mut further_undo_stack = vec![];
    while let Some(_) = undo_stack.pop() {
        // Add the undo to the list of further operations to perform:
        further_operations.push(DbOperation::Undo);
        // Randomly add a redo as well:
        if random::<bool>() == true {
            further_operations.push(DbOperation::Redo);
            // Randomly add an undo either immediately after the redo, or to a further
            // stack of undos to be performed at the end:
            if random::<bool>() == true {
                further_operations.push(DbOperation::Undo);
            } else {
                further_undo_stack.push(DbOperation::Undo);
            }
        }
    }

    operations.append(&mut further_operations);
    // Since further_undo_stack is a stack, we need to reverse it:
    further_undo_stack.reverse();
    operations.append(&mut further_undo_stack);
    Ok(operations)
}

async fn test_randomized_api_test_with_undo_redo(valve: &mut Valve) -> Result<()> {
    // Randomly generate a number of insert/update/delete operations, possibly followed by undos
    // and/or redos.
    eprint!("Running test_randomized_api_test_with_undo_redo() ... ");

    fn generate_value() -> String {
        let mut value = Alphanumeric.sample_string(&mut rand::thread_rng(), 10);
        while random::<bool>() && random::<bool>() {
            value.push_str(" ");
            value.push_str(&Alphanumeric.sample_string(&mut rand::thread_rng(), 10));
        }
        if random::<bool>() && random::<bool>() {
            value.push_str(" ");
        }
        value
    }

    fn generate_row() -> JsonRow {
        let mut row = JsonRow::new();
        row.insert("prefix".to_string(), json!(generate_value()));
        row.insert("base".to_string(), json!(generate_value()));
        row.insert("ontology IRI".to_string(), json!(generate_value()));
        row.insert("version IRI".to_string(), json!(generate_value()));
        row
    }

    let operations_list = generate_operation_sequence(&valve.pool).await?;
    for operation in operations_list {
        match operation {
            DbOperation::Delete => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let sql_row = query.fetch_one(&valve.pool).await?;
                let raw_row_number = sql_row.try_get_raw("row_number")?;
                if raw_row_number.is_null() {
                    return Err(ValveError::DataError("No rows in table1_view".into()).into());
                } else {
                    let row_number: i64 = sql_row.get("row_number");
                    let row_number = row_number as u32;
                    valve.delete_row("table1", &row_number).await?;
                }
            }
            DbOperation::Update => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let sql_row = query.fetch_one(&valve.pool).await?;
                let raw_row_number = sql_row.try_get_raw("row_number")?;
                if raw_row_number.is_null() {
                    return Err(ValveError::DataError("No rows in table1_view".into()).into());
                } else {
                    let row_number: i64 = sql_row.get("row_number");
                    let row_number = row_number as u32;
                    let row = generate_row();
                    valve.update_row("table1", &row_number, &row).await?;
                }
            }
            DbOperation::Insert => {
                let row = generate_row();
                let (_rn, _r) = valve.insert_row("table1", &row).await?;
            }
            DbOperation::Move => {
                let query = sqlx_query("SELECT COUNT(1) AS num_rows FROM table1_view");
                let sql_row = query.fetch_one(&valve.pool).await?;
                let num_rows: i64 = sql_row.get("num_rows");
                let num_rows = num_rows as u32;
                if num_rows == 0 {
                    return Err(ValveError::DataError("No rows in table1_view".into()).into());
                }

                async fn row_exists(valve: &Valve, row: u32) -> bool {
                    let sql = format!(
                        "SELECT COUNT(1) AS num_rows FROM table1_view WHERE row_number = {}",
                        row
                    );
                    let query = sqlx_query(&sql);
                    let sql_row = query.fetch_one(&valve.pool).await.unwrap();
                    let num_rows: i64 = sql_row.get("num_rows");
                    let num_rows = num_rows as u32;
                    num_rows > 0
                }

                // Randomly generate a row number to move and find the previous_row for it (after
                // making sure that the randomly generated row number exists in the db):
                let mut row = rand::thread_rng().gen_range(1..num_rows + 1);
                while !row_exists(valve, row).await {
                    row = rand::thread_rng().gen_range(1..num_rows + 1);
                }
                let old_previous_row = valve.get_previous_row("table1", &row).await?;

                // Randomly generate a new_previous_row, so that we can try to move `row` to the
                // position immediately after it. Note that we also make sure that
                // `new_previous_row` exists and is distinct both from `row` and `old_previous_row`:
                let mut new_previous_row = rand::thread_rng().gen_range(1..num_rows + 1);
                while !row_exists(valve, new_previous_row).await
                    || new_previous_row == old_previous_row
                    || new_previous_row == row
                {
                    new_previous_row = rand::thread_rng().gen_range(1..num_rows + 1);
                }
                valve.move_row("table1", &row, &new_previous_row).await?;
            }
            DbOperation::Undo => {
                valve.undo().await?;
            }
            DbOperation::Redo => {
                valve.redo().await?;
            }
        };
    }

    eprintln!("done.");
    Ok(())
}

async fn test_undo_redo(valve: &mut Valve) -> Result<()> {
    eprint!("Running test_undo_redo() ... ");

    // Undo/redo tests
    let row_1 = json!({
        "foreign_column": "j",
        "other_foreign_column": "j",
        "numeric_foreign_column": "10",
    });
    let row_2 = json!({
        "foreign_column": "k",
        "other_foreign_column": "k",
        "numeric_foreign_column": "11",
    });

    // Undo/redo test 1:
    let (_rn, _r) = valve
        .insert_row("table10", &row_1.as_object().unwrap())
        .await?;

    valve.undo().await?;

    valve.redo().await?;

    valve.undo().await?;

    // Undo/redo test 2:
    valve
        .update_row("table10", &8, &row_2.as_object().unwrap())
        .await?;

    valve.undo().await?;

    valve.redo().await?;

    valve.undo().await?;

    // Undo/redo test 3:
    valve.delete_row("table10", &8).await?;

    valve.undo().await?;

    valve.redo().await?;

    valve.undo().await?;

    // Undo/redo test 4:
    let (rn, _row) = valve
        .insert_row("table10", &row_1.as_object().unwrap())
        .await?;

    valve
        .update_row("table10", &rn, &row_2.as_object().unwrap())
        .await?;

    // Undo update:
    valve.undo().await?;

    // Redo update:
    valve.redo().await?;

    valve.delete_row("table10", &rn).await?;

    // Undo delete:
    valve.undo().await?;

    // Undo update:
    valve.undo().await?;

    // Undo insert:
    valve.undo().await?;

    // Undo/redo test 5:
    valve.move_row("table10", &1, &5).await?;

    valve.undo().await?;

    valve.redo().await?;

    valve.move_row("table10", &3, &1).await?;

    valve.undo().await?;

    eprintln!("done.");
    Ok(())
}

async fn test_modes(valve: &Valve) -> Result<()> {
    eprint!("Running test_modes() ... ");
    let readonly_row = json!({
        "study_name": "FAKE123",
        "sample_number": 100,
        "species": "Adelie Penguin (Pygoscelis adeliae)",
        "region": "Anvers",
        "island": "Briscoe",
        "stage": "Adult, 1 Egg Stage",
        "individual_id": "ZH64",
        "clutch_completion": "Yes",
        "date_egg": "2024-01-01",
        "culmen_length": 38.7,
        "culmen_depth": 20.4,
        "flipper_length": 215,
        "body_mass": 2401,
        "sex": "FEMALE",
        "delta_15_n": 8.233,
        "delta_13_c": -23.2,
        "comments": "",
    });
    let readonly_row = readonly_row.as_object().unwrap();
    let expected_vrow = indoc! {r#"ValveRow {
                                       row_number: None,
                                       contents: {
                                           "body_mass": ValveCell {
                                               nulltype: None,
                                               value: Number(2401),
                                               valid: true,
                                               messages: [],
                                           },
                                           "clutch_completion": ValveCell {
                                               nulltype: None,
                                               value: String("Yes"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "comments": ValveCell {
                                               nulltype: Some(
                                                   "empty",
                                               ),
                                               value: String(""),
                                               valid: true,
                                               messages: [],
                                           },
                                           "culmen_depth": ValveCell {
                                               nulltype: None,
                                               value: Number(20.4),
                                               valid: true,
                                               messages: [],
                                           },
                                           "culmen_length": ValveCell {
                                               nulltype: None,
                                               value: Number(38.7),
                                               valid: true,
                                               messages: [],
                                           },
                                           "date_egg": ValveCell {
                                               nulltype: None,
                                               value: String("2024-01-01"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "delta_13_c": ValveCell {
                                               nulltype: None,
                                               value: Number(-23.2),
                                               valid: true,
                                               messages: [],
                                           },
                                           "delta_15_n": ValveCell {
                                               nulltype: None,
                                               value: Number(8.233),
                                               valid: true,
                                               messages: [],
                                           },
                                           "flipper_length": ValveCell {
                                               nulltype: None,
                                               value: Number(215),
                                               valid: true,
                                               messages: [],
                                           },
                                           "individual_id": ValveCell {
                                               nulltype: None,
                                               value: String("ZH64"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "island": ValveCell {
                                               nulltype: None,
                                               value: String("Briscoe"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "region": ValveCell {
                                               nulltype: None,
                                               value: String("Anvers"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "sample_number": ValveCell {
                                               nulltype: None,
                                               value: Number(100),
                                               valid: true,
                                               messages: [],
                                           },
                                           "sex": ValveCell {
                                               nulltype: None,
                                               value: String("FEMALE"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "species": ValveCell {
                                               nulltype: None,
                                               value: String("Adelie Penguin (Pygoscelis adeliae)"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "stage": ValveCell {
                                               nulltype: None,
                                               value: String("Adult, 1 Egg Stage"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "study_name": ValveCell {
                                               nulltype: None,
                                               value: String("FAKE123"),
                                               valid: true,
                                               messages: [],
                                           },
                                       },
                                   }"#};
    let vrow = valve.validate_row("readonly1", &readonly_row, None).await?;
    assert_eq!(format!("{:#?}", vrow), expected_vrow);

    let view_row = json!({
        "foo": "a",
        "bar": "b",
    });
    let view_row = view_row.as_object().unwrap();
    let expected_vrow = indoc! {r#"ValveRow {
                                       row_number: None,
                                       contents: {
                                           "bar": ValveCell {
                                               nulltype: None,
                                               value: String("b"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "foo": ValveCell {
                                               nulltype: None,
                                               value: String("a"),
                                               valid: true,
                                               messages: [],
                                           },
                                       },
                                   }"#};
    let vrow = valve.validate_row("view1", &view_row, None).await?;
    assert_eq!(format!("{:#?}", vrow), expected_vrow);

    let result = valve.insert_row("readonly1", &readonly_row).await;
    match result {
        Err(e) => assert!(format!("{:?}", e)
            .starts_with(r#"InputError("Inserting to table 'readonly1' is not allowed")"#)),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    let result = valve.insert_row("view1", &view_row).await;
    match result {
        Err(e) => assert!(format!("{:?}", e)
            .starts_with(r#"InputError("Inserting to table 'view1' is not allowed")"#)),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    let result = valve.update_row("readonly1", &1, &readonly_row).await;
    match result {
        Err(e) => assert!(format!("{:?}", e)
            .starts_with(r#"InputError("Updating table 'readonly1' is not allowed")"#)),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    let result = valve.update_row("view1", &1, &view_row).await;
    match result {
        Err(e) => assert!(format!("{:?}", e)
            .starts_with(r#"InputError("Updating table 'view1' is not allowed")"#)),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    let result = valve.delete_row("readonly1", &1).await;
    match result {
        Err(e) => assert!(format!("{:?}", e)
            .starts_with(r#"InputError("Deleting from table 'readonly1' is not allowed")"#)),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    let result = valve.delete_row("view1", &1).await;
    match result {
        Err(e) => assert!(format!("{:?}", e)
            .starts_with(r#"InputError("Deleting from table 'view1' is not allowed")"#)),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    eprintln!("done.");
    Ok(())
}

async fn test_default(valve: &Valve) -> Result<()> {
    eprint!("Running test_default() ... ");

    // Default value of text type:
    let row = json!({
        "prefix": "g",
        "ontology_IRI": "foo",
        "version_IRI": "bar",
    });
    let (_, new_row) = valve.insert_row("table8", row.as_object().unwrap()).await?;
    let expected = indoc! {r#"ValveRow {
                                  row_number: Some(
                                      3,
                                  ),
                                  contents: {
                                      "ontology_IRI": ValveCell {
                                          nulltype: None,
                                          value: String("foo"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "prefix": ValveCell {
                                          nulltype: None,
                                          value: String("g"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "version_IRI": ValveCell {
                                          nulltype: None,
                                          value: String("bar"),
                                          valid: true,
                                          messages: [],
                                      },
                                  },
                              }"#};
    assert_eq!(format!("{:#?}", new_row), expected);
    let query = sqlx_query(
        r#"SELECT * FROM "table8"
            WHERE "prefix" = 'g'
              AND "ontology_IRI" = 'foo'
              AND "version_IRI" = 'bar'"#,
    );
    let rows = query.fetch_all(&valve.pool).await?;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    let base = row.get::<&str, &str>("base");
    assert_eq!("b", base);

    // Default value of numeric type:
    let row = json!({
        "child": "f",
        "parent": "d",
        "xyzzy": "x",
        "bar": "w",
    });
    let (_, new_row) = valve.insert_row("table9", row.as_object().unwrap()).await?;
    let expected = indoc! {r#"ValveRow {
                                  row_number: Some(
                                      10,
                                  ),
                                  contents: {
                                      "bar": ValveCell {
                                          nulltype: None,
                                          value: String("w"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "child": ValveCell {
                                          nulltype: None,
                                          value: String("f"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "parent": ValveCell {
                                          nulltype: None,
                                          value: String("d"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "xyzzy": ValveCell {
                                          nulltype: None,
                                          value: String("x"),
                                          valid: true,
                                          messages: [],
                                      },
                                  },
                              }"#};
    assert_eq!(format!("{:#?}", new_row), expected);
    let query = sqlx_query(
        r#"SELECT * FROM "table9"
            WHERE "bar" = 'w'
              AND "child" = 'f'
              AND "parent" = 'd'
              AND "xyzzy" = 'x'"#,
    );
    let rows = query.fetch_all(&valve.pool).await?;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    let foo: i32 = row.get("foo");
    assert_eq!(1, foo);

    // Override default value:
    let row = json!({
        "child": "g",
        "parent": "e",
        "xyzzy": "y",
        "bar": "x",
        "foo": 2,
    });
    let (_, new_row) = valve.insert_row("table9", row.as_object().unwrap()).await?;
    let expected = indoc! {r#"ValveRow {
                                  row_number: Some(
                                      11,
                                  ),
                                  contents: {
                                      "bar": ValveCell {
                                          nulltype: None,
                                          value: String("x"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "child": ValveCell {
                                          nulltype: None,
                                          value: String("g"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "foo": ValveCell {
                                          nulltype: None,
                                          value: Number(2),
                                          valid: true,
                                          messages: [],
                                      },
                                      "parent": ValveCell {
                                          nulltype: None,
                                          value: String("e"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "xyzzy": ValveCell {
                                          nulltype: None,
                                          value: String("y"),
                                          valid: true,
                                          messages: [],
                                      },
                                  },
                              }"#};
    assert_eq!(format!("{:#?}", new_row), expected);
    let query = sqlx_query(
        r#"SELECT * FROM "table9"
            WHERE "bar" = 'x'
              AND "child" = 'g'
              AND "parent" = 'e'
              AND "xyzzy" = 'y'"#,
    );
    let rows = query.fetch_all(&valve.pool).await?;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    let foo: i32 = row.get("foo");
    assert_eq!(2, foo);
    eprintln!("done.");
    Ok(())
}

async fn test_move(valve: &mut Valve) -> Result<()> {
    eprint!("Running test_move() ... ");

    async fn get_rows_in_order(valve: &Valve) -> Result<Vec<u32>> {
        let query = sqlx_query("SELECT row_number FROM table1_view ORDER BY row_order");
        let rows = query.fetch_all(&valve.pool).await?;
        let rows: Vec<i64> = rows.iter().map(|r| r.get("row_number")).collect::<Vec<_>>();
        let rows = rows.iter().map(|n| *n as u32).collect::<Vec<_>>();
        Ok(rows)
    }

    // Move a bunch of rows:
    valve.move_row("table1", &5, &2).await?;
    assert_eq!(
        vec![1, 2, 5, 3, 4, 6, 7, 8, 9, 10, 11, 12],
        get_rows_in_order(valve).await?
    );
    valve.move_row("table1", &7, &5).await?;
    assert_eq!(
        vec![1, 2, 5, 7, 3, 4, 6, 8, 9, 10, 11, 12],
        get_rows_in_order(valve).await?
    );
    valve.move_row("table1", &9, &5).await?;
    assert_eq!(
        vec![1, 2, 5, 9, 7, 3, 4, 6, 8, 10, 11, 12],
        get_rows_in_order(valve).await?
    );
    valve.move_row("table1", &5, &12).await?;
    assert_eq!(
        vec![1, 2, 9, 7, 3, 4, 6, 8, 10, 11, 12, 5],
        get_rows_in_order(valve).await?
    );
    valve.move_row("table1", &2, &0).await?;
    assert_eq!(
        vec![2, 1, 9, 7, 3, 4, 6, 8, 10, 11, 12, 5],
        get_rows_in_order(valve).await?
    );

    // Delete a row and then undo the delete, then check to see if it has been
    // placed back into the right order.
    valve.delete_row("table1", &8).await?;
    assert_eq!(
        vec![2, 1, 9, 7, 3, 4, 6, 10, 11, 12, 5],
        get_rows_in_order(valve).await?
    );
    valve.undo().await?;
    assert_eq!(
        vec![2, 1, 9, 7, 3, 4, 6, 8, 10, 11, 12, 5],
        get_rows_in_order(valve).await?
    );

    // Update a row and verify that its previous_row has not changed as a result:
    valve.move_row("table2", &3, &4).await?;
    let previous_row_before = valve.get_previous_row("table2", &3).await?;
    assert_eq!(previous_row_before, 4);
    let row = json!({
        "child": "b",
        "parent": "f",
        "xyzzy": "w",
        "foo": 1,
        "bar": "B",
    });
    valve
        .update_row("table2", &3, &row.as_object().unwrap())
        .await?;
    let previous_row_after = valve.get_previous_row("table2", &3).await?;
    assert_eq!(previous_row_before, previous_row_after);
    valve.undo().await?;
    valve.undo().await?;

    // Undo the moves:
    valve.undo().await?;
    assert_eq!(
        vec![1, 2, 9, 7, 3, 4, 6, 8, 10, 11, 12, 5],
        get_rows_in_order(valve).await?
    );
    valve.undo().await?;
    assert_eq!(
        vec![1, 2, 5, 9, 7, 3, 4, 6, 8, 10, 11, 12],
        get_rows_in_order(valve).await?
    );
    valve.undo().await?;
    assert_eq!(
        vec![1, 2, 5, 7, 3, 4, 6, 8, 9, 10, 11, 12],
        get_rows_in_order(valve).await?
    );
    valve.undo().await?;
    assert_eq!(
        vec![1, 2, 5, 3, 4, 6, 7, 8, 9, 10, 11, 12],
        get_rows_in_order(valve).await?
    );
    valve.undo().await?;
    assert_eq!(
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        get_rows_in_order(valve).await?
    );

    eprintln!("done.");
    Ok(())
}

pub async fn run_api_tests(valve: &mut Valve) -> Result<()> {
    // NOTE that you must use an external script to fetch the data from the database and run a diff
    // against a known good sample to verify that these tests yield the expected results:
    test_matching(&valve).await?;
    test_update_1(&valve).await?;
    test_insert_1(&valve).await?;
    test_update_2(&valve).await?;
    test_insert_2(&valve).await?;
    test_insert_3(&valve).await?;
    test_dependencies(&valve).await?;
    test_undo_redo(valve).await?;
    test_randomized_api_test_with_undo_redo(valve).await?;
    test_modes(&valve).await?;
    test_default(&valve).await?;
    test_move(valve).await?;

    // When the first argument to Valve::build() is not a string ending in .tsv, the table table
    // should be read from the database string (given by the second argument) instead, i.e., valve
    // will look in the given database and read the configuration from the "table" db table. Here
    // we just make sure that this is possible. If there is a problem an error will be returned.
    let _valve = Valve::build("ignored", &valve.db_path).await?;

    eprintln!("All done.");
    Ok(())
}

pub fn run_dt_hierarchy_tests(valve: &Valve) -> Result<()> {
    // Looks in the valve configuration for any column with the same datatype as the given
    // datatype, and returns the column name and the name of the table to which it belongs:
    fn get_any_column_with_datatype(valve: &Valve, datatype: &str) -> Option<(String, String)> {
        let mut found = false;
        let mut table_name = String::from("");
        let mut column_name = String::from("");
        for (table, table_config) in valve.config.table.iter() {
            table_name = table.to_string();
            for (column, column_config) in table_config.column.iter() {
                column_name = column.to_string();
                if column_config.datatype == datatype {
                    found = true;
                    break;
                }
            }
            if found {
                break;
            }
        }
        if found {
            Some((table_name, column_name))
        } else {
            None
        }
    }

    // Verifies that the format string (if any) for the given datatype does not violate
    // its datatype condition:
    fn check_dt_format(valve: &Valve, datatype: &str) {
        let colformat = block_on(valve.get_datatype_format(datatype)).unwrap();
        // If there is no format for this datatype then there is nothing to check:
        if colformat == "" {
            return;
        }

        let format_regex = Regex::new(PRINTF_RE).unwrap();
        let conversion_spec = match format_regex.captures(&colformat) {
            Some(c) => c[1].to_lowercase(),
            None => {
                log::error!("Illegal format: '{}'", colformat);
                return;
            }
        };
        // Randomly generate 1000 strings in accordance with colformat and test them each with
        // dt_condition, recording any failures:
        let dt_condition = valve.datatype_conditions.get(datatype).unwrap();
        let mut failures = vec![];
        for _ in 0..1000 {
            let value_to_check = match conversion_spec.as_str() {
                "d" | "i" | "c" => sprintf!(&colformat, random::<isize>()).unwrap(),
                "o" | "u" | "x" => sprintf!(&colformat, random::<usize>()).unwrap(),
                "e" | "f" | "g" | "a" => sprintf!(&colformat, random::<f64>()).unwrap(),
                "s" => sprintf!(&colformat, {
                    let mut rng = rand::thread_rng();
                    let chars: String = (0..20)
                        .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
                        .collect();
                    chars
                })
                .unwrap(),
                _ => {
                    log::error!(
                        "Unsupported conversion specifier '{}' in column format '{}'",
                        conversion_spec,
                        colformat
                    );
                    return;
                }
            };
            if !dt_condition.compiled.clone()(&value_to_check) {
                failures.push(value_to_check.to_string());
            }
        }

        if failures.len() == 0 {
            return;
        }

        // Now we take the shortest failing string:
        failures.sort_by(|a, b| a.len().cmp(&b.len()));
        let shortest_failure = &failures[0];

        // If there is a configured column corresponding to the datatype, use shortest_failure to
        // construct a ValveCell which we then run through the validation engine to generate
        // validation messages for the value. Otherwise just report the failure.
        let error_msg = format!(
            "Value '{}' generated using format '{}' for datatype '{}' fails validation.",
            shortest_failure, colformat, datatype
        );

        match get_any_column_with_datatype(valve, datatype) {
            None => log::error!("{}", error_msg),
            Some((table_name, column_name)) => {
                let mut cell = ValveCell {
                    nulltype: None,
                    value: SerdeValue::String(shortest_failure.to_string()),
                    valid: true,
                    messages: vec![],
                };
                // Send the cell through the validator:
                validate_cell_datatype(
                    &valve.config,
                    &valve.datatype_conditions,
                    &table_name,
                    &column_name,
                    &mut cell,
                );
                // Add the generated validation messages to the error log:
                let mut vmessages = String::from("");
                for msg in cell.messages {
                    vmessages.push_str(&format!(" {} ({} {});", msg.message, msg.rule, msg.level));
                }
                log::error!(
                    "{} Adding this value to, for instance, the column '{}' of the table '{}' \
                     would result in the following validation messages:{}",
                    error_msg,
                    column_name,
                    table_name,
                    vmessages
                );
            }
        };
    }

    // Verifies, for the given datatype, that any string which satisfies its condition also
    // satisfies the condition of its parent datatype (if any):
    fn check_dt_parent(valve: &Valve, dt_config: &ValveDatatypeConfig) {
        // First find the datatype condition info (it's type, the regex string used, and
        // the compiled version of the condition) for the child datatype:
        let dt_condition = valve.datatype_conditions.get(&dt_config.datatype).unwrap();
        let unquoted_re = Regex::new(r#"^['"](?P<unquoted>.*)['"]$"#).unwrap();
        let (ctype, cregex, ccond) = match &dt_condition.parsed {
            Expression::Function(name, args) => {
                if name == "equals" {
                    if let Expression::Label(label) = &*args[0] {
                        (
                            name,
                            String::from(unquoted_re.replace(&label, "$unquoted")),
                            dt_condition.compiled.clone(),
                        )
                    } else {
                        panic!("ERROR: Invalid condition: {}", dt_config.condition);
                    }
                } else if vec!["exclude", "match", "search"].contains(&name.as_str()) {
                    if let Expression::RegexMatch(pattern, flags) = &*args[0] {
                        let pattern = String::from(unquoted_re.replace(pattern, "$unquoted"));
                        // Anchors are not supported by the rand_regex crate:
                        if pattern.starts_with("^") || pattern.ends_with("$") {
                            log::warn!("Not testing unsupported pattern with anchors: {}", pattern);
                            return;
                        }
                        let mut flags = String::from(flags);
                        if flags != "" {
                            flags = format!("(?{})", flags.as_str());
                        }

                        (
                            name,
                            format!("{}{}", flags, pattern),
                            dt_condition.compiled.clone(),
                        )
                    } else {
                        panic!(
                            "Argument to condition: {} is not a regular expression",
                            dt_config.condition
                        );
                    }
                } else if name == "in" {
                    let mut alternatives: Vec<String> = vec![];
                    for arg in args {
                        if let Expression::Label(value) = &**arg {
                            let value = unquoted_re.replace(&value, "$unquoted");
                            alternatives.push(value.to_string());
                        } else {
                            panic!("Argument: {:?} to function 'in' is not a label", arg);
                        }
                    }

                    (
                        name,
                        format!("({})", alternatives.join("|")),
                        dt_condition.compiled.clone(),
                    )
                } else {
                    panic!("Unrecognized function name: {}", name);
                }
            }
            _ => {
                panic!("Unrecognized condition: {}", dt_config.condition);
            }
        };

        // Next, get the type and compiled condition corresponding to the datatype of the child's
        // parent:
        let (ptype, pcond) = match valve.datatype_conditions.get(&dt_config.parent) {
            Some(parent_cond) => match &parent_cond.parsed {
                Expression::Function(name, _) => (name.to_string(), parent_cond.compiled.clone()),
                _ => panic!("Unrecognized condition: {}", dt_config.condition),
            },
            None => {
                // When a datatype has no compiled condition this means that it accepts
                // everything.
                let default_pcond: Arc<dyn Fn(&str) -> bool + Sync + Send> = Arc::new(|_| true);
                ("search".to_string(), default_pcond)
            }
        };

        // We do not support testing of datatypes with exclude conditions, unless *both* the parent
        // and the child are excludes. In that case we can use modus tollens instead of modus
        // ponens to check whether the parent and child conditions have the right relationship.
        if ctype == "exclude" || ptype == "exclude" && !(ctype == "exclude" && ptype == "exclude") {
            log::warn!(
                "Testing of exclude() conditions is not supported unless both the child \
                 and parent datatypes are excludes. Skipping {}",
                dt_config.condition
            );
            return;
        }
        let (antecedent, consequent) = {
            if ctype == "exclude" {
                // modus tollens
                (pcond, ccond)
            } else {
                // modus ponens
                (ccond, pcond)
            }
        };

        // Next, generate 1000 random strings based on cregex and test each of them against
        // the child and parent datatypes, recording any failures:
        let mut rng = StdRng::from_entropy();
        // We attempt to generate ASCII-only strings for readability. In case the regex is
        // for whatever reason incompatible with this, fall back to using RandRegex without
        // regex_syntax::ParserBuilder:
        let mut parser = regex_syntax::ParserBuilder::new().unicode(false).build();
        let samples = {
            match parser.parse(&cregex) {
                Ok(hir) => {
                    let gen = RandRegex::with_hir(hir, 5).unwrap();
                    let samples = (&mut rng)
                        .sample_iter(&gen)
                        .take(1000)
                        .collect::<Vec<String>>();
                    samples
                }
                _ => {
                    let gen = RandRegex::compile(&cregex, 10).unwrap();
                    let samples = (&mut rng)
                        .sample_iter(&gen)
                        .take(1000)
                        .collect::<Vec<String>>();
                    samples
                }
            }
        };
        let mut failures = vec![];
        for sample in &samples {
            if antecedent(&sample) {
                if !consequent(&sample) {
                    failures.push(sample.to_string());
                }
            }
        }

        if failures.len() > 0 {
            // Sort the failures according to whether a given sample is ASCII-only, and secondarily
            // according to length:
            failures.sort_by(|a, b| {
                if a.is_ascii() {
                    std::cmp::Ordering::Less
                } else if b.is_ascii() {
                    std::cmp::Ordering::Greater
                } else {
                    a.len().cmp(&b.len())
                }
            });
            let failure_example = &failures[0];

            let error_msg = format!(
                "The datatype condition for '{}' was satisfied but the condition \
                 for its parent, '{}', was not satisfied, for the test value '{}'.",
                dt_config.datatype, dt_config.parent, failure_example,
            );

            // Now look through the column configuration, and find a column in some table which
            // has the datatype of the failing condition (the parent datatype). If there is a
            // configured column corresponding to the datatype, use shortest_failure to construct a
            // ValveCell which we then run through the validation engine to generate validation
            // messages for the value. Otherwise just report the failure:
            match get_any_column_with_datatype(valve, &dt_config.parent) {
                None => log::error!("{}", error_msg),
                Some((table_name, column_name)) => {
                    // Run the value through the validation engine to generate the normal validation
                    // messages for the value. First construct the ValveCell which will contain the
                    // validation info:
                    let mut cell = ValveCell {
                        nulltype: None,
                        value: SerdeValue::String(failure_example.to_string()),
                        valid: true,
                        messages: vec![],
                    };
                    // Send the cell through the validator:
                    validate_cell_datatype(
                        &valve.config,
                        &valve.datatype_conditions,
                        &table_name,
                        &column_name,
                        &mut cell,
                    );
                    let mut vmessages = String::from("");
                    for msg in cell.messages {
                        vmessages
                            .push_str(&format!(" {} ({} {});", msg.message, msg.rule, msg.level));
                    }
                    log::error!(
                        "{} Adding this value to, for instance, the column '{}' of the table '{}' \
                         would result in the following validation messages:{}",
                        error_msg,
                        column_name,
                        table_name,
                        vmessages
                    );
                }
            };
        }
    }

    // Iterate over all configured datatypes, checking their format strings (if any) as well
    // as the relation between a given datatype's condition and the condition of its parent:
    for (_, dt_config) in valve.config.datatype.iter() {
        check_dt_format(valve, &dt_config.datatype);
        if dt_config.parent != "" {
            check_dt_parent(valve, &dt_config);
        }
    }
    Ok(())
}
