//! Implementation of the column configuration guesser

use crate::{
    toolkit::{get_query_param, local_sql_syntax, QueryParam},
    valve::{Valve, ValveConfig, ValveDatatypeConfig},
    MOVE_INTERVAL, SQL_PARAM,
};
use fix_fn::fix_fn;
use indexmap::IndexMap;
use pad::{Alignment, PadStr};
use rand::{distributions, rngs::StdRng, Rng, SeedableRng};
use regex::Regex;
use serde_json::json;
use sqlx::{query as sqlx_query, Acquire, Row, Transaction, ValueRef};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::File,
};

/// Represents a sample of values from a given column
#[derive(Default)]
pub struct Sample {
    pub normalized: String,
    pub nulltype: String,
    pub datatype: String,
    pub structure: String,
    pub description: String,
    pub values: Vec<String>,
}

/// Represents a datatype match.
#[derive(Debug)]
pub struct DTMatch {
    pub datatype: String,
    pub success_rate: f32,
}

/// Represents a foreign key column match
pub struct FCMatch {
    pub table: String,
    pub column: String,
    pub sql_type: String,
}

/// Given a valve instance and a tsv file containing the contents of a table, draw random samples
/// of size `sample_size` from the table using the given random number generation seed, and use
/// them, while considering the given error rate that should be tolerated, to try and guess the
/// table and column configuration for the given table. If `verbose` is set to true progress
/// messages will be written to STDOUT. if `assume_yes` is set to true the guessed configuration
/// will be written to the database immediately without prompting the user. Returns true if the
/// changes have been written to the database, and false otherwise.
pub async fn guess(
    valve: &Valve,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: Option<&str>,
    table_tsv: &str,
    seed: &Option<u64>,
    sample_size: &usize,
    error_rate: &f32,
) -> bool {
    // If a seed was provided, use it to create the random number generator instead of
    // creating it using fresh entropy:
    let mut rng = match seed {
        None => StdRng::from_entropy(),
        Some(seed) => StdRng::seed_from_u64(*seed),
    };

    // Use the name of the TSV file to determine the table name if it has not been given:
    let table = match table {
        None => std::path::Path::new(table_tsv)
            .file_stem()
            .expect(&format!(
                "Error geting file stem for {}: Not a filename",
                table_tsv
            ))
            .to_str()
            .expect(&format!(
                "Error getting file stem for {}: Not a valid UTF-8 string",
                table_tsv
            )),
        Some(table) => table,
    };

    // Collect the random data samples from the tsv file:
    if valve.verbose {
        println!(
            "Getting {} random samples of rows from {} ...",
            sample_size, table_tsv
        );
    }
    let mut samples = get_random_samples(table_tsv, *sample_size, &mut rng);

    // Annotate the samples:
    for (i, (label, sample)) in samples.iter_mut().enumerate() {
        if valve.verbose {
            println!("Annotating sample for label '{}' ...", label);
        }
        annotate(tx, label, sample, &valve, error_rate, i == 0).await;
    }
    if valve.verbose {
        println!("Done!");
    }

    let required_table_table_headers = vec!["table", "path", "type", "description"];
    let required_column_table_headers = vec![
        "table",
        "column",
        "label",
        "nulltype",
        "datatype",
        "structure",
        "description",
    ];

    if valve.interactive {
        // Given tabular data, find the longest cell and return its length.
        fn get_col_width(data: &Vec<Vec<String>>) -> usize {
            let col_width = data
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|column| column.len())
                        .max()
                        .expect(&format!("Could not determine longest word in {:?}", row))
                })
                .max()
                .expect(&format!("Could not determine longest word in {:?}", data));
            // We add +2 for padding:
            col_width + 2
        }

        // Given tabular data, print it to the console using the given column width.
        fn print_data(data: &Vec<Vec<String>>, col_width: usize) {
            for row in data {
                for word in row {
                    print!(
                        "{}",
                        word.pad_to_width_with_alignment(col_width, Alignment::Left)
                    );
                }
                println!("");
            }
        }

        println!("\nThe following row will be inserted to \"table\":");
        let data = vec![
            required_table_table_headers
                .iter()
                .map(|h| h.to_string())
                .collect::<Vec<_>>(),
            vec![
                format!("{}", table),
                format!("{}", table_tsv),
                "".to_string(),
                "".to_string(),
            ],
        ];
        let col_width = get_col_width(&data);
        print_data(&data, col_width);

        println!("");

        println!("The following row will be inserted to \"column\":");
        let mut data = vec![required_column_table_headers
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<_>>()];
        for (label, sample) in &samples {
            let row = vec![
                format!("{}", table),
                format!("{}", sample.normalized),
                format!("{}", {
                    if *label != sample.normalized {
                        label
                    } else {
                        ""
                    }
                }),
                format!("{}", sample.nulltype),
                format!("{}", sample.datatype),
                format!("{}", sample.structure),
                format!("{}", sample.description),
            ];
            data.push(row);
        }
        let col_width = get_col_width(&data);
        print_data(&data, col_width);

        println!("");

        print!("Do you want to write this updated configuration to the database? [y/N] ");
        if !proceed::proceed() {
            println!("Not writing updated configuration to the database.");
            return false;
        }
    }

    // Returns the largest row number in the given configured table:
    async fn get_max_row_number_from_table(
        tx: &mut Transaction<'_, sqlx::Any>,
        table: &str,
    ) -> u32 {
        let sql = format!(
            r#"SELECT MAX("row_number") AS "row_number" FROM "{}""#,
            table
        );
        let query = sqlx_query(&sql);
        // TODO: Remove expect:
        let result = query
            .fetch_one(tx.acquire().await.expect("Bah!"))
            .await
            .expect(&format!("Error executing SQL: '{}'", sql));
        let raw_row_number = result
            .try_get_raw("row_number")
            .expect("Error retrieving row_number");
        let row_number = if raw_row_number.is_null() {
            0 as i64
        } else {
            result.get("row_number")
        };
        (row_number + 1) as u32
    }

    // Table configuration
    if valve.verbose {
        println!("Updating the table configuration in the database ...");
    }
    let row_number = get_max_row_number_from_table(tx, "table").await;
    let row_order = row_number * MOVE_INTERVAL;
    let sql = {
        let column_names = &required_table_table_headers
            .iter()
            .map(|h| format!(r#""{}""#, h))
            .collect::<Vec<_>>()
            .join(", ");
        local_sql_syntax(
            &valve.db_kind,
            &format!(
                r#"INSERT INTO "table" ("row_number", "row_order", {column_names}) VALUES
                   ({row_number}, {row_order}, {SQL_PARAM}, {SQL_PARAM}, NULL, NULL)"#,
            ),
        )
    };
    if valve.verbose {
        println!("Executing SQL: {}", sql);
    }
    let query = sqlx_query(&sql).bind(table).bind(table_tsv);
    // TODO: Remove expect.
    query
        .execute(tx.acquire().await.expect("Bah!"))
        .await
        .expect(&format!("Error executing SQL '{}'", sql));

    // Column configuration
    if valve.verbose {
        println!("Updating the column configuration in the database ...");
    }
    let mut row_number = get_max_row_number_from_table(tx, "column").await;
    for (label, sample) in &samples {
        let column_names = &required_column_table_headers
            .iter()
            .map(|h| format!(r#""{}""#, h))
            .collect::<Vec<_>>()
            .join(", ");
        let mut params = vec![];
        let values = vec![
            // row_number
            format!("{row_number}"),
            format!("{row_order}", row_order = row_number * MOVE_INTERVAL),
            // table
            {
                params.push(table);
                format!("{}", SQL_PARAM)
            },
            // column
            {
                params.push(&sample.normalized);
                format!("{}", SQL_PARAM)
            },
            // label
            {
                if *label != sample.normalized {
                    params.push(label);
                    format!("{}", SQL_PARAM)
                } else {
                    "NULL".to_string()
                }
            },
            // nulltype
            {
                if sample.nulltype != "" {
                    params.push(&sample.nulltype);
                    format!("{}", SQL_PARAM)
                } else {
                    "NULL".to_string()
                }
            },
            // datatype
            {
                params.push(&sample.datatype);
                format!("{}", SQL_PARAM)
            },
            // structure
            {
                if sample.structure != "" {
                    params.push(&sample.structure);
                    format!("{}", SQL_PARAM)
                } else {
                    "NULL".to_string()
                }
            },
            // description
            {
                if sample.description != "" {
                    params.push(&sample.description);
                    format!("{}", SQL_PARAM)
                } else {
                    "NULL".to_string()
                }
            },
        ]
        .join(", ");

        let sql = local_sql_syntax(
            &valve.db_kind,
            &format!(
                r#"INSERT INTO "column" ("row_number", "row_order", {}) VALUES ({})"#,
                column_names, values
            ),
        );
        if valve.verbose {
            println!("Executing SQL: {}", sql);
        }
        let mut query = sqlx_query(&sql);
        for param in &params {
            query = query.bind(param);
        }
        // TODO Remove expects
        query
            .execute(tx.acquire().await.expect("Bah!"))
            .await
            .expect(&format!("Error executing SQL '{}'", sql));
        row_number += 1;
    }

    if valve.verbose {
        println!("Done!");
    }
    return true;
}

/// Add annotations to the data sample indicating best guesses as to the datatype, nulltype,
/// and structure associated with the column identified by the given label.
pub async fn annotate(
    tx: &mut Transaction<'_, sqlx::Any>,
    label: &str,
    sample: &mut Sample,
    valve: &Valve,
    error_rate: &f32,
    is_primary_candidate: bool,
) {
    // Guess the datatype of the column associated with the given sample, given the provided
    // datatype hierarchy information:
    fn get_datatype(
        valve: &Valve,
        sample: &Sample,
        dt_hierarchies: &HashMap<usize, HashMap<String, Vec<ValveDatatypeConfig>>>,
        error_rate: &f32,
    ) -> String {
        // Decides whether `sample` is a match for the given datatype and return true or false,
        // as the case may be, along with the success rate when testing the datatype's condition
        // against the sample.
        let is_match = |datatype: &ValveDatatypeConfig| -> (bool, f32) {
            // If the datatype has no associated condition then it matches anything:
            if datatype.condition == "" {
                return (true, 1 as f32);
            }
            // If the SQL type of the datatype is NULL then it cannot match anything:
            if datatype.sql_type.to_lowercase() == "null" {
                return (false, 0 as f32);
            }
            // Otherwise we test the datatype condition against all of the values in the sample in
            // order to decide whether the sample matches the given datatype:
            let condition = &valve
                .datatype_conditions
                .get(&datatype.datatype)
                .expect(&format!(
                    "Condition '{}' not found in datatype conditions",
                    datatype.condition
                ))
                .compiled;
            let num_values = sample.values.len();
            let num_passed = sample.values.iter().filter(|v| condition(v)).count();
            let success_rate = num_passed as f32 / num_values as f32;
            return ((1 as f32 - success_rate) <= *error_rate, success_rate);
        };

        // Given a number of datatypes that match `sample`, uses a tiebreaking procedure to
        // determine which one to use:
        let tiebreak = |dt_matches: &Vec<DTMatch>| -> String {
            let mut in_types = vec![];
            let mut other_types = vec![];
            let parents = dt_matches
                .iter()
                .map(
                    |dt_match| match valve.config.datatype.get(&dt_match.datatype) {
                        Some(dt) => dt.parent.to_string(),
                        None => String::new(),
                    },
                )
                .filter(|parent| parent != "")
                .collect::<HashSet<_>>();

            for dt in dt_matches {
                // We only want concrete matches, so any match that is a parent of a concrete match
                // is ignored:
                if !parents.contains(&dt.datatype) {
                    let dt_config = valve.config.datatype.get(&dt.datatype).expect(&format!(
                        "Could not find datatype config for '{}'",
                        dt.datatype
                    ));
                    if dt_config
                        .condition
                        .trim_start()
                        .to_lowercase()
                        .starts_with("in(")
                    {
                        in_types.push(dt);
                    } else {
                        other_types.push(dt);
                    }
                }
            }

            // Datatypes whose condition is of the form "in(...)" are preferred over other
            // datatypes:
            if in_types.len() >= 1 {
                in_types
                    .sort_unstable_by(|a, b| b.success_rate.partial_cmp(&a.success_rate).unwrap());
                return in_types[0].datatype.to_string();
            } else if other_types.len() >= 1 {
                other_types
                    .sort_unstable_by(|a, b| b.success_rate.partial_cmp(&a.success_rate).unwrap());
                return other_types[0].datatype.to_string();
            } else {
                panic!("Error tiebreaking datatypes: {:#?}", dt_matches);
            }
        };

        // For each level in the datatype hierachy, check whether one of the datatypes at that
        // level is a good match for the sample, and if so add it to the list of matching datatypes
        // found for that level. If any matching datatypes are found then return the best match,
        // determined in accordance with a tiebreaking procedure (see above) in the case where there
        // is more than one.
        for depth in 0..dt_hierarchies.len() {
            let datatypes_to_check = dt_hierarchies
                .get(&depth)
                .unwrap()
                .iter()
                .map(|(_dt_name, dt)| &dt[0]);
            let mut matching_datatypes = vec![];
            for datatype in datatypes_to_check {
                let (success, success_rate) = is_match(datatype);
                if success {
                    matching_datatypes.push(DTMatch {
                        datatype: datatype.datatype.to_string(),
                        success_rate: success_rate,
                    });
                }
            }

            if matching_datatypes.len() == 1 {
                return matching_datatypes[0].datatype.to_string();
            } else if matching_datatypes.len() > 1 {
                return tiebreak(&matching_datatypes);
            }
        }

        // Return an empty string if nothing is found:
        return String::new();
    }

    // Uses the given valve config to get a list of the columns, other than those belonging to
    // internal tables, which have a primary or unique key constraint and whose datatype is
    // compatible with the given datatype.
    fn get_potential_foreign_columns(valve: &Valve, datatype: &str) -> Vec<FCMatch> {
        // Maps a given datatype name to its corresponding SQL type.
        fn get_sql_type(valve: &Valve, datatype: &str) -> String {
            match valve.config.datatype.get(datatype) {
                None => "".to_string(),
                Some(dt_config) => {
                    if dt_config.sql_type != "" {
                        dt_config.sql_type.to_string()
                    } else {
                        get_sql_type(valve, &dt_config.parent)
                    }
                }
            }
        }

        // Gets the SQL type corresponding to the given datatype, then further maps the result as
        // follows:
        //   'integer' -> 'integer'
        //   'numeric' -> 'numeric'
        //   'real' -> 'real'
        //   everything else -> 'text'
        fn get_coarser_sql_type(valve: &Valve, datatype: &str) -> String {
            let sql_type = get_sql_type(valve, datatype).to_lowercase();
            if !vec!["integer", "numeric", "real"].contains(&sql_type.as_str()) {
                return "text".to_string();
            } else {
                return sql_type;
            }
        }

        let mut potential_foreign_columns = vec![];
        let this_sql_type = get_coarser_sql_type(valve, datatype);
        for (table, table_config) in valve.config.table.iter() {
            if !table_config.options.contains("internal") {
                for (column, column_config) in table_config.column.iter() {
                    if vec!["primary", "unique"].contains(&column_config.structure.as_str()) {
                        let foreign_sql_type = get_coarser_sql_type(valve, &column_config.datatype);
                        if foreign_sql_type == this_sql_type {
                            potential_foreign_columns.push(FCMatch {
                                table: table.to_string(),
                                column: column.to_string(),
                                sql_type: foreign_sql_type.to_string(),
                            });
                        }
                    }
                }
            }
        }
        potential_foreign_columns
    }

    // Given a Valve configuration, return a representation of the datatype hierarchy in the
    // following form:
    // {0: {'dt_name_1': [valve_datatype_config_a, valve_datatype_config_b, ...],
    //      ...
    //      'dt_name_n': [...]},
    //  1: {...}
    //  ...
    // }
    fn get_dt_hierarchies(
        config: &ValveConfig,
    ) -> HashMap<usize, HashMap<String, Vec<ValveDatatypeConfig>>> {
        // Given a datatype, return its hierarchy in the form of a vector of datatype names ordered
        // from the least to the most generic.
        fn get_hierarchy_for_dt(
            config: &ValveConfig,
            primary_dt_name: &str,
        ) -> Vec<ValveDatatypeConfig> {
            let get_parents = fix_fn!(|get_parents, dt_name: &str| -> Vec<ValveDatatypeConfig> {
                let mut datatypes = vec![];
                if dt_name != "" {
                    let datatype = config
                        .datatype
                        .get(dt_name)
                        .expect(&format!("'{}' not found in datatype config", dt_name));
                    if datatype.datatype != primary_dt_name {
                        datatypes.push(datatype.clone())
                    }
                    datatypes.append(&mut get_parents(&datatype.parent));
                }
                datatypes
            });
            let mut hierarchy_for_dt = vec![config.datatype.get(primary_dt_name).unwrap().clone()];
            hierarchy_for_dt.append(&mut get_parents(primary_dt_name));
            hierarchy_for_dt
        }

        // Returns datatype hierarchies, from the given collections of datatype hierachies and
        // universal datatypes, that are deeper in the hierarchy than `depth`.
        fn get_higher_datatypes(
            dt_hierarchies: &HashMap<usize, HashMap<String, Vec<ValveDatatypeConfig>>>,
            universals: &HashMap<String, Vec<ValveDatatypeConfig>>,
            depth: usize,
        ) -> HashMap<String, Vec<ValveDatatypeConfig>> {
            let datatypes_at_depth = dt_hierarchies
                .get(&depth)
                .and_then(|d| Some(d.keys().collect::<Vec<_>>()))
                .unwrap_or(vec![]);
            let mut higher_datatypes = HashMap::new();
            if !datatypes_at_depth.is_empty() {
                let universals = universals.keys().collect::<Vec<_>>();
                let lower_datatypes = {
                    let mut lower_datatypes = vec![];
                    for i in 0..depth {
                        lower_datatypes.append(
                            &mut dt_hierarchies
                                .get(&i)
                                .and_then(|d| Some(d.keys().collect::<Vec<_>>()))
                                .unwrap_or(vec![]),
                        );
                    }
                    lower_datatypes
                };
                for (_dt_name, dt_hierarchy) in dt_hierarchies
                    .get(&depth)
                    .expect(&format!("No datatype hierarchies at depth: {}", depth,))
                    .iter()
                {
                    if dt_hierarchy.len() > 1 {
                        let parent_hierarchy = &dt_hierarchy[1..];
                        let parent = &parent_hierarchy[0].datatype;
                        if !datatypes_at_depth.contains(&&parent)
                            && !lower_datatypes.contains(&&parent)
                            && !universals.contains(&&parent)
                        {
                            higher_datatypes.insert(parent.to_string(), parent_hierarchy.to_vec());
                        }
                    }
                }
            }
            higher_datatypes
        }

        let all_dt_names = config.datatype.keys().collect::<Vec<_>>();
        let mut dt_hierarchies = HashMap::from([(0, HashMap::new())]);
        let mut universals = HashMap::new();
        for dt_name in &all_dt_names {
            if all_dt_names
                .iter()
                .filter(|child| &config.datatype.get(**child).unwrap().parent == *dt_name)
                .collect::<Vec<_>>()
                .is_empty()
            {
                // Add all leaf datatypes (i.e., those without children) to dt_hierarchies at 0 depth:
                dt_hierarchies
                    .get_mut(&0)
                    .unwrap()
                    .insert(dt_name.to_string(), get_hierarchy_for_dt(config, dt_name));
            } else if config
                .datatype
                .get(*dt_name)
                .expect(&format!("'{}' not found in datatype config", dt_name))
                .parent
                == ""
                || config.datatype.get(*dt_name).unwrap().condition == ""
            {
                // Ungrounded and unconditioned datatypes go into the universals category, which are
                // added to the top of dt_hierarchies later:
                universals.insert(dt_name.to_string(), get_hierarchy_for_dt(config, dt_name));
            }
        }
        let mut depth = 0;
        let mut higher_dts = get_higher_datatypes(&dt_hierarchies, &universals, depth);
        while !higher_dts.is_empty() {
            depth += 1;
            dt_hierarchies.insert(depth, higher_dts);
            higher_dts = get_higher_datatypes(&dt_hierarchies, &universals, depth);
        }
        dt_hierarchies.insert(depth + 1, universals);
        dt_hierarchies
    }

    // Checks a list of potential foreign columns against the given sample, and return those
    // columns from the list that are a good enough match, taking into consideration the given
    // acceptable error rate.
    async fn get_candidate_froms(
        valve: &Valve,
        tx: &mut Transaction<'_, sqlx::Any>,
        sample: &Sample,
        potential_foreign_columns: &Vec<FCMatch>,
        error_rate: &f32,
    ) -> Vec<String> {
        // Regular expression to check for numbers:
        let re = Regex::new(r"^-?\d+(\.\d+)?$").unwrap();
        let mut candidate_froms = vec![];
        for foreign in potential_foreign_columns {
            let mut num_matches = 0;
            let mut num_values = sample.values.len();
            for value in &sample.values {
                if sample.nulltype == "empty" && value == "" {
                    // If this value is legitimately empty then it should not be taken into account
                    // when counting the number of values in the target that are found in the
                    // candidate foreign column:
                    num_values -= 1;
                    continue;
                }
                if foreign.sql_type != "text" && !re.is_match(value) {
                    // If this value is of the wrong type then there is no need to explicitly check
                    // if it exists in the foreign column. It is automatically a failed match.
                    continue;
                }
                let sql = local_sql_syntax(
                    &valve.db_kind,
                    &format!(
                        r#"SELECT 1 FROM "{}" WHERE "{}" = {} LIMIT 1"#,
                        foreign.table, foreign.column, SQL_PARAM
                    ),
                );
                let param = get_query_param(&json!(value), &foreign.sql_type);
                let mut query = sqlx_query(&sql);
                match param {
                    QueryParam::Integer(p) => query = query.bind(p),
                    QueryParam::Numeric(p) => query = query.bind(p),
                    QueryParam::Real(p) => query = query.bind(p),
                    QueryParam::String(p) => query = query.bind(p),
                }
                // TODO: Remove expect:
                num_matches += query
                    .fetch_all(tx.acquire().await.expect("Bah!"))
                    .await
                    .expect(&format!("Error executing SQL: {}", sql))
                    .len();
                if ((num_values as f32 - num_matches as f32) / num_values as f32) < *error_rate {
                    candidate_froms.push(format!("from({}.{})", foreign.table, foreign.column))
                }
            }
        }
        candidate_froms
    }

    let sample_has_nulltype = {
        let num_values = sample.values.len();
        let num_empties = sample.values.iter().filter(|v| *v == "").count();
        let pct_empty = num_empties as f32 / num_values as f32;
        pct_empty > *error_rate
    };

    // Use the data sample to guess whether the given column should allow empty values:
    if sample_has_nulltype {
        sample.nulltype = "empty".to_string();
    }

    // Use the valve config to retrieve the valve datatype hierarchies and use them to guess the
    // datatype of the sample:
    let dt_hierarchies = get_dt_hierarchies(&valve.config);
    sample.datatype = get_datatype(valve, &sample, &dt_hierarchies, error_rate);

    // Use the valve config to get a list of columns that are allowed to be foreign columns for
    // a column of the given datatype, then compare the contents of each column with the contents
    // of the sampled column and possibly annotate the sample with a from() structure if there is
    // one and only candidate from().
    let potential_foreign_columns = get_potential_foreign_columns(valve, &sample.datatype);
    let froms =
        get_candidate_froms(valve, tx, sample, &potential_foreign_columns, error_rate).await;
    if froms.len() == 1 {
        sample.structure = froms[0].to_string();
    } else if froms.len() > 1 {
        println!(
            "Warning: Column '{}' has multiple from() candidates: {:?}. \
             Declining to choose one.",
            label, froms
        );
    }

    // Check if the column is a unique/primary column:
    if sample.structure == "" && sample.nulltype == "" {
        let sample_has_duplicates = {
            // Ignore empties when looking for duplicates:
            let distinct_values = sample
                .values
                .iter()
                .filter(|v| *v != "")
                .collect::<HashSet<_>>();
            (sample.values.len() as f32 - distinct_values.len() as f32)
                > (error_rate * sample.values.len() as f32)
        };
        if !sample_has_duplicates {
            if is_primary_candidate {
                sample.structure = "primary".to_string();
            } else {
                sample.structure = "unique".to_string();
            }
        }
    }
}

/// Get a random sample of the data in the given table, of size `sample_size`, using `rng` as the
/// random number generator. The sample is returned in the form of an index map with entries for
/// every column, mapping column labels to the sample values for each column. For instance if
/// sample_size is 3 and the column names are foo and bar, the map returned will look like:
/// ```
/// {
///   "foo": Sample {
///            values: [value1, value2, value3]
///            [other fields ...],
///          },
///   "bar": Sample {
///            values: [value1, value2, value3]
///            [other fields ...],
///          },
/// }
/// ```
pub fn get_random_samples(
    table_tsv: &str,
    sample_size: usize,
    rng: &mut StdRng,
) -> IndexMap<String, Sample> {
    // Count the number of rows in the file and then generate a random sample of row numbers:
    let sample_row_numbers = {
        let total_rows = std::fs::read_to_string(table_tsv)
            .expect(&format!("Error reading from {}", table_tsv))
            .lines()
            .count();
        // If the total number of rows in the file is smaller than sample_size then sample
        // everything, otherwise take a random sample of row_numbers from the file. The reason
        // that the range runs from 0 to (total_rows - 1) is that total_rows includes the
        // header row, which is going to be removed in the first step below (as a result of
        // calling next()) when the headers are read. Note also that we are using a BTreeSet so
        // as to make sure that the elements of the set are always in sorted order.
        if total_rows <= sample_size {
            (0..total_rows - 1).collect::<BTreeSet<_>>()
        } else {
            let sample_row_numbers = rng
                .sample_iter(distributions::Uniform::new(0, total_rows - 1))
                .take(sample_size)
                .collect::<BTreeSet<_>>();
            sample_row_numbers
        }
    };

    // Create a CSV reader. We set has_headers to false even though the file does have headers,
    // since we are going to be reading them explicitly.
    let mut rdr = match File::open(table_tsv) {
        Err(e) => panic!("Unable to open '{}': {}", table_tsv, e),
        Ok(table_file) => csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b'\t')
            .from_reader(table_file),
    };

    // Use the CSV reader and the sample row numbers collected above to construct the random
    // sample of rows from the file. We begin by explicitly reading the header row:
    let mut records = rdr.records();
    let headers = records
        .next()
        .expect("Header row not found.")
        .expect("Error while reading header row")
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    // We will use this pattern to normalize the labels represented by the column headers:
    let pattern = Regex::new(r#"[^0-9a-zA-Z_]+"#).expect("Invalid regex pattern");

    // Read in the specified samples in order:
    let mut samples: IndexMap<String, Sample> = IndexMap::new();
    let mut prev_rn = None;
    for rn in &sample_row_numbers {
        let nth_record = {
            let n = match prev_rn {
                None => *rn,
                Some(prev_rn) => *rn - prev_rn - 1,
            };
            records
                .nth(n)
                .expect(&format!("No record found at position {}", n))
                .expect(&format!("Error while reading record at position {}", n))
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        };
        for (i, label) in headers.iter().enumerate() {
            // If samples doesn't already contain an entry for this label, initialize and add one:
            if !samples.contains_key(label) {
                let ncolumn = {
                    let mut ncolumn = pattern.replace_all(label, "_").to_lowercase();
                    if ncolumn.starts_with("_") {
                        ncolumn = ncolumn.strip_prefix("_").unwrap().to_string();
                    }
                    if ncolumn.ends_with("_") {
                        ncolumn = ncolumn.strip_suffix("_").unwrap().to_string();
                    }
                    for (_label, sample) in samples.iter() {
                        if sample.normalized == ncolumn {
                            panic!(
                                "The data has more than one column with the normalized name {}",
                                ncolumn
                            );
                        }
                    }
                    ncolumn
                };
                samples.insert(
                    label.to_string(),
                    Sample {
                        normalized: ncolumn,
                        values: vec![],
                        ..Default::default()
                    },
                );
            }
            // Add the ith entry of the nth_record to the sample under the entry for label:
            samples
                .get_mut(label)
                .expect(&format!("Could not find label '{}' in sample", label))
                .values
                .push(nth_record[i].to_string());
        }
        prev_rn = Some(*rn);
    }
    samples
}
