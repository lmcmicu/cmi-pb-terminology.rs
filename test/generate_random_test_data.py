#!/usr/bin/env python3

import json
import math
import random
import string
import subprocess
import sys

from argparse import ArgumentParser


TOKEN_LENGTH = 9
WINDOW_SIZE = 50


def get_special_tables(config):
    return ["message", "history"] + [k for k, v in config["special"].items() if v is not None]


def get_table_columns(config, table):
    return [column for column in config["table"][table]["column_order"]]


def has_nulltype(config, table, column):
    return bool(config["table"][table]["column"][column].get("nulltype"))


def get_column_structure(config, table, column):
    return config["table"][table]["column"][column].get("structure")


def get_column_datatype(config, table, column):
    return config["table"][table]["column"][column]["datatype"]


def get_foreign_key(config, table, column):
    return [f for f in config["constraint"]["foreign"][table] if f["column"] == column][0]


def get_tree(config, table, column):
    return [f for f in config["constraint"]["tree"][table] if f["parent"] == column][0]


def get_under(config, table, column):
    return [f for f in config["constraint"]["under"][table] if f["column"] == column][0]


def get_value_from_prev_insert(config, prev_inserts, from_table, from_column, to_table, to_column):
    global WINDOW_SIZE

    # Note: because we are loading the tables and columns in the correct order (i.e. such that
    # all dependencies are loaded before the tables and columns they depend on), the list of
    # previous inserts for the from_table/from_column will never be empty.
    if len(prev_inserts[from_table][from_column]) == 1:
        if has_nulltype(config, to_table, to_column):
            return ""
        else:
            return prev_inserts[from_table][from_column][0]
    else:
        # Select at random from the last N inserted values, with N given by WINDOW_SIZE:
        prev_inserts[from_table][from_column] = prev_inserts[from_table][from_column][-WINDOW_SIZE:]
        from_values = prev_inserts[from_table][from_column]
        # We'd ideally like to exclude the last inserted value from consideration, but we save it
        # here in case we cannot:
        last_val = from_values[len(from_values) - 1]
        from_values = from_values[0 : len(from_values) - 1]

        to_values = set(prev_inserts[to_table][to_column])
        values_to_choose_from = [f for f in from_values if f not in to_values]

        if not values_to_choose_from:
            return last_val
        else:
            return values_to_choose_from[random.randrange(len(values_to_choose_from))]


def get_constrained_cell_value(config, table, column, row_num, prev_inserts):
    global TOKEN_LENGTH

    structure = get_column_structure(config, table, column)
    datatype = get_column_datatype(config, table, column).casefold()
    table4_numerics = [
        "623355055",
        "534436633",
        "399784475",
        "877227330",
        "398143301",
        "394334137",
        "708425682",
        "508369551",
        "241572254",
        "256276245",
    ]
    if table == "table4" and column == "numeric_foreign_column":
        cell = table4_numerics[random.randrange(len(table4_numerics))]
    elif structure.startswith("from("):
        fkey = get_foreign_key(config, table, column)
        ftable = fkey["ftable"]
        fcolumn = fkey["fcolumn"]
        cell = get_value_from_prev_insert(config, prev_inserts, ftable, fcolumn, table, column)
    elif structure.startswith("tree("):
        tkey = get_tree(config, table, column)
        tcolumn = tkey["child"]
        cell = get_value_from_prev_insert(config, prev_inserts, table, tcolumn, table, column)
    elif structure.startswith("under("):
        # Note that properly satisfying the under constraint requires, not only that
        # the cell is in the specified tree column, but also (a) that the tree
        # actually exists, and (b) that the value is "under" the under value. To do
        # this properly, though, would require a decent amount of memory. So perhaps
        # it's not worth it to check for (a) and (b) and allow any offending cells
        # to generate errors which we can then verify are handled properly by valve.
        ukey = get_under(config, table, column)
        ttable = ukey["ttable"]
        tcolumn = ukey["tcolumn"]
        cell = get_value_from_prev_insert(config, prev_inserts, ttable, tcolumn, table, column)
    elif datatype in [
        "prefix",
        "iri",
        "trimmed_line",
        "label",
        "word",
    ]:
        cell = "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
    elif datatype == "curie":
        cell = (
            "".join(random.choices(string.ascii_lowercase, k=3)).upper()
            + ":"
            + "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
        )
    elif datatype == "text":
        cell = (
            "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
            + " "
            + "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
        )
    elif datatype == "integer":
        # No leading 0s:
        cell = "".join(random.choices("123456789", k=1)) + "".join(
            random.choices(string.digits, k=TOKEN_LENGTH - 1)
        )
    else:
        print(f"Warning: Unknown datatype: {datatype}. Generating a random string.")
        cell = "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))

    return cell


def main():
    global TOKEN_LENGTH

    parser = ArgumentParser(
        description="""
    Deterministically generate a specified amount of data, a specified percentage of which are
    errors, using the given VALVE table configuration and seed, to the output directory."""
    )
    parser.add_argument("seed", help="The seed to use to generate the random data")
    parser.add_argument("num_rows", help="The number of rows per table to generate")
    parser.add_argument(
        "pct_errors", help="The percentage of rows in each table that should have errors"
    )
    parser.add_argument(
        "input_table", help="The .TSV file representing the VALVE table configuration"
    )
    parser.add_argument(
        "output_dir", help="The output directory to write the new table configuration to"
    )
    args = parser.parse_args()
    seed = int(args.seed)
    num_rows = int(args.num_rows)
    pct_errors = int(args.pct_errors)
    input_table = args.input_table
    outdir = args.output_dir

    # Use the seed argument to seed the random data that will be generated:
    random.seed(seed)

    # Get the VALVE configuration:
    result = subprocess.run(
        ["./valve", "--source", input_table, "--database", "", "get", "valve-config"],
        capture_output=True,
    )
    if result.returncode != 0:
        error = result.stderr.decode()
        output = result.stdout.decode()
        if output:
            error = f"{error}\n{output}"
        print(f"{error}", file=sys.stderr)
        sys.exit(result.returncode)
    config = json.loads(result.stdout.decode())

    # Get the sorted list of tables to generate:
    result = subprocess.run(
        ["./valve", "--source", input_table, "--database", "", "get", "table-order"],
        capture_output=True,
    )
    if result.returncode != 0:
        error = result.stderr.decode()
        output = result.stdout.decode()
        if output:
            error = f"{error}\n{output}"
        print(f"{error}", file=sys.stderr)
        sys.exit(result.returncode)
    data_tables = [t.strip() for t in result.stdout.decode().split(",")]
    data_tables = [t for t in data_tables if t not in get_special_tables(config)]

    # This is a record of the last inserted values for each table and column. When one column
    # takes its values from another column, then we look here and fetch the last inserted value of
    # the second column.
    prev_inserts = {}

    # The TSV files corresponding to each data table:
    tsv_files = {}
    for table in data_tables:
        tsv_files[table] = open(f"{outdir}/{table}.tsv", "w")
        columns = get_table_columns(config, table)
        print("\t".join(columns), file=tsv_files[table])

    num_error_rows = math.ceil((pct_errors / 100) * num_rows)
    error_proportion = None if not num_error_rows else math.floor(num_rows / num_error_rows)
    for row_num in range(1, num_rows + 1):
        for table in data_tables:
            is_error_row = error_proportion and row_num % error_proportion == 1
            columns = get_table_columns(config, table)
            error_column = random.randrange(len(columns))
            row = {}
            for column_num, column in enumerate(columns):
                is_error_column = is_error_row and column_num == error_column
                if (
                    not is_error_column
                    and has_nulltype(config, table, column)
                    and row_num % random.randrange(2, num_rows) == 1
                ):
                    # If the column allows empty values, assign an empty value "sometimes":
                    cell = ""
                elif not is_error_column:
                    cell = get_constrained_cell_value(config, table, column, row_num, prev_inserts)
                else:
                    structure = get_column_structure(config, table, column)
                    datatype = get_column_datatype(config, table, column)
                    if structure in ["unique", "primary"]:
                        cell = ""
                    elif datatype in [
                        "prefix",
                        "iri",
                        "word",
                        "curie",
                    ]:
                        cell = (
                            "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
                            + " "
                            + "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
                        )
                    else:
                        if datatype == "integer":
                            cell = "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
                        else:
                            # No leading 0s:
                            cell = "".join(random.choices("123456789", k=1)) + "".join(
                                random.choices(string.digits, k=TOKEN_LENGTH - 1)
                            )

                row[column] = cell
                if not prev_inserts.get(table):
                    prev_inserts[table] = {}
                if not prev_inserts[table].get(column):
                    prev_inserts[table][column] = []
                prev_inserts[table][column].append(cell)

            row = "\t".join([row[column] for column in row])
            print(row, file=tsv_files[table])


if __name__ == "__main__":
    main()
