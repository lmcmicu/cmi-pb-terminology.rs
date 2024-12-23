#!/usr/bin/env python3

import csv
import json
import random
import re
import sqlite3
import subprocess
import sys
import time

from copy import deepcopy
from guess_grammar import grammar, TreeToDict

from argparse import ArgumentParser
from lark import Lark
from numbers import Number
from pathlib import Path
from pprint import pformat
from textwrap import dedent


SPECIAL_TABLES = ["table", "column", "datatype", "rule", "history", "message"]
VERBOSE = False


def log(message, force=False, suppress_time=False):
    global VERBOSE

    if force or VERBOSE:
        if not suppress_time:
            print(f"{time.asctime()} {message}", file=sys.stderr)
        else:
            print(f"{message}", file=sys.stderr)


def has_ncolumn(sample, ncolumn):
    return bool([label for label in sample if sample[label]["normalized"] == ncolumn])


def get_random_sample(table, sample_size):
    # Get the number of rows in the file (we substract 1 for the header row):
    with open(table, "rb") as f:
        total_rows = sum(1 for _ in f) - 1

    if total_rows <= sample_size:
        sample_size = total_rows
        sample_row_numbers = range(0, total_rows)
    else:
        sample_row_numbers = random.sample(range(0, total_rows), sample_size)
    with open(table) as f:
        rows = [r for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)]
        sample = {}
        pattern = re.compile(r"[^0-9a-zA-Z_]+")
        for i in sample_row_numbers:
            for label, value in rows[i].items():
                if label not in sample:
                    ncolumn = re.sub(pattern, "_", label).casefold().strip("_")
                    if has_ncolumn(sample, ncolumn):
                        print(
                            "The data has more than one column with the normalized name "
                            f"{ncolumn}"
                        )
                        sys.exit(1)
                    sample[label] = {
                        "normalized": ncolumn,
                        "values": [],
                    }
                sample[label]["values"].append(value)
    return sample


def get_valve_config(valve_table):
    result = subprocess.run(
        ["./valve", "--source", valve_table, "--database", "", "dump-config"], capture_output=True
    )
    if result.returncode != 0:
        error = result.stderr.decode()
        output = result.stdout.decode()
        if output:
            error = f"{error}\n{output}"
        print(f"{error}", file=sys.stderr)
        sys.exit(result.returncode)
    return json.loads(result.stdout.decode())


def get_hierarchy_for_dt(config, primary_dt_name):
    def get_parents(dt_name):
        datatypes = []
        if dt_name is not None and dt_name != "":
            datatype = config["datatype"][dt_name]
            if datatype["datatype"] != primary_dt_name:
                datatypes.append(datatype)
            datatypes += get_parents(datatype.get("parent"))
        return datatypes

    return [config["datatype"][primary_dt_name]] + get_parents(primary_dt_name)


def get_dt_hierarchies(config):
    """
    Given a VALVE configuration, return a datatype hierarchy that looks like this:
    {0: {'dt_name_1': [{'datatype': 'dt_name_1',
                        'description': 'a description',
                         ...},
                       {'datatype': 'parent datatype',
                        'description': 'a description',
                        ...},
                       {'datatype': 'grandparent datatype',
                        'description': 'a description',
                        ...},
                       ...],
         'dt_name_2': etc.},
     1: ... etc.}
    """

    def get_higher_datatypes(datatype_hierarchies, universals, depth):
        current_datatypes = [dt_name for dt_name in datatype_hierarchies.get(depth, [])]
        higher_datatypes = {}
        if current_datatypes:
            universals = [dt_name for dt_name in universals]
            lower_datatypes = []
            for i in range(0, depth):
                lower_datatypes += [dt_name for dt_name in datatype_hierarchies.get(i, [])]
            for dt_name in dt_hierarchies[depth]:
                dt_hierarchy = dt_hierarchies[depth][dt_name]
                if len(dt_hierarchy) > 1:
                    parent_hierarchy = dt_hierarchy[1:]
                    parent = parent_hierarchy[0]["datatype"]
                    if parent not in current_datatypes + lower_datatypes + universals:
                        higher_datatypes[parent] = parent_hierarchy
        return higher_datatypes

    dt_config = config["datatype"]
    dt_names = [dt_name for dt_name in dt_config]
    dt_hierarchies = {0: {}}
    universals = {}
    for dt_name in dt_names:
        # Add all the leaf datatypes to dt_hierarchies at 0 depth:
        children = [child for child in dt_names if dt_config[child].get("parent") == dt_name]
        if not children:
            dt_hierarchies[0][dt_name] = get_hierarchy_for_dt(config, dt_name)
        # Ungrounded and unconditioned datatypes go into the universals category, which are added
        # to the top of dt_hierarchies later:
        elif not dt_config[dt_name].get("parent") or not dt_config[dt_name].get("condition"):
            universals[dt_name] = get_hierarchy_for_dt(config, dt_name)

    depth = 0
    higher_dts = get_higher_datatypes(dt_hierarchies, universals, depth)
    while higher_dts:
        depth += 1
        dt_hierarchies[depth] = deepcopy(higher_dts)
        higher_dts = get_higher_datatypes(dt_hierarchies, universals, depth)
    dt_hierarchies[depth + 1] = universals
    return dt_hierarchies


def get_sql_type(config, datatype):
    """Given the config map and the name of a datatype, climb the datatype tree (as required),
    and return the first 'SQL type' found."""
    if "datatype" not in config:
        print("Missing datatypes in config")
        sys.exit(1)
    if datatype not in config["datatype"]:
        return None
    if config["datatype"][datatype].get("SQL type"):
        return config["datatype"][datatype]["SQL type"]
    return get_sql_type(config, config["datatype"][datatype].get("parent"))


def get_potential_foreign_columns(config, datatype):
    global SPECIAL_TABLES

    def get_coarser_sql_type(datatype):
        sql_type = get_sql_type(config, datatype)
        if sql_type not in ["integer", "numeric", "real"]:
            return "text"
        else:
            return sql_type.casefold()

    potential_foreign_columns = []
    this_sql_type = get_coarser_sql_type(datatype)
    for table, table_config in config["table"].items():
        if table not in SPECIAL_TABLES:
            for column, column_config in table_config["column"].items():
                if column_config.get("structure") in ["primary", "unique"]:
                    foreign_sql_type = get_coarser_sql_type(column_config["datatype"])
                    if foreign_sql_type == this_sql_type:
                        potential_foreign_columns.append(
                            {
                                "table": table,
                                "column": column,
                                "sql_type": foreign_sql_type,
                            }
                        )
    return potential_foreign_columns


SAVED_CONDITIONS = {}


def get_compiled_condition(condition, parser):
    global SAVED_CONDITIONS

    if condition in SAVED_CONDITIONS:
        return SAVED_CONDITIONS[condition]

    parsed_condition = parser.parse(condition)
    if len(parsed_condition) != 1:
        print(
            f"'{condition}' is invalid. Only one condition per column is allowed.", file=sys.stderr
        )
        sys.exit(1)
    parsed_condition = parsed_condition[0]
    if parsed_condition["type"] == "function" and parsed_condition["name"] == "equals":
        expected = re.sub(r"^['\"](.*)['\"]$", r"\1", parsed_condition["args"][0]["value"])
        compiled_condition = lambda x: x == expected
    elif parsed_condition["type"] == "function" and parsed_condition["name"] in (
        "exclude",
        "match",
        "search",
    ):
        pattern = re.sub(r"^['\"](.*)['\"]$", r"\1", parsed_condition["args"][0]["pattern"])
        flags = parsed_condition["args"][0]["flags"]
        flags = "(?" + "".join(flags) + ")" if flags else ""
        pattern = re.compile(flags + pattern)
        if parsed_condition["name"] == "exclude":
            compiled_condition = lambda x: not bool(pattern.search(x))
        elif parsed_condition["name"] == "match":
            compiled_condition = lambda x: bool(pattern.fullmatch(x))
        else:
            compiled_condition = lambda x: bool(pattern.search(x))
    elif parsed_condition["type"] == "function" and parsed_condition["name"] == "in":
        alternatives = [
            re.sub(r"^['\"](.*)['\"]$", r"\1", arg["value"]) for arg in parsed_condition["args"]
        ]
        compiled_condition = lambda x: x in alternatives
    else:
        print(f"Unrecognized condition: {condition}", file=sys.stderr)
        sys.exit(1)

    SAVED_CONDITIONS[condition] = compiled_condition
    return compiled_condition


def annotate(label, sample, config, error_rate, is_primary_candidate):
    def has_nulltype(target):
        num_values = len(target["values"])
        num_empties = target["values"].count("")
        return num_empties / num_values > error_rate

    def has_duplicates(target, ignore_empties):
        if ignore_empties:
            values = [v for v in target["values"] if v != ""]
        else:
            values = target["values"]
        distinct_values = set(values)
        return (len(values) - len(distinct_values)) > (error_rate * len(values))

    def get_datatype(target, dt_hierarchies):
        def is_match(datatype):
            # If the datatype has no associated condition then it matches anything:
            if not datatype.get("condition"):
                return True
            # If the SQL type is NULL this datatype is ruled out:
            sql_type = datatype.get("SQL type")
            if sql_type and sql_type.casefold() == "null":
                return False

            condition = get_compiled_condition(datatype["condition"], config["parser"])
            num_values = len(target["values"])
            num_passed = [condition(v) for v in target["values"]].count(True)
            success_rate = num_passed / num_values
            if (1 - success_rate) <= error_rate:
                return success_rate

        def tiebreak(datatypes):
            in_types = []
            other_types = []
            parents = set([dt["datatype"].get("parent") for dt in datatypes])
            parents.discard(None)
            for dt in datatypes:
                if dt["datatype"]["datatype"] not in parents:
                    if dt["datatype"].get("condition", "").lstrip().startswith("in("):
                        in_types.append(dt)
                    else:
                        other_types.append(dt)

            if len(in_types) == 1:
                return in_types[0]["datatype"]
            elif len(in_types) > 1:
                in_types = sorted(in_types, key=lambda k: k["success_rate"], reverse=True)
                return in_types[0]["datatype"]
            elif len(other_types) == 1:
                return other_types[0]["datatype"]
            elif len(other_types) > 1:
                other_types = sorted(other_types, key=lambda k: k["success_rate"], reverse=True)
                return other_types[0]["datatype"]
            else:
                print(f"Error tiebreaking datatypes: {pformat(datatypes)}")
                sys.exit(1)

        for depth in range(0, len(dt_hierarchies)):
            datatypes_to_check = [dt_hierarchies[depth][dt][0] for dt in dt_hierarchies[depth]]
            matching_datatypes = []
            for datatype in datatypes_to_check:
                success_rate = is_match(datatype)
                if success_rate:
                    matching_datatypes.append({"datatype": datatype, "success_rate": success_rate})

            if len(matching_datatypes) == 1:
                return matching_datatypes[0]["datatype"]
            elif len(matching_datatypes) > 1:
                return tiebreak(matching_datatypes)

    def get_from(target, potential_foreign_columns):
        candidate_froms = []
        for foreign in potential_foreign_columns:
            table = foreign["table"]
            column = foreign["column"]
            sql_type = foreign["sql_type"]
            num_matches = 0
            num_values = len(target["values"])
            for value in target["values"]:
                if target.get("nulltype") == "empty" and value == "":
                    # If this value is legitimately empty then it should not be taken into account
                    # when counting the number of values in the target that are found in the
                    # candidate foreign column:
                    num_values -= 1
                    continue
                if sql_type != "text" and not isinstance(value, Number):
                    # If this value is of the wrong type then there is no need to explicitly check
                    # if it exists in the foreign column:
                    continue
                if sql_type == "text":
                    value = f"'{value}'"
                sql = f'SELECT 1 FROM "{table}" WHERE "{column}" = {value} LIMIT 1'
                num_matches += len(config["db"].execute(sql).fetchall())
            if ((num_values - num_matches) / num_values) < error_rate:
                candidate_froms.append(f"from({foreign['table']}.{foreign['column']})")
        return candidate_froms

    target = sample[label]
    if has_nulltype(target):
        target["nulltype"] = "empty"

    # Use the valve config to retrieve the valve datatype hierarchies:
    dt_hierarchies = get_dt_hierarchies(config)
    target["datatype"] = get_datatype(target, dt_hierarchies)["datatype"]

    # Use the valve config to get a list of columns already loaded to the database, then compare
    # the contents of each column with the contents of the target column and possibly annotate the
    # target with a from() structure, if there is one and only one candidate from().
    potential_foreign_columns = get_potential_foreign_columns(config, target["datatype"])
    froms = get_from(target, potential_foreign_columns)
    if len(froms) == 1:
        target["structure"] = froms[0]
    elif len(froms) > 1:
        print(f"Column '{label}' has multiple from() candidates: {', '.join(froms)}")

    # Check if the column is a unique/primary column:
    if not target.get("structure"):
        if target.get("nulltype") is None and not has_duplicates(target, True):
            if is_primary_candidate:
                target["structure"] = "primary"
            else:
                target["structure"] = "unique"


if __name__ == "__main__":
    parser = ArgumentParser(description="VALVE guesser (prototype)")
    parser.add_argument("--verbose", action="store_true", help="Print logging output to STDERR.")
    parser.add_argument(
        "--sample_size",
        type=int,
        default=10000,
        help="Sample size to use when guessing (default: 10,000)",
    )
    parser.add_argument(
        "--error_rate",
        type=float,
        default=0.1,
        help="""A number between 0 and 1 (inclusive) representing the proportion of errors expected
        (default: 0.1)""",
    )
    parser.add_argument(
        "--enum_size",
        type=int,
        default=10,
        help="The maximum number of values to use for in(...) datatype conditions",
    )
    parser.add_argument(
        "--seed", type=int, help="Seed to use for random sampling (default: current epoch time)"
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Do not ask for confirmation before writing suggested modifications to the database",
    )
    parser.add_argument(
        "VALVE_TABLE", help="The VALVE table table from which to read the VALVE configuration"
    )
    parser.add_argument(
        "DATABASE",
        help="""Can be one of (A) A URL of the form `postgresql://...` or
        `sqlite://...` (B) The filename (including path) of a sqlite database.""",
    )
    parser.add_argument(
        "TABLE", help="A .TSV file containing the data for which we will be guessing"
    )
    args = parser.parse_args()

    VERBOSE = args.verbose

    # Use the seed argument, or the epoch time if no seed is given, to set up the random generator:
    if args.seed is not None:
        seed = args.seed
    else:
        seed = time.time_ns()
    random.seed(seed)

    # Get the valve configuration and database info:
    config = get_valve_config(args.VALVE_TABLE)
    table_tsv = args.TABLE
    table = Path(args.TABLE).stem
    if table in config["table"]:
        print(f"{table} is already configured.", file=sys.stderr)
        sys.exit(0)
    with sqlite3.connect(args.DATABASE) as conn:
        config["db"] = conn

    # Attach the condition parser to the config as well:
    config["parser"] = Lark(grammar, parser="lalr", transformer=TreeToDict())

    log(f"Getting random sample of {args.sample_size} rows from {table_tsv} ...")
    sample = get_random_sample(table_tsv, args.sample_size)
    for i, label in enumerate(sample):
        log(f"Annotating label '{label}' ...")
        annotate(label, sample, config, args.error_rate, i == 0)
    log("Done!")

    table_table_headers = ["table", "path", "type", "description"]
    column_table_headers = [
        "table",
        "column",
        "label",
        "nulltype",
        "datatype",
        "structure",
        "description",
    ]
    if not args.yes:
        print()

        print('The following row will be inserted to "table":')
        data = [table_table_headers, [f"{table}", f"{table_tsv}", "", ""]]
        # We add +2 for padding
        col_width = max(len(word) for row in data for word in row) + 2
        for row in data:
            print("".join(word.ljust(col_width) for word in row))

        print()

        print('The following row will be inserted to "column":')
        data = [column_table_headers]
        for label in sample:
            row = [
                f"{table}",
                f"{sample[label]['normalized']}",
                f"{label if label != sample[label]['normalized'] else ''}",
                f"{sample[label].get('nulltype', '')}",
                f"{sample[label]['datatype']}",
                f"{sample[label].get('structure', '')}",
                f"{sample[label].get('description', '')}",
            ]
            data.append(row)
        # We add +2 for padding
        col_width = max(len(word) for row in data for word in row) + 2
        for row in data:
            print("".join(word.ljust(col_width) for word in row))

        print()

        answer = input("Do you want to write this updated configuration to the database? (y/n) ")
        if answer.casefold() != "y":
            print("Not writing updated configuration to the database.")
            sys.exit(0)

    log("Updating table configuration in database ...")
    row_number = conn.execute('SELECT MAX(row_number) FROM "table"').fetchall()[0][0] + 1
    sql = dedent(
        f"""
    INSERT INTO "table" ("row_number", {', '.join([f'"{k}"' for k in table_table_headers])})
    VALUES ({row_number}, '{table}', '{table_tsv}', NULL, NULL)"""
    )
    log(sql, suppress_time=True)
    log("", suppress_time=True)
    conn.execute(sql)
    conn.commit()

    log("Updating column configuration in database ...")
    row_number = conn.execute('SELECT MAX(row_number) FROM "column"').fetchall()[0][0] + 1
    for label in sample:
        values = ", ".join(
            [
                f"{row_number}",
                f"'{table}'",
                f"'{sample[label]['normalized']}'",
                f"'{label}'" if label != sample[label]["normalized"] else "NULL",
                f"'{sample[label]['nulltype']}'" if sample[label].get("nulltype") else "NULL",
                f"'{sample[label]['datatype']}'",
                f"'{sample[label]['structure']}'" if sample[label].get("structure") else "NULL",
                f"'{sample[label]['description']}'" if sample[label].get("description") else "NULL",
            ]
        )
        sql = dedent(
            f"""
        INSERT INTO "column" ("row_number", {', '.join([f'"{k}"' for k in column_table_headers])})
        VALUES ({values})"""
        )
        log(sql, suppress_time=True)
        conn.execute(sql)
        conn.commit()
        row_number += 1
    log("", suppress_time=True)
    log("Done!")
