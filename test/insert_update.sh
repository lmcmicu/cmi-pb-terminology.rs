#!/usr/bin/env bash

if [[ $# -lt 2 ]]
then
    echo "Usage: $(basename $0) DATABASE TABLE_CONFIG"
    exit 1
fi

db=$1
table_defs=$2
shift 2
if [[ $# -gt 0 ]]
then
    echo "Warning: Extra arguments: '$*' will be ignored"
fi

export VALVE_SOURCE=${table_defs}
export VALVE_DATABASE=${db}

pwd=$(dirname $(readlink -f $0))
output_dir=$pwd/output
expected_dir=$pwd/expected

ret_value=0
for table_path in table2.tsv table3.tsv table6.tsv table10.tsv table11.tsv
do
    table_path=${table_path#test/}
    table_path=$pwd/output/$table_path
    table_file=$(basename $table_path)
    table=${table_file%.*}
    ./valve save --save-dir $output_dir $table
    diff -q $expected_dir/${table}.tsv ${table_path}
    ret_value=$(expr $ret_value + $?)
done

exit $ret_value
