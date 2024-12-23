MAKEFLAGS += --warn-undefined-variables
.DEFAULT_GOAL := valve
.DELETE_ON_ERROR:
.SUFFIXES:

# NOTE:
# -----
# The test targets assume that we have a postgresql server, accessible by the current user via the
# UNIX socket /var/run/postgresql, in which a database called `valve_postgres` has been created.
# They also requires that `psycopg2` has been installed.

.PHONY: build
build:
	mkdir -p build

.PHONY: doc
doc:
	cargo doc --document-private-items

.PHONY: readme
readme:
	cargo readme --no-title > README.md

valve: src/*.rs src/*.lalrpop
	@$(MAKE) valve_debug

.PHONY: valve_release
valve_release:
	rm -f valve
	cargo build --release
	ln -s target/release/ontodev_valve valve

.PHONY: valve_debug
valve_debug:
	rm -f valve
	cargo build
	ln -s target/debug/ontodev_valve valve

build/valve.db: valve | build test/output
	cp -f test/src/ontology/view1_sqlite.sql test/output/view1.sql
	cp -f test/src/ontology/view2_sqlite.sql test/output/view2.sql
	cp -f test/src/ontology/view2_sqlite.sh test/output/view2.sh
	cp -f test/src/ontology/readonly1_sqlite_load.sh test/output/readonly1.sh
	cp -f test/src/ontology/readonly3_sqlite_load.sql test/output/readonly3.sql
	chmod u+x test/output/readonly1.sh
	sqlite3 $@ < test/src/ontology/view3_sqlite.sql

test/output:
	mkdir -p test/output

tables_to_test := $(shell cut -f 1 test/src/table.tsv)
pg_connect_string := postgresql:///valve_postgres

.PHONY: test
test: sqlite_test pg_test

.PHONY: sqlite_test
sqlite_test: clean_test_db sqlite_load_test sqlite_api_test sqlite_cli_test sqlite_random_test

.PHONY: pg_test
pg_test: pg_load_test pg_api_test pg_cli_test pg_random_test

.PHONY: sqlite_load_test
sqlite_load_test: build/valve.db test/src/table.tsv | test/output
	@echo "Testing valve on sqlite ..."
	./valve --source test/src/table.tsv --database build/valve.db --assume-yes load-all
	test/round_trip.sh $^
	scripts/export_messages.py $< $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages.tsv test/output/messages.tsv
	scripts/export_messages.py --a1 $< $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages_a1.tsv test/output/messages.tsv
	# The "pk" test is run on table7 only since it is the only table whose primary keys are all valid:
	scripts/export_messages.py --pk $< $| table7
	diff --strip-trailing-cr -q test/expected/messages_pk.tsv test/output/messages.tsv
	@echo "Test succeeded!"

.PHONY: sqlite_api_test
sqlite_api_test: valve test/src/table.tsv build/valve.db test/insert_update.sh | test/output
	@echo "Testing API functions on sqlite ..."
	./$< --source $(word 2,$^) --database $(word 3,$^) --assume-yes test api
	$(word 4,$^) $(word 3,$^) $(word 2,$^)
	scripts/export_messages.py $(word 3,$^) $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages_after_api_test.tsv test/output/messages.tsv
	echo "select \"history_id\", \"table\", \"row\", \"from\", \"to\", \"summary\", \"user\", \"undone_by\" from history where history_id < 16 order by history_id" | sqlite3 -header -tabs build/valve.db > test/output/history.tsv
	diff --strip-trailing-cr -q test/expected/history.tsv test/output/history.tsv
	@echo "Test succeeded!"

.PHONY: sqlite_cli_test
sqlite_cli_test: valve test/src/table.tsv build/valve.db | test/output
	@echo "Testing CLI functions on sqlite ..."
	echo '{"parent": "text", "condition": "equals(custom_str)", "description": "", "sql_type": "TEXT"}' | \
		./$< --source $(word 2,$^) --database $(word 3,$^) --assume-yes --input JSON add datatype datatype1
	echo '{"table": "table1", "column": "extra_column", "datatype": "datatype1"}' | \
		./$< --source $(word 2,$^) --database $(word 3,$^) --assume-yes --input JSON add column table1
	./$< --source $(word 2,$^) --database $(word 3,$^) --assume-yes rename datatype datatype1 datatype1a
	./$< --source $(word 2,$^) --database $(word 3,$^) --assume-yes rename column table1 extra_column column_extra
	diff --strip-trailing-cr -q test/src/datatype.tsv test/expected/cli_test/datatype1.tsv
	diff --strip-trailing-cr -q test/src/column.tsv test/expected/cli_test/column1.tsv
	diff --strip-trailing-cr -q test/src/ontology/table1.tsv test/expected/cli_test/table1_1.tsv
	./$< --source $(word 2,$^) --database $(word 3,$^) --assume-yes delete column table1 column_extra
	./$< --source $(word 2,$^) --database $(word 3,$^) --assume-yes delete datatype datatype1a
	diff --strip-trailing-cr -q test/src/datatype.tsv test/expected/cli_test/datatype2.tsv
	diff --strip-trailing-cr -q test/src/column.tsv test/expected/cli_test/column2.tsv
	diff --strip-trailing-cr -q test/src/ontology/table1.tsv test/expected/cli_test/table1_2.tsv
	./$< --source $(word 2,$^) --database $(word 3,$^) --assume-yes rename table table1 tableOne
	diff --strip-trailing-cr -q test/src/column.tsv test/expected/cli_test/column3.tsv
	diff --strip-trailing-cr -q test/src/table.tsv test/expected/cli_test/table3.tsv
	./$< --source $(word 2,$^) --database $(word 3,$^) --assume-yes undo
	diff --strip-trailing-cr -q test/src/datatype.tsv test/expected/cli_test/datatype2.tsv
	diff --strip-trailing-cr -q test/src/column.tsv test/expected/cli_test/column2.tsv
	diff --strip-trailing-cr -q test/src/ontology/table1.tsv test/expected/cli_test/table1_2.tsv
	# We drop all of the db tables because the schema for the next test (random test) is different
	# from the schema used for this test.
	./$<  --source $(word 2,$^) --database $(word 3,$^) --assume-yes drop-all
	@echo "Test succeeded!"

.PHONY: pg_load_test
pg_load_test: valve test/src/table.tsv | test/output
	@echo "Testing valve on postgresql ..."
	cp -f test/src/ontology/view1_postgresql.sql test/output/view1.sql
	cp -f test/src/ontology/view2_postgresql.sql test/output/view2.sql
	cp -f test/src/ontology/view2_postgresql.sh test/output/view2.sh
	cp -f test/src/ontology/readonly1_postgresql_load.sh test/output/readonly1.sh
	cp -f test/src/ontology/readonly3_postgresql_load.sql test/output/readonly3.sql
	chmod u+x test/output/readonly1.sh
	psql $(pg_connect_string) < test/src/ontology/view3_postgresql.sql
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes load-all
	test/round_trip.sh $(pg_connect_string) $(word 2,$^)
	scripts/export_messages.py $(pg_connect_string) $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages.tsv test/output/messages.tsv
	scripts/export_messages.py --a1 $(pg_connect_string) $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages_a1.tsv test/output/messages.tsv
	# The "pk" test is run on table7 only since it is the only table whose primary keys are all valid:
	scripts/export_messages.py --pk $(pg_connect_string) $| table7
	diff --strip-trailing-cr -q test/expected/messages_pk.tsv test/output/messages.tsv
	@echo "Test succeeded!"

.PHONY: pg_api_test
pg_api_test: valve test/src/table.tsv test/insert_update.sh | test/output
	@echo "Testing API functions on postgresql ..."
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes load-all
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes test api
	$(word 3,$^) $(pg_connect_string) $(word 2,$^)
	scripts/export_messages.py $(pg_connect_string) $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages_after_api_test.tsv test/output/messages.tsv
	psql $(pg_connect_string) -c "COPY (select \"history_id\", \"table\", \"row\", \"from\", \"to\", \"summary\", \"user\", \"undone_by\" from history where history_id < 16 order by history_id) TO STDOUT WITH NULL AS ''" > test/output/history.tsv
	tail -n +2 test/expected/history.tsv | diff --strip-trailing-cr -q test/output/history.tsv -
	@echo "Test succeeded!"

.PHONY: pg_cli_test
pg_cli_test: valve test/src/table.tsv pg_test | test/output
	@echo "Testing CLI functions on postgresql ..."
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes load-all
	echo '{"parent": "text", "condition": "equals(custom_str)", "description": "", "sql_type": "TEXT"}' | \
		./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes --input JSON add datatype datatype1
	echo '{"table": "table1", "column": "extra_column", "datatype": "datatype1"}' | \
		./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes --input JSON add column table1
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes rename datatype datatype1 datatype1a
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes rename column table1 extra_column column_extra
	diff --strip-trailing-cr -q test/src/datatype.tsv test/expected/cli_test/datatype1.tsv
	diff --strip-trailing-cr -q test/src/column.tsv test/expected/cli_test/column1.tsv
	diff --strip-trailing-cr -q test/src/ontology/table1.tsv test/expected/cli_test/table1_1.tsv
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes delete column table1 column_extra
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes delete datatype datatype1a
	diff --strip-trailing-cr -q test/src/datatype.tsv test/expected/cli_test/datatype2.tsv
	diff --strip-trailing-cr -q test/src/column.tsv test/expected/cli_test/column2.tsv
	diff --strip-trailing-cr -q test/src/ontology/table1.tsv test/expected/cli_test/table1_2.tsv
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes rename table table1 tableOne
	diff --strip-trailing-cr -q test/src/column.tsv test/expected/cli_test/column3.tsv
	diff --strip-trailing-cr -q test/src/table.tsv test/expected/cli_test/table3.tsv
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes undo
	diff --strip-trailing-cr -q test/src/datatype.tsv test/expected/cli_test/datatype2.tsv
	diff --strip-trailing-cr -q test/src/column.tsv test/expected/cli_test/column2.tsv
	diff --strip-trailing-cr -q test/src/ontology/table1.tsv test/expected/cli_test/table1_2.tsv
	# We drop all of the db tables because the schema for the next test (random test) is different
	# from the schema used for this test.
	./$< --source $(word 2,$^) --database $(pg_connect_string) --assume-yes drop-all
	@echo "Test succeeded!"

sqlite_random_db = build/valve_random.db
random_test_dir = test/random_test_data

$(random_test_dir)/ontology:
	mkdir -p $@

.PHONY: random_test_data
random_test_data: test/generate_random_test_data.py valve valve test/random_test_data/table.tsv | $(random_test_dir)/ontology
	./$< $$(date +"%s") 100 5 $(word 3,$^) $|

.PHONY: sqlite_random_test
sqlite_random_test: valve clean_random_data random_test_data | build test/output
	@echo "Testing with random data on sqlite ..."
	./$< --source $(random_test_dir)/table.tsv --database $(sqlite_random_db) --assume-yes load-all
	test/round_trip.sh $(sqlite_random_db) $(random_test_dir)/table.tsv
	@echo "Test succeeded!"

.PHONY: pg_random_test
pg_random_test: valve clean_random_data random_test_data | build test/output
	@echo "Testing with random data on postgresql ..."
	./$< --source $(random_test_dir)/table.tsv --database $(pg_connect_string) --assume-yes load-all
	test/round_trip.sh $(pg_connect_string) $(random_test_dir)/table.tsv
	@echo "Test succeeded!"

test/penguins/src/data:
	mkdir -p $@

# At last check, the penguin performance test was running on GitHub's runner
# (Ubuntu 22.04.4 LTS, runner version 2.317.0) in just under 30s. GitHub
# sometimes changes the runner version, however, thus if we set the threshold
# too low we might get a failure. The threshold below is about 10s more than the time
# it takes on my laptop (while plugged).
penguin_test_threshold = 50
num_penguin_rows = 100000
penguin_command_sqlite = ./valve --source src/schema/table.tsv --database penguins.db --assume-yes load-all
penguin_command_pg = ./valve --source src/schema/table.tsv --database $(pg_connect_string) --assume-yes load-all
penguin_command_pg_drop = ./valve --source src/schema/table.tsv --database $(pg_connect_string)  --assume-yes drop-all

.PHONY: penguin_test
penguin_test: valve | test/penguins/src/data
	@echo "Running penguin test ..."
	rm -f test/penguins/penguins.db
	cd test/penguins && ./generate.py $(num_penguin_rows)
	cd test/penguins && ln -f -s ../../target/debug/ontodev_valve valve
	@echo "cd test/penguins && $(penguin_command_sqlite)"
	@cd test/penguins && \
		timeout $(penguin_test_threshold) \
		time -p \
		$(penguin_command_sqlite) || \
		(echo "Penguin test (SQLite) took longer than $(penguin_test_threshold) seconds." && false)
	cd test/penguins && $(penguin_command_pg_drop)
	@echo "cd test/penguins && $(penguin_command_pg)"
	@cd test/penguins && \
		timeout $(penguin_test_threshold) \
		time -p \
		$(penguin_command_pg) || \
		(echo "Penguin test (PostgreSQL) took longer than $(penguin_test_threshold) seconds." && false)
	@echo "Test succeeded!"

guess_test_dir = test/guess_test_data
guess_test_db = build/valve_guess.db
num_guess_test_rows = 30000

$(guess_test_dir)/table1.tsv: test/generate_random_test_data.py valve $(guess_test_dir)/*.tsv
	./$< 0 $(num_guess_test_rows) 5 $(guess_test_dir)/table.tsv $(guess_test_dir)

$(guess_test_dir)/ontology:
	mkdir -p $@

.PHONY: guess_test_data
guess_test_data: test/generate_random_test_data.py $(guess_test_dir)/table1.tsv valve confirm_overwrite.sh $(guess_test_dir)/*.tsv | $(guess_test_dir)/ontology
	./confirm_overwrite.sh $(guess_test_dir)/ontology
	rm -f $(guess_test_dir)/table1.tsv
	./$< 0 $(num_guess_test_rows) 5 $(guess_test_dir)/table.tsv $(guess_test_dir)
	rm -f $(guess_test_dir)/ontology/*.tsv
	./$< 0 $(num_guess_test_rows) 5 $(guess_test_dir)/table_expected.tsv $|
	rm -f $(guess_test_dir)/ontology/table1.tsv

$(guess_test_db): valve guess_test_data $(guess_test_dir)/*.tsv | build $(guess_test_dir)/ontology
	rm -f $@
	./$< --source $(guess_test_dir)/table.tsv --database $@ --assume-yes load-all

# At last check, the performance test was running on GitHub's runner
# (Ubuntu 22.04.4 LTS, runner version 2.317.0) in just over 20s. GitHub
# sometimes changes the runner version, however, thus if we set the threshold
# too low we might get a failure. The threshold below is about 10s more than the time
# it takes using postgresql on my laptop (while plugged), and about 15s more than it takes
# using sqlite.
perf_test_threshold = 45
perf_test_dir = test/perf_test_data
perf_test_db = build/valve_perf.db
num_perf_test_rows = 10000
perf_test_error_rate = 5

$(perf_test_dir)/ontology:
	mkdir -p $@

.PHONY: perf_test_data
perf_test_data: test/generate_random_test_data.py valve confirm_overwrite.sh $(perf_test_dir)/*.tsv | $(perf_test_dir)/ontology
	./confirm_overwrite.sh $(perf_test_dir)/ontology
	rm -f $(perf_test_dir)/ontology/*.tsv
	./$< 0 $(num_perf_test_rows) $(perf_test_error_rate) $(perf_test_dir)/table.tsv $|

$(perf_test_db): valve perf_test_data $(perf_test_dir)/*.tsv | build $(perf_test_dir)/ontology
	rm -f $@
	timeout $(perf_test_threshold) time -p ./$< --source $(perf_test_dir)/table.tsv --database $@ --assume-yes --verbose load-all || \
		(echo "Performance test (SQLite) took longer than $(perf_test_threshold) seconds." && false)


.PHONY: sqlite_perf_test
sqlite_perf_test: $(perf_test_db) | test/output
	time -p scripts/export_messages.py $< $| $(tables_to_test)

.PHONY: pg_perf_test
pg_perf_test: valve $(perf_test_dir)/ontology | test/output
	timeout $(perf_test_threshold) time -p ./$< --source $(perf_test_dir)/table.tsv --database $(pg_connect_string) --assume-yes --verbose load-all || \
        (echo "Performance test (PostgreSQL) took longer than $(perf_test_threshold) seconds." && false)
	time -p scripts/export_messages.py $(pg_connect_string) $| $(tables_to_test)

.PHONY: perf_test
perf_test: sqlite_perf_test pg_perf_test

.PHONY: clean
clean:
	rm -Rf build/valve.db* build/valve_random.db* test/output $(random_test_dir)/ontology valve

.PHONY: clean_random_data
clean_random_data:
	rm -Rf $(random_test_dir)/ontology

.PHONY: clean_test_db
clean_test_db:
	rm -Rf build/valve.db

.PHONY: clean_guess_db
clean_guess_db:
	rm -Rf build/valve_guess.db

.PHONY: clean_guess_data
clean_guess_data:
	rm -Rf $(guess_test_dir)/table1.tsv $(guess_test_dir)/ontology

.PHONY: clean_perf_db
clean_perf_db:
	rm -Rf build/valve_perf.db

.PHONY: clean_perf_data
clean_perf_data:
	rm -Rf $(perf_test_dir)/ontology

.PHONY: cleanall
cleanall: clean clean_perf_db clean_perf_data clean_guess_db clean_guess_data clean_pg
	cargo clean
	rm -f valve
