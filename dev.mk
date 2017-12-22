all: stop clean start enter

start:
	tarantoolctl start storage_1_a
	tarantoolctl start storage_1_b
	tarantoolctl start storage_2_a
	tarantoolctl start storage_2_b
	tarantoolctl start router_1

stop:
	tarantoolctl stop storage_1_a
	tarantoolctl stop storage_1_b
	tarantoolctl stop storage_2_a
	tarantoolctl stop storage_2_b
	tarantoolctl stop router_1

enter:
	tarantoolctl enter router_1

logcat:
	tail -f data/*.log

clean:
	rm -rf data/

test:
	test -d test/test-run || git submodule update --init --recursive
	cd test && ./test-run.py

.PHONY: console test deploy clean
