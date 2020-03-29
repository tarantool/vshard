start_storage:
	tarantoolctl start storage_1_1
	tarantoolctl start storage_2_1

start_router:
	tarantoolctl start router_1
	tarantoolctl start router_2
	echo "vshard.router.bootstrap({if_not_bootstrapped = true})" | \
		tarantoolctl enter router_1

start_client:
	tarantoolctl start client
	tarantoolctl enter client

start: start_storage start_router

stop:
	tarantoolctl stop client
	tarantoolctl stop router_1
	tarantoolctl stop router_2
	tarantoolctl stop storage_1_1
	tarantoolctl stop storage_2_1

clean:
	rm -rf data/
