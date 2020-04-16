start_storage:
	tarantoolctl start storage_1_1
	tarantoolctl start storage_2_1
	tarantoolctl start storage_3_1
	tarantoolctl start storage_4_1

start_router:
	tarantoolctl start router_01
	tarantoolctl start router_02
	tarantoolctl start router_03
	tarantoolctl start router_04
	tarantoolctl start router_05
	tarantoolctl start router_06
	tarantoolctl start router_07
	tarantoolctl start router_08
	tarantoolctl start router_09
	tarantoolctl start router_10
	tarantoolctl start router_11
	tarantoolctl start router_12
	tarantoolctl start router_13
	tarantoolctl start router_14
	tarantoolctl start router_15
	tarantoolctl start router_16

start_client:
	tarantoolctl start client_1
	tarantoolctl start client_2
	tarantoolctl start client_3
	tarantoolctl start client_4
	tarantoolctl enter client_1

start: start_storage start_router

stop:
	tarantoolctl stop client_1
	tarantoolctl stop client_2
	tarantoolctl stop client_3
	tarantoolctl stop client_4
	tarantoolctl stop router_01
	tarantoolctl stop router_02
	tarantoolctl stop router_03
	tarantoolctl stop router_04
	tarantoolctl stop router_05
	tarantoolctl stop router_06
	tarantoolctl stop router_07
	tarantoolctl stop router_08
	tarantoolctl stop router_09
	tarantoolctl stop router_10
	tarantoolctl stop router_11
	tarantoolctl stop router_12
	tarantoolctl stop router_13
	tarantoolctl stop router_14
	tarantoolctl stop router_15
	tarantoolctl stop router_16
	tarantoolctl stop storage_1_1
	tarantoolctl stop storage_2_1
	tarantoolctl stop storage_3_1
	tarantoolctl stop storage_4_1

clean:
	rm -rf data/
