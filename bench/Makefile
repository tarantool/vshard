start:
	ln -s -F node_template.lua node1.lua
	ln -s -F node_template.lua node2.lua
	ln -s -F node_template.lua node3.lua
	tarantoolctl start node1
	tarantoolctl start node2
	tarantoolctl start node3
	tarantoolctl start router

stop:
	tarantoolctl stop router
	tarantoolctl stop node1
	tarantoolctl stop node2
	tarantoolctl stop node3

clear: stop
	rm -rf data
	rm node1.lua node2.lua node3.lua
