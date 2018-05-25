test_run = require('test_run').new()

--
-- gh-110: Tarantool has a bug: when box.cfg{} is called with
-- no arguments, then
-- box.cfg{instance/replicaset_uuid = box.info.uuid/cluster.uuid}
-- does not work.
-- Vshard must not include these uuids in config when it is called
-- not the first time.
--
test_run:cmd("create server storage_1_1 with script='storage/storage_1_1.lua'")
test_run:cmd("start server storage_1_1")
test_run:switch('storage_1_1')
vshard.storage.cfg(cfg, instance_uuid)
test_run:switch('default')
test_run:cmd("stop server storage_1_1")
test_run:cmd("cleanup server storage_1_1")

test_run:cmd("create server storage_1_2 with script='storage/storage_1_2.lua'")
test_run:cmd("start server storage_1_2")
test_run:switch('storage_1_2')
util.check_error(vshard.storage.cfg, cfg, instance_uuid)
test_run:switch('default')
test_run:cmd("stop server storage_1_2")
test_run:cmd("cleanup server storage_1_2")

test_run:cmd("create server storage_1_3 with script='storage/storage_1_3.lua'")
test_run:cmd("start server storage_1_3")
test_run:switch('storage_1_3')
util.check_error(vshard.storage.cfg, cfg, instance_uuid)
test_run:switch('default')
test_run:cmd("stop server storage_1_3")
test_run:cmd("cleanup server storage_1_3")
