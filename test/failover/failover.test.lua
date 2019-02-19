test_run = require('test_run').new()

REPLICASET_1 = { 'box_1_a', 'box_1_b', 'box_1_c', 'box_1_d' }
REPLICASET_2 = { 'box_2_a', 'box_2_b', 'box_2_c' }
REPLICASET_3 = { 'box_3_a', 'box_3_b' }

test_run:create_cluster(REPLICASET_1, 'failover')
test_run:create_cluster(REPLICASET_2, 'failover')
test_run:create_cluster(REPLICASET_3, 'failover')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'box_1_a')
util.wait_master(test_run, REPLICASET_2, 'box_2_a')
util.wait_master(test_run, REPLICASET_3, 'box_3_b')

test_run:cmd("setopt delimiter ';'")
function create_router(name)
    test_run:cmd('create server '..name..' with script="failover/'..name..'.lua"')
    test_run:cmd('start server '..name)
end;
function kill_router(name)
    test_run:cmd('stop server '..name)
    test_run:cmd('cleanup server '..name)
end;
test_run:cmd("setopt delimiter ''");

test_run:cmd('switch box_1_a')
for i = 1, 30 do box.space._bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:cmd('switch box_2_a')
for i = 31, 60 do box.space._bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:cmd('switch box_3_b')
for i = 61, 90 do box.space._bucket:replace{i, vshard.consts.BUCKET.ACTIVE} end

test_run:cmd('switch default')

--
-- On router 1 test the following things:
-- * correct priority order (order of zones for router_1 from
--   zone 1);
-- * use nearest replica for 'read' requests instead of master;
-- * down nearest replica priority if a current connection is not
--   available;
-- * up nearest replica priority if the best one is available
--   again;
-- * replicaset uses master connection, if the nearest's one is
--   not available before call();
-- * current nearest connection is not down, when trying to
--   connect to the replica with less weight.
--
-- On other routers test priority order only.
--
create_router('router_1')
test_run:switch('router_1')
vshard.router.cfg(cfg)
while not test_run:grep_log('router_1', 'New replica box_1_d%(storage%@') do fiber.sleep(0.1) end
priority_order()
vshard.router.bucket_discovery(1).uuid == rs_uuid[1]
vshard.router.bucket_discovery(31).uuid == rs_uuid[2]
vshard.router.bucket_discovery(61).uuid == rs_uuid[3]
vshard.router.call(1, 'read', 'echo', {123})
test_run:switch('box_1_d')
-- Not 0 - 'read' echo was called here.
echo_count
test_run:switch('box_1_a')
-- 0 - 'read' echo was called not on a master, but on the nearest
-- server.
echo_count
test_run:switch('router_1')
-- Write requests still are beeing sent to master.
vshard.router.call(1, 'write', 'echo', {123})
test_run:switch('box_1_a')
echo_count

-- Ensure that replica_up_ts is updated periodically.
test_run:switch('router_1')
rs1 = vshard.router.static.replicasets[rs_uuid[1]]
while not rs1.replica_up_ts do fiber.sleep(0.1) end
old_up_ts = rs1.replica_up_ts
while rs1.replica_up_ts == old_up_ts do fiber.sleep(0.1) end
rs1.replica_up_ts - old_up_ts >= vshard.consts.FAILOVER_UP_TIMEOUT

-- Down box_1_d to trigger down replica priority to box_1_b.
-- After box_1_b becomes replica, revive box_1_d (best read
-- replica). The correct failover fiber must reconnect back to
-- box_1_d.
test_run:cmd('stop server box_1_d')
-- Down_ts must be set in on_disconnect() trigger.
while rs1.replica.down_ts == nil do fiber.sleep(0.1) end
-- Try to execute read-only request - it must use master
-- connection, because a replica's one is not available.
vshard.router.call(1, 'read', 'echo', {123})
test_run:switch('box_1_a')
echo_count
test_run:switch('router_1')
-- New replica is box_1_b.
while rs1.replica.name ~= 'box_1_b' do fiber.sleep(0.1) end
rs1.replica.down_ts == nil
rs1.replica_up_ts ~= nil
test_run:grep_log('router_1', 'New replica box_1_b%(storage%@')
-- gh-69: ensure callro() goes to a replica.
vshard.router.callro(1, 'echo', {123})
test_run:cmd('switch box_1_b')
-- Ensure the 'read' echo was executed on box_1_b - nearest
-- available replica.
echo_count
test_run:switch('router_1')

-- Revive the best replica. A router must reconnect to it in
-- FAILOVER_UP_TIMEOUT seconds.
test_run:cmd('start server box_1_d')
ts1 = fiber.time()
while rs1.replica.name ~= 'box_1_d' do fiber.sleep(0.1) end
ts2 = fiber.time()
ts2 - ts1 < vshard.consts.FAILOVER_UP_TIMEOUT
test_run:grep_log('router_1', 'New replica box_1_d%(storage%@')

-- Ensure the master connection is used as replica's one instead
-- of creation of a new connection to the same host.
test_run:cmd('stop server box_1_b')
test_run:cmd('stop server box_1_c')
test_run:cmd('stop server box_1_d')
while rs1.replica.name ~= 'box_1_a' do fiber.sleep(0.1) end
rs1.replica.conn == rs1.master.conn

test_run:cmd('start server box_1_b with wait=False, wait_load=False')
test_run:cmd('start server box_1_c with wait=False, wait_load=False')
test_run:cmd('start server box_1_d with wait=False, wait_load=False')
while rs1.replica.name ~= 'box_1_d' do fiber.sleep(0.1) end
-- Replica's connection has been changed, but the master was not
-- closed regardless of that they were equal.
rs1.master.conn:is_connected()

test_run:switch('default')

create_router('router_2')
test_run:switch('router_2')
vshard.router.cfg(cfg)
priority_order()
vshard.router.bucket_discovery(1).uuid == rs_uuid[1]
vshard.router.bucket_discovery(31).uuid == rs_uuid[2]
vshard.router.bucket_discovery(61).uuid == rs_uuid[3]
test_run:switch('default')

create_router('router_3')
test_run:switch('router_3')
vshard.router.cfg(cfg)
priority_order()
vshard.router.bucket_discovery(1).uuid == rs_uuid[1]
vshard.router.bucket_discovery(31).uuid == rs_uuid[2]
vshard.router.bucket_discovery(61).uuid == rs_uuid[3]
test_run:switch('default')

create_router('router_4')
test_run:switch('router_4')
vshard.router.cfg(cfg)
priority_order()
vshard.router.bucket_discovery(1).uuid == rs_uuid[1]
vshard.router.bucket_discovery(31).uuid == rs_uuid[2]
vshard.router.bucket_discovery(61).uuid == rs_uuid[3]

--
-- gh-169: do not close connections on too long ping when this is
-- because of too big response.
--
test_run:switch('router_1')
log.info(string.rep('padding', 200))
cfg.failover_ping_timeout = 0.0000001
vshard.router.cfg(cfg)
while not test_run:grep_log('router_1', 'Ping error from', 1000) do fiber.sleep(0.01) end

t = string.rep('a', 1024 * 1024 * 500)
rs = vshard.router.route(31)
while rs.master ~= rs.replica do fiber.sleep(0.01) end
future = nil
while not future do fiber.sleep(0.01) future = vshard.router.callrw(31, 'echo', {t}, {is_async = true}) end
vshard.router.static.failover_fiber:wakeup()
res, err = future:wait_result(5)
err
#res[1]
cfg.failover_ping_timeout = nil
vshard.router.cfg(cfg)

test_run:switch('default')
kill_router('router_1')
kill_router('router_2')
kill_router('router_3')
kill_router('router_4')
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)
test_run:drop_cluster(REPLICASET_3)
