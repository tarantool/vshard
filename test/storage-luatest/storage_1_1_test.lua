local t = require('luatest')
local vconst = require('vshard.consts')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local group_config = {{engine = 'memtx'}, {engine = 'vinyl'}}

if vutil.feature.memtx_mvcc then
    table.insert(group_config, {
        engine = 'memtx', memtx_use_mvcc_engine = true
    })
    table.insert(group_config, {
        engine = 'vinyl', memtx_use_mvcc_engine = true
    })
end

local test_group = t.group('storage', group_config)

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
            },
        },
        {
            replicas = {
                replica_2_a = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 10,
    replication_timeout = 0.1,
}
local global_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

--
-- Test how bucket_send preserves tuple field types over the network (gh-327).
--
test_group.test_bucket_send_field_types = function(g)
    -- Make a space with all the field types whose msgpack representation is
    -- nontrivial.
    local _, err = vtest.cluster_exec_each_master(g, function(engine)
        local format = {
            {'id', 'unsigned'},
            {'bid', 'unsigned'},
        }
        local i = 1
        local tuple = {i, box.NULL}
        -- Decimal appeared earlier, but this is when it can be used as a field
        -- type. Same logic with other types.
        if ivutil.version_is_at_least(2, 3, 0, nil, 0, 0) then
            ivutil.table_extend(format, {
                {'fdecimal', 'decimal'},
            })
            i = i + 1
            table.insert(tuple, require('decimal').new(i))
        end
        if ivutil.version_is_at_least(2, 4, 0, nil, 0, 0) then
            ivutil.table_extend(format, {
                {'fuuid', 'uuid'},
            })
            i = i + 1
            table.insert(tuple, ivtest.uuid_from_int(i))
        end
        if ivutil.version_is_at_least(2, 10, 0, nil, 0, 0) then
            ivutil.table_extend(format, {
                {'fdatetime', 'datetime'},
                {'finterval', 'interval'},
                {'fdouble', 'double'},
                {'fbinary', 'varbinary'},
            })
            local dt = require('datetime')
            i = i + 1
            table.insert(tuple, dt.new({year = i}))
            i = i + 1
            table.insert(tuple, dt.interval.new({year = i}))
            i = i + 1
            table.insert(tuple, require('ffi').cast('double', i))
            -- That is the simplest way to get an MP_BIN in Lua without FFI.
            i = i + 1
            local value = box.execute([[SELECT CAST(? AS VARBINARY)]],
                                      {tostring(i)}).rows[1]
            value = imsgpack.object(value):iterator()
            -- Skip tuple's MP_ARRAY header.
            value:decode_array_header()
            -- Get first and only field - MP_BIN.
            table.insert(tuple, value:take())
        end
        local s = box.schema.create_space('test', {
            engine = engine,
            format = format,
        })
        s:create_index('pk')
        s:create_index('bucket_id', {unique = false, parts = {2}})
        rawset(_G, 'test_tuple', tuple)
    end, {g.params.engine})
    t.assert_equals(err, nil, 'space creation no error')

    -- Send the bucket with a complicated tuple.
    local bid = g.replica_1_a:exec(function(dst)
        local bid = _G.get_first_bucket()
        local tuple = _G.test_tuple
        tuple[2] = bid
        box.space.test:replace(tuple)
        local ok, err = ivshard.storage.bucket_send(bid, dst,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil, 'bucket_send no error')
        ilt.assert(ok, 'bucket_send ok')
        return bid
    end, {g.replica_2_a:replicaset_uuid()})

    -- Ensure the tuple is delivered as is and fits into the space's format.
    g.replica_2_a:exec(function(bid)
        local src_tuple = _G.test_tuple
        src_tuple[2] = bid
        local dst_tuple = box.space.test:get{src_tuple[1]}
        -- Comparison unfortunately can only be done in Lua. Msgpack objects are
        -- incomparable which means the original MP_BIN wouldn't be equal to
        -- anything. But that should be safe anyway if the tuple managed to fit
        -- into the space.
        dst_tuple = dst_tuple:totable()
        src_tuple = box.tuple.new(src_tuple):totable()
        ilt.assert_equals(dst_tuple, src_tuple, 'tuple is delivered as is')
    end, {bid})

    -- Cleanup.
    g.replica_1_a:exec(function()
        _G.bucket_gc_wait()
    end)

    g.replica_2_a:exec(function(bid, dst)
        box.space.test:truncate()
        local ok, err = ivshard.storage.bucket_send(bid, dst,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil, 'bucket_send no error')
        ilt.assert(ok, 'bucket_send ok')
        _G.bucket_gc_wait()
    end, {bid, g.replica_1_a:replicaset_uuid()})

    vtest.cluster_exec_each_master(g, function()
        box.space.test:drop()
    end)
end

--
-- Custom triggers on bucket changes.
--
test_group.test_on_bucket_event = function(g)
    vtest.cluster_exec_each_master(g, function(engine)
        local event_space = box.schema.space.create('events', {
            engine = engine,
        })
        event_space:create_index('primary')

        local function data_space_new(name)
            local s = box.schema.space.create(name, {
                engine = engine,
                format = {
                    {'id', 'unsigned'},
                    {'bucket_id', 'unsigned'},
                },
            })
            s:create_index('id', {parts = {'id'}})
            s:create_index('bucket_id', {
                parts = {'bucket_id'}, unique = false
            })
        end
        data_space_new('data1')
        data_space_new('data2')

        local test_txn_id = 0
        local test_events = {}
        local test_commits = {}
        rawset(_G, 'test_trigger', function(event_type, bucket_id, data)
            test_txn_id = test_txn_id + 1
            local tid = test_txn_id
            local event_data = {event_type, bucket_id, data}
            ilt.assert(box.is_in_txn())
            ilt.assert(not test_events[tid])
            event_space:insert{tid, event_data}
            test_events[tid] = event_data

            box.on_commit(function(iterator)
                for _, _, _, space_id in iterator() do
                    if space_id == event_space.id then
                        test_commits[tid] = true
                        break
                    end
                end
            end)
        end)
        ivshard.storage.on_bucket_event(_G.test_trigger)

        rawset(_G, 'process_events', function(expected_events)
            -- Compare only the items, not looking at the order. Because the
            -- spaces can be sent in any order, can't rely on one.
            ilt.assert_items_equals(test_events, expected_events)
            -- Ensure that each saved event is also persisted.
            for tid, event_data in pairs(test_events) do
                ilt.assert_equals(box.space.events:get{tid}, {tid, event_data})
                ilt.assert_not_equals(test_commits[tid], nil)
                test_commits[tid] = nil
                box.space.events:delete{tid}
            end
            test_events = {}
            -- No unexpected events anywhere.
            ilt.assert(not next(test_commits))
            ilt.assert_equals(box.space.events.index[0]:min(), nil)
        end)
    end, {g.params.engine})

    local rs1_uuid = g.replica_1_a:replicaset_uuid()
    local bid = g.replica_2_a:exec(function(rs1_uuid)
        local bid = _G.get_first_bucket()
        box.space.data1:insert({10, bid})
        box.space.data2:insert({20, bid})
        local ok, err = ivshard.storage.bucket_send(
            bid, rs1_uuid, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
        return bid
    end, {rs1_uuid})

    g.replica_2_a:exec(function(events)
        _G.process_events(events)
    end, {{
        [1] = {vconst.BUCKET_EVENT.GC, bid, {spaces = {'data1'}}},
        [2] = {vconst.BUCKET_EVENT.GC, bid, {spaces = {'data2'}}},
    }})

    g.replica_1_a:exec(function(events)
        _G.process_events(events)
    end, {{
        [1] = {vconst.BUCKET_EVENT.RECV, bid, {spaces = {'data1'}}},
        [2] = {vconst.BUCKET_EVENT.RECV, bid, {spaces = {'data2'}}},
    }})

    -- Cleanup
    vtest.cluster_exec_each_master(g, function()
        ivshard.storage.on_bucket_event(nil, _G.test_trigger)
        box.space.events:drop()
        box.space.data1:drop()
        box.space.data2:drop()
    end)
end

--
-- gh-434: bucket_send() shouldn't change the transfer flags if a transfer for
-- the same bucket is already in progress.
--
test_group.test_bucket_double_send = function(g)
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = true
    end)
    local bid = g.replica_1_a:exec(function(uuid)
        local transfer_flags =
            ivshard.storage.internal.rebalancer_transfering_buckets
        local bid = _G.get_first_bucket()
        local f = ifiber.create(ivshard.storage.bucket_send, bid, uuid,
                                {timeout = 100000})
        f:set_joinable(true)
        rawset(_G, 'test_f', f)
        ilt.assert(transfer_flags[bid])
        local ok, err = ivshard.storage.bucket_send(bid, uuid)
        ilt.assert_equals(err.code, iverror.code.WRONG_BUCKET)
        ilt.assert(not ok)
        -- Before the bug was fixed, the second send would clear the flag, thus
        -- leaving the first sending unprotected.
        ilt.assert(transfer_flags[bid])
        return bid
    end, {g.replica_2_a:replicaset_uuid()})
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = false
    end)
    g.replica_1_a:exec(function(bid)
        local transfer_flags =
            ivshard.storage.internal.rebalancer_transfering_buckets
        local f = _G.test_f
        _G.test_f = nil
        local f_ok, ok, err = f:join(iwait_timeout)
        ilt.assert_equals(err, nil)
        ilt.assert(f_ok)
        ilt.assert(ok)
        ilt.assert(not transfer_flags[bid])
        _G.bucket_gc_wait()
    end, {bid})
    --
    -- Cleanup.
    --
    g.replica_2_a:exec(function(bid, uuid)
        local ok, err = ivshard.storage.bucket_send(bid, uuid,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, {bid, g.replica_1_a:replicaset_uuid()})
end

--
-- gh-434: bucket_recv() shouldn't change the transfer flags if a transfer for
-- the same bucket is already in progress.
--
test_group.test_bucket_double_recv = function(g)
    g.replica_2_a:exec(function(bid, uuid)
        local transfer_flags =
            ivshard.storage.internal.rebalancer_transfering_buckets
        local f = ifiber.create(ivshard.storage.bucket_recv, bid, uuid, {})
        f:set_joinable(true)
        ilt.assert(transfer_flags[bid])
        local ok, err = ivshard.storage.bucket_recv(bid, uuid, {})
        -- Before the bug was fixed, the second recv would clear the flag, thus
        -- leaving the first recv unprotected.
        ilt.assert(transfer_flags[bid])
        ilt.assert_equals(err.code, iverror.code.WRONG_BUCKET)
        ilt.assert(not ok)
        local f_ok
        f_ok, ok, err = f:join(iwait_timeout)
        ilt.assert(not transfer_flags[bid])
        ilt.assert_equals(err, nil)
        ilt.assert(f_ok)
        ilt.assert(ok)
        --
        -- Cleanup.
        --
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
        ilt.assert_equals(box.space._bucket:get{bid}, nil)
    end, {vtest.storage_first_bucket(g.replica_1_a),
          g.replica_1_a:replicaset_uuid()})
end

--
-- gh-433: SENT bucket could be deleted by GC before bucket_send() is completed.
-- It could lead to a problem that bucket_send() would notice that in the end
-- and treat as an error. The bucket should not be deleted while its transfer is
-- still in progress.
--
test_group.test_bucket_send_last_step_gc = function(g)
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = true
    end)
    local bid = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        local f = ifiber.create(ivshard.storage.bucket_send, bid, uuid,
                                {timeout = 1000000})
        f:set_joinable(true)
        rawset(_G, 'test_f', f)
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            ilt.assert_equals(box.space._bucket:get{bid}.status,
                              ivconst.BUCKET.SENT)
        end)
        ivtest.service_wait_for_new_ok(ivshard.storage.internal.gc_service, {
            on_yield = ivshard.storage.garbage_collector_wakeup
        })
        ilt.assert_equals(box.space._bucket:get{bid}.status,
                          ivconst.BUCKET.SENT)
        ilt.assert_equals(ivshard.storage._call('bucket_test_gc', {bid}),
                          {bids_not_ok = {bid}})
        return bid
    end, {g.replica_2_a:replicaset_uuid()})
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_LAST_RECEIVE_DELAY = false
    end)
    g.replica_1_a:exec(function()
        local f = _G.test_f
        _G.test_f = nil
        local f_ok, ok, err = f:join()
        ilt.assert(f_ok)
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end)
    g.replica_2_a:exec(function(bid, uuid)
        local ok, err = ivshard.storage.bucket_send(bid, uuid)
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, {bid, g.replica_1_a:replicaset_uuid()})
end

--
-- Recovery shouldn't touch SENDING bucket whose transfer is in progress, even
-- if the target instance doesn't have the same RECEIVING bucket. The first
-- bucket_recv() could just not arrive yet or still wasn't processed.
--
test_group.test_bucket_recovery_quick_after_send_is_started = function(g)
    g.replica_2_a:exec(function()
        rawset(_G, 'test_recv_f', ivshard.storage.bucket_recv)
        rawset(_G, 'test_is_recv_blocked', true)
        ivshard.storage.bucket_recv = function(...)
            while _G.test_is_recv_blocked do
                ifiber.sleep(0.01)
            end
            return _G.test_recv_f(...)
        end
    end)
    local bid = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        local f = ifiber.new(ivshard.storage.bucket_send, bid, uuid,
                             {timeout = iwait_timeout})
        f:set_joinable(true)
        rawset(_G, 'test_f', f)
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            assert(box.space._bucket:get{bid}.status == ivconst.BUCKET.SENDING)
        end)
        ivtest.service_wait_for_new_status(
            ivshard.storage.internal.recovery_service, 'recovering', {
                on_yield = ivshard.storage.recovery_wakeup,
                timeout = iwait_timeout
            })
        ilt.assert_equals(box.space._bucket:get{bid}.status,
                          ivconst.BUCKET.SENDING)
        return bid
    end, {g.replica_2_a:replicaset_uuid()})
    g.replica_2_a:exec(function()
        _G.test_is_recv_blocked = false
    end)
    g.replica_1_a:exec(function()
        local f = _G.test_f
        _G.test_f = nil
        local f_ok, ok, err = f:join()
        ilt.assert(f_ok)
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end)
    --
    -- Cleanup.
    --
    g.replica_2_a:exec(function(bid, uuid)
        _G.test_is_recv_blocked = nil
        ivshard.storage.bucket_recv = _G.test_recv_f
        local ok, err = ivshard.storage.bucket_send(bid, uuid,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, {bid, g.replica_1_a:replicaset_uuid()})
end

test_group.test_master_exclusive_api = function(g)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.sharding[1].replicas.replica_1_a.master = false
    local new_global_cfg = vtest.config_new(new_cfg_template)
    vtest.cluster_cfg(g, new_global_cfg)

    local dst_bid = vtest.storage_first_bucket(g.replica_2_a)
    g.replica_1_a:exec(function(dst_uuid, dst_bid)
        ilt.assert(not ivshard.storage.internal.is_master)

        local function is_error_non_master(ok, err)
            ilt.assert_not_equals(err, nil)
            ilt.assert_equals(err.code, iverror.code.NON_MASTER)
            ilt.assert_equals(ok, nil)
        end

        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(bid, dst_uuid)
        is_error_non_master(ok, err)
        ilt.assert_equals(ivshard.storage.bucket_stat(bid).status,
                          ivconst.BUCKET.ACTIVE)

        ok, err = ivshard.storage.bucket_recv(dst_bid, dst_uuid, {})
        is_error_non_master(ok, err)
        ilt.assert_equals(box.space._bucket:get{dst_bid}, nil)

        ok, err = ivshard.storage._call('rebalancer_apply_routes', {})
        is_error_non_master(ok, err)

        ok, err = ivshard.storage._call('rebalancer_request_state', {})
        is_error_non_master(ok, err)

        ok, err = ivshard.storage._call('recovery_bucket_stat', {bid})
        is_error_non_master(ok, err)
    end, {g.replica_2_a:replicaset_uuid(), dst_bid})

    vtest.cluster_cfg(g, global_cfg)
end

test_group.test_noactivity_timeout_for_explicit_master = function(g)
    local bid = g.replica_1_a:exec(function(uuid)
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(bid, uuid,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        local master = ivshard.storage.internal.replicasets[uuid].master
        ilt.assert_not_equals(master, nil)
        ilt.assert_not_equals(master.conn, nil)

        local old_timeout = ivconst.REPLICA_NOACTIVITY_TIMEOUT
        ivconst.REPLICA_NOACTIVITY_TIMEOUT = 0.01
        ifiber.sleep(ivconst.REPLICA_NOACTIVITY_TIMEOUT)
        ivtest.service_wait_for_new_ok(
            ivshard.storage.internal.conn_manager_service,
            {on_yield = function()
                ivshard.storage.internal.conn_manager_fiber:wakeup()
            end})
        master = ivshard.storage.internal.replicasets[uuid].master
        ilt.assert_not_equals(master, nil)
        ilt.assert_equals(master.conn, nil)
        ivconst.REPLICA_NOACTIVITY_TIMEOUT = old_timeout
        _G.bucket_gc_wait()
        return bid
    end, {g.replica_2_a:replicaset_uuid()})

    g.replica_2_a:exec(function(uuid, bid)
        local ok, err = ivshard.storage.bucket_send(bid, uuid,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
    end, {g.replica_1_a:replicaset_uuid(), bid})
end

test_group.test_named_config_identification = function(g)
    t.run_only_if(vutil.feature.persistent_names)
    local new_cfg_template = table.deepcopy(cfg_template)
    new_cfg_template.identification_mode = 'name_as_key'
    new_cfg_template.sharding['replicaset_1'] = new_cfg_template.sharding[1]
    new_cfg_template.sharding['replicaset_2'] = new_cfg_template.sharding[2]
    new_cfg_template.sharding[1] = nil
    new_cfg_template.sharding[2] = nil
    local new_global_cfg = vtest.config_new(new_cfg_template)

    -- Attempt to configure with named config without name set causes error.
    g.replica_1_a:exec(function(cfg)
        -- Name is not shown in info, when uuid identification is used.
        local rs = ivshard.storage.info().replicasets[ivutil.replicaset_uuid()]
        ilt.assert_equals(rs.name, nil)
        ilt.assert_equals(rs.master.name, nil)
        ilt.assert_error_msg_contains('Instance name mismatch',
                                      ivshard.storage.cfg, cfg, 'replica_1_a')
    end, {new_global_cfg})

    -- Set names on all replicas.
    g.replica_1_a:exec(function()
        box.cfg{instance_name = 'replica_1_a', replicaset_name = 'replicaset_1'}
    end)
    g.replica_2_a:exec(function()
        box.cfg{instance_name = 'replica_2_a', replicaset_name = 'replicaset_2'}
    end)

    -- Check, that UUIDs are validated, when named config is used.
    local rs_1_cfg = new_global_cfg.sharding.replicaset_1
    local replica_1_a_cfg = rs_1_cfg.replicas.replica_1_a
    replica_1_a_cfg.uuid = g.replica_2_a:instance_uuid()
    g.replica_1_a:exec(function(cfg)
        ilt.assert_error_msg_contains('Instance UUID mismatch',
                                      ivshard.storage.cfg, cfg, 'replica_1_a')
    end, {new_global_cfg})
    -- The correct UUID should be OK.
    replica_1_a_cfg.uuid = g.replica_1_a:instance_uuid()

    -- Now the config can be finally applied.
    vtest.cluster_cfg(g, new_global_cfg)

    -- Test, that sending by name works properly
    local rs_name_2 = g.replica_2_a:replicaset_name()
    t.assert_equals(rs_name_2, 'replicaset_2')
    local bid = g.replica_1_a:exec(function(name)
        -- Name is shown, when name identification is used.
        local rs = ivshard.storage.info().replicasets[box.info.replicaset.name]
        ilt.assert_equals(rs.name, box.info.replicaset.name)
        ilt.assert_equals(rs.uuid, nil)
        ilt.assert_equals(rs.master.name, box.info.name)

        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(
            bid, name, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        return bid
    end, {rs_name_2})

    -- Test, that rebalancer is also ok.
    vtest.cluster_rebalancer_enable(g)
    g.replica_1_a:exec(function(bid)
        local internal = ivshard.storage.internal
        ivtest.service_wait_for_new_ok(internal.rebalancer_service,
            {on_yield = ivshard.storage.rebalancer_wakeup})

        -- Cleanup
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
        ilt.assert_equals(ivshard.storage.buckets_info()[bid], nil)
    end, {bid})

    g.replica_2_a:exec(function(bid)
        _G.bucket_recovery_wait()
        _G.bucket_gc_wait()
        local buckets_info = ivshard.storage.buckets_info()
        ilt.assert_not_equals(buckets_info[bid], nil)
        ilt.assert_equals(buckets_info[bid].status, 'active')
    end, {bid})

    vtest.cluster_rebalancer_disable(g)
    -- Back to UUID identification.
    vtest.cluster_cfg(g, global_cfg)
end
