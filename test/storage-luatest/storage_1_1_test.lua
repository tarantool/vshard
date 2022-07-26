local t = require('luatest')
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
    bucket_count = 10
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
