local t = require('luatest')
local server = require('test.luatest_helpers.server')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local group_config = {{mode = 'auto'}, {mode = 'manual_access'}}

if vutil.feature.memtx_mvcc then
    table.insert(group_config, {
        mode = 'auto', memtx_use_mvcc_engine = true
    })
    table.insert(group_config, {
        mode = 'manual_access', memtx_use_mvcc_engine = true
    })
end

local test_group = t.group('storage_schema_bootstrap', group_config)

local cfg_template = {
    sharding = {
        {
            master = 'auto',
            replicas = {
                replica_1_a = {read_only = false},
            },
        },
    },
    replication = {},
    bucket_count = 10,
}

test_group.before_each(function(g)
    local cfg = vtest.config_new(cfg_template)
    cfg.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    cfg.schema_management_mode = g.params.mode
    g.cfg = cfg
    local rs_id, rs = next(cfg.sharding)
    local replica_id = next(rs.replicas)
    -- Each case needs an instance whose vshard schema is not initialized yet.
    g.replica_1_a = server:new({
        alias = 'replica_1_a',
        command = 'test/instances/storage.lua',
        box_cfg = {
            instance_uuid = replica_id,
            replicaset_uuid = rs_id,
            memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine,
        },
    })
    g.replica_1_a:start()
    if g.params.mode == 'manual_access' then
        g.replica_1_a:exec(function()
            box.session.su('admin', function()
                local vexports = require('vshard.storage.exports')
                box.schema.user.create('storage', {password = 'storage'})
                local exports = vexports.compile(vexports.log[#vexports.log])
                vexports.deploy_funcs(exports)
                vexports.deploy_privs(exports, 'storage')
                box.schema.user.grant('storage', 'replication')
            end)
        end)
    end
    g.replica_1_a:exec(function(cfg)
        require('vshard.storage.schema').cfg(cfg)
        rawset(_G, 'schema_upgrade', function()
            local schema = require('vshard.storage.schema')
            box.session.su('admin', schema.upgrade,
                           schema.latest_version,
                           'storage', 'storage')
        end)
    end, {g.cfg})
end)

test_group.after_each(function(g)
    g.replica_1_a:drop()
end)

--
-- Schema bootstrap must work in both management modes, and switching modes
-- must preserve existing buckets.
--
test_group.test_change_management_mode = function(g)
    g.replica_1_a:exec(function(cfg)
        ilt.assert_equals(box.space._bucket, nil)
        -- Granting the replication role requires admin on Tarantool 2.x.
        box.session.su('admin', ivshard.storage.cfg, cfg, box.info.uuid)
        ivshard.storage.rebalancer_disable()
        ilt.assert(ivshard.storage.bucket_force_create(1, 3))
    end, {g.cfg})
    local cfg = table.deepcopy(g.cfg)
    if cfg.schema_management_mode == 'auto' then
        cfg.schema_management_mode = 'manual_access'
    else
        cfg.schema_management_mode = 'auto'
    end
    g.replica_1_a:exec(function(cfg)
        box.session.su('admin', ivshard.storage.cfg, cfg, box.info.uuid)
        local schema = require('vshard.storage.schema')
        ilt.assert_equals(schema.current_version(), schema.latest_version)
        ilt.assert_equals(ivshard.storage.buckets_count(), 3)
    end, {cfg})
end

--
-- gh-671: failing after _bucket creation must leave no partial schema, so a
-- later bootstrap attempt can succeed.
--
test_group.test_bootstrap_rollback = function(g)
    g.replica_1_a:exec(function()
        local is_called = false
        local function fail_index(_, new)
            local bucket = box.space._bucket
            if new and bucket and new[1] == bucket.id then
                ilt.assert_equals(#bucket:format(), 3)
                is_called = true
                error('injected bootstrap failure')
            end
            return new
        end
        box.space._index:before_replace(fail_index)
        ilt.assert_error_msg_contains('injected bootstrap failure',
                                      _G.schema_upgrade)
        box.space._index:before_replace(nil, fail_index)
        ilt.assert(is_called)
        ilt.assert_equals(box.space._bucket, nil)

        _G.schema_upgrade()
        local bucket = box.space._bucket
        ilt.assert(bucket.index.pk)
        ilt.assert(bucket.index.status)
        ilt.assert(box.schema.user.exists('storage'))
        bucket:insert{1, 'active'}
        ilt.assert_equals(bucket.index.status:count{'active'}, 1)
    end)
end

--
-- Triggers installed during failed bootstrap must not survive rollback or
-- attach to another space that reuses the aborted _bucket ID.
--
test_group.test_delayed_triggers_after_rollback = function(g)
    g.replica_1_a:exec(function(cfg)
        local storage = ivshard.storage
        local bucket_id
        local function fail_version(_, new)
            if new and new[1] == 'vshard_version' then
                bucket_id = box.space._bucket.id
                error('injected bootstrap failure')
            end
        end
        -- Register before cfg() so the delayed installer runs before failure.
        -- Tarantool runs triggers in reverse registration order, so VShard
        -- registers its on_commit callback before fail_version() throws.
        box.space._schema:on_replace(fail_version)
        local _, rs = next(cfg.sharding)
        rs.master = nil
        rs.replicas[box.info.uuid].read_only = true
        storage.cfg(cfg, box.info.uuid)
        local internal = storage.internal
        local delayed = internal.schema_on_replace
        ilt.assert(delayed)
        box.cfg{read_only = false}
        ilt.assert_error_msg_contains('injected bootstrap failure',
                                      _G.schema_upgrade)
        box.space._schema:on_replace(nil, fail_version)
        ilt.assert_equals(box.space._bucket, nil)
        ilt.assert_is(internal.schema_on_replace, delayed)
        ilt.assert_equals(#box.space._truncate:on_replace(), 0)

        local other = box.schema.space.create('other')
        ilt.assert_equals(other.id, bucket_id)
        ilt.assert_equals(#other:on_replace(), 0)
        _G.schema_upgrade()
        ilt.assert_equals(#box.space._bucket:on_replace(), 1)
        ilt.assert_equals(#box.space._truncate:on_replace(), 1)
        ilt.assert_equals(internal.schema_on_replace, nil)
        ilt.assert_equals(#box.space._schema:on_replace(), 0)
    end, {g.cfg})
end
