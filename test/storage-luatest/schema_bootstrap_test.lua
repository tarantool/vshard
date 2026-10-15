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
