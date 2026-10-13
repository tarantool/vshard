local t = require('luatest')
local server = require('test.luatest_helpers.server')
local vutil = require('vshard.util')

local group_config = {{}}
if vutil.feature.memtx_mvcc then
    table.insert(group_config, {memtx_use_mvcc_engine = true})
end

local g = t.group('storage_schema_upgrade', group_config)

g.before_each(function(g)
    g.server = server:new({alias = 'schema', box_cfg = {
        memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine,
    }})
    g.server:start()
end)

g.after_each(function(g)
    g.server:drop()
end)

--
-- gh-672: a read-only upgrade must return an error instead of exiting when
-- its downgrade cannot write. It must remain retryable after writability
-- is restored.
--
g.test_read_only_upgrade = function(g)
    g.server:exec(function()
        local schema = require('vshard.storage.schema')
        local function upgrade(target)
            box.session.su('admin', schema.upgrade,
                           target or schema.latest_version,
                           'storage', 'storage')
        end
        local old_version = schema.version_make{0, 1, 16, 0}
        upgrade(old_version)
        local format = box.space._bucket:format()
        box.cfg{read_only = true}
        ilt.assert_error_covers({code = box.error.READONLY}, upgrade)
        ilt.assert_equals(schema.current_version(), old_version)
        ilt.assert_equals(box.space._bucket:format(), format)
        box.cfg{read_only = false}
        upgrade()
        ilt.assert_equals(schema.current_version(), schema.latest_version)
    end)
end

--
-- A waiting upgrade to an older target must succeed when the first upgrade
-- fails after reaching that target.
--
g.test_concurrent_lower_target = function(g)
    g.server:exec(function()
        local fiber = require('fiber')
        local schema = require('vshard.storage.schema')
        local old_version = schema.version_make({0, 1, 16, 0})
        schema.upgrade_handlers[2].upgrade = function()
            error('injected upgrade failure')
        end
        schema.errinj.ERRINJ_UPGRADE_DELAY = true
        local upgrader = fiber.create(pcall, schema.upgrade,
                                      schema.latest_version,
                                      'storage', 'storage')
        upgrader:set_joinable(true)
        local waiter = fiber.create(schema.upgrade, old_version,
                                    'storage', 'storage')
        waiter:set_joinable(true)
        ilt.assert_not_equals(waiter:status(), 'dead')
        schema.errinj.ERRINJ_UPGRADE_DELAY = false
        local joined, ok, err = upgrader:join()
        ilt.assert(joined)
        ilt.assert_not(ok)
        ilt.assert_str_contains(err, 'injected upgrade failure')
        ilt.assert(waiter:join())
        ilt.assert_equals(schema.current_version(), old_version)
        ilt.assert(schema.is_core_up_to_date)
    end)
end
