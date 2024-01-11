local t = require('luatest')
local server = require('test.luatest_helpers.server')
local vutil = require('vshard.util')

local group_config = {{}}

if vutil.feature.memtx_mvcc then
    table.insert(group_config, {memtx_use_mvcc_engine = true})
end

local test_group = t.group('storage_schema', group_config)

test_group.before_all(function(g)
    g.server = server:new({alias = 'node', box_cfg = {
        memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    }})
    g.server:start()
    g.server:exec(function()
        rawset(_G, 'ivexports', require('vshard.storage.exports'))
        rawset(_G, 'ivutil', require('vshard.util'))
        rawset(_G, 'iversion', require('vshard.version'))
        box.schema.user.create('storage', {password = 'storage'})
    end)
end)

test_group.after_each(function(g)
    -- Drop all the exports to keep the test cases isolated.
    g.server:exec(function()
        _G.ivexports.deploy_funcs(_G.ivexports.compile({
            version = '0',
            funcs = {},
        }))
    end)
end)

test_group.after_all(function(g)
    g.server:stop()
end)

--
-- Functions are created as expected. VShard upgrade works.
--
test_group.test_basic = function(g)
    g.server:exec(function()
        local exports = _G.ivexports.log[1]
        exports = _G.ivexports.compile(exports)
        _G.ivexports.deploy_funcs(exports)
        for name, _ in pairs(exports.funcs) do
            ilt.assert(box.schema.func.exists(name))
        end
        local func_name_index = box.space._func.index.name
        local func_tuple = func_name_index:get('vshard.storage.bucket_recv')
        ilt.assert(func_tuple)
        ilt.assert_equals(func_tuple.setuid, 1)

        func_tuple = func_name_index:get('vshard.storage._call')
        ilt.assert_not(func_tuple)

        exports = _G.ivexports.log[2]
        exports = _G.ivexports.compile(exports)
        _G.ivexports.deploy_funcs(exports)
        for name, _ in pairs(exports.funcs) do
            ilt.assert(box.schema.func.exists(name))
        end
        func_tuple = func_name_index:get('vshard.storage._call')
        ilt.assert(func_tuple)
    end)
end

--
-- On core upgrade the exports can get an upgrade too, even if the vshard
-- version didn't change.
--
test_group.test_core_upgrade = function(g)
    t.run_only_if(vutil.version_is_at_least(2, 10, 0, 'beta', 2, 0))
    g.server:exec(function()
        local orig_version = _G.ivutil.core_version
        _G.ivutil.core_version = _G.iversion.parse('1.10.0')
        local raw_exports = _G.ivexports.log[#_G.ivexports.log]
        local exports = _G.ivexports.compile(raw_exports)
        _G.ivexports.deploy_funcs(exports)
        for name, _ in pairs(exports.funcs) do
            ilt.assert(box.schema.func.exists(name))
        end

        local func_name_index = box.space._func.index.name
        local func_tuple = func_name_index:get('vshard.storage.bucket_recv')
        ilt.assert(func_tuple)
        ilt.assert_equals(func_tuple.setuid, 1)
        ilt.assert_not(func_tuple.opts.takes_raw_args)

        _G.ivutil.core_version = orig_version
        exports = _G.ivexports.compile(raw_exports)
        _G.ivexports.deploy_funcs(exports)
        func_tuple = func_name_index:get('vshard.storage.bucket_recv')
        ilt.assert(func_tuple.opts.takes_raw_args)
    end)
end

-- Make sure that privileges can be granted to both the user and the role.
test_group.test_deploy_privs = function(g)
    g.server:exec(function()
        local vexports = require('vshard.storage.exports')
        local exports = vexports.log[#vexports.log]
        exports = vexports.compile(exports)
        vexports.deploy_funcs(exports)

        -- Deploy the privileges for a user.
        box.schema.user.create('one')
        local user = box.space._user.index.name:get('one')
        ilt.assert(user ~= nil)
        ilt.assert_equals(user.type, 'user')
        vexports.deploy_privs(exports, 'one')
        for func_name in pairs(exports.funcs) do
            local func = box.space._func.index.name:get(func_name)
            ilt.assert(func ~= nil)
            local priv = box.space._priv:get({user.id, 'function', func.id})
            ilt.assert_equals(priv.privilege, box.priv.X)
        end

        -- Deploy the privileges for a role.
        box.schema.role.create('two')
        user = box.space._user.index.name:get('two')
        ilt.assert(user ~= nil)
        ilt.assert_equals(user.type, 'role')
        vexports.deploy_privs(exports, 'two')
        for func_name in pairs(exports.funcs) do
            local func = box.space._func.index.name:get(func_name)
            ilt.assert(func ~= nil)
            local priv = box.space._priv:get({user.id, 'function', func.id})
            ilt.assert_equals(priv.privilege, box.priv.X)
        end

        box.schema.user.drop('one')
        box.schema.role.drop('two')
    end)
end

test_group.test_core_version = function(g)
    g.server:exec(function()
        local exports = _G.ivexports.log[#_G.ivexports.log]
        exports = _G.ivexports.compile(exports)

        local version = _TARANTOOL
        local pos = version:find('-g')
        version = version:sub(1, pos - 1)
        ilt.assert_equals(exports.core_version, version)
    end)
end
