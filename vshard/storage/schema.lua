local llog = require('log')
local lutil = require('vshard.util')
local lvexports = require('vshard.storage.exports')

local MODULE_INTERNALS = '__module_vshard_storage_schema'
-- Update when change behaviour of anything in the file, to be able to reload.
local MODULE_VERSION = 1

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {
        ----------------------- Common module attributes -----------------------
        module_version = MODULE_VERSION,
        -- Flag whether Tarantool core version was already checked and the
        -- schema is up to date with this version.
        is_core_up_to_date = false,
        -- Index which is a trigger to shard its space by numbers in this index.
        -- It must have at first part either unsigned, or integer or number type
        -- and be not nullable. Values in this part are considered as bucket
        -- identifiers.
        shard_index = nil,
        -- How to manage the schema. Auto - do everything automatically,
        -- manual - expect some things to be done externally outside of vshard.
        management_mode = 'auto',
        errinj = {
            ERRINJ_UPGRADE = false,
        },
    }
else
    return M
end

local schema_version_mt = {
    __tostring = function(self)
        return string.format('{%s}', table.concat(self, '.'))
    end,
    __serialize = function(self)
        return tostring(self)
    end,
    __eq = function(l, r)
        return l[1] == r[1] and l[2] == r[2] and l[3] == r[3] and l[4] == r[4]
    end,
    __lt = function(l, r)
        for i = 1, 4 do
            local diff = l[i] - r[i]
            if diff < 0 then
                return true
            elseif diff > 0 then
                return false
            end
        end
        return false
    end,
}

local function schema_version_make(ver)
    return setmetatable(ver, schema_version_mt)
end

local function schema_find_exports_for_vshard_version(version)
    if type(version) == 'table' then
        version = table.concat(version, '.')
    end
    local prev_exports = lvexports.log[1]
    for _, exports in pairs(lvexports.log) do
        if exports.version == version then
            return exports
        end
    end
    return prev_exports
end

--
-- VShard versioning works in 4 numbers: major, minor, patch, and a last helper
-- number incremented on every schema change, if first 3 numbers stay not
-- changed. That happens when users take the latest master version not having a
-- tag yet. They couldn't upgrade if not the 4th number changed inside one tag.

-- The schema first time appeared with 0.1.16. So this function describes schema
-- before that - 0.1.15.
--
local function schema_init_0_1_15_0(username, password)
    llog.info("Initializing schema %s", schema_version_make({0, 1, 15, 0}))

    local bucket = box.schema.space.create('_bucket')
    bucket:format({
        {'id', 'unsigned'},
        {'status', 'string'},
        {'destination', 'string', is_nullable = true}
    })
    bucket:create_index('pk', {parts = {'id'}})
    bucket:create_index('status', {parts = {'status'}, unique = false})

    if M.management_mode == 'auto' then
        box.schema.user.create(username, {
            password = password,
            if_not_exists = true,
        })
        box.schema.user.grant(username, 'replication', nil, nil,
                              {if_not_exists = true})
        local exports = lvexports.log[1]
        assert(exports.version == '0.1.15.0')
        exports = lvexports.compile(exports)
        lvexports.deploy_funcs(exports)
        lvexports.deploy_privs(exports, username)
    end
    box.space._schema:replace({'vshard_version', 0, 1, 15, 0})
end

local function schema_upgrade_to_0_1_16_0()
    -- Since 0.1.16.0 the old versioning by 'oncevshard:storage:<number>' is
    -- dropped because it is not really extendible nor understandable.
    llog.info("Insert 'vshard_version' into _schema")
    box.space._schema:replace({'vshard_version', 0, 1, 16, 0})
    box.space._schema:delete({'oncevshard:storage:1'})
end

local function schema_downgrade_from_0_1_16_0()
    llog.info("Remove 'vshard_version' from _schema")
    box.space._schema:replace({'oncevshard:storage:1'})
    box.space._schema:delete({'vshard_version'})
end

local function schema_current_version()
    local version = box.space._schema:get({'vshard_version'})
    if version == nil then
        return schema_version_make({0, 1, 15, 0})
    else
        return schema_version_make(version:totable(2))
    end
end

--
-- Upgrade schema features which depend on Tarantool version and are not bound
-- to a certain vshard version.
--
local function schema_upgrade_core_features()
    if M.is_core_up_to_date or M.management_mode ~= 'auto' then
        return
    end
    local version = schema_current_version()
    local exports = schema_find_exports_for_vshard_version(version)
    lvexports.deploy_funcs(lvexports.compile(exports))
    M.is_core_up_to_date = true
end

local schema_latest_version = schema_version_make({0, 1, 16, 0})

-- Every handler should be atomic. It is either applied whole, or not applied at
-- all. Atomic upgrade helps to downgrade in case something goes wrong. At least
-- by doing restart with the latest successfully applied version. However,
-- atomicity does not prohibit yields, in case the upgrade, for example, affects
-- huge number of tuples (_bucket records, maybe).
local schema_upgrade_handlers = {
    {
        version = schema_version_make({0, 1, 16, 0}),
        upgrade = schema_upgrade_to_0_1_16_0,
        downgrade = schema_downgrade_from_0_1_16_0
    },
}

local function schema_upgrade(target_version, username, password)
    local _schema = box.space._schema
    local is_old_versioning = _schema:get({'oncevshard:storage:1'}) ~= nil
    local version = schema_current_version()
    local is_bootstrap = not box.space._bucket

    if is_bootstrap then
        schema_init_0_1_15_0(username, password)
    elseif is_old_versioning then
        llog.info("The instance does not have 'vshard_version' record. "..
                  "It is 0.1.15.0.")
    end
    assert(schema_upgrade_handlers[#schema_upgrade_handlers].version ==
           schema_latest_version)
    local prev_version = version
    local ok, err1, err2
    local errinj = M.errinj.ERRINJ_UPGRADE
    for _, handler in pairs(schema_upgrade_handlers) do
        local next_version = handler.version
        if next_version > target_version then
            break
        end
        if next_version > version then
            llog.info("Upgrade vshard schema to %s", next_version)
            if errinj == 'begin' then
                ok, err1 = false, 'Errinj in begin'
            else
                ok, err1 = pcall(function()
                    local exports = schema_find_exports_for_vshard_version(
                        next_version)
                    exports = lvexports.compile(exports)
                    handler.upgrade()
                    if M.management_mode == 'auto' then
                        lvexports.deploy_funcs(exports)
                        lvexports.deploy_privs(exports, username)
                    end
                end)
                if ok and errinj == 'end' then
                    ok, err1 = false, 'Errinj in end'
                end
            end
            if not ok then
                -- Rollback in case the handler started a transaction before the
                -- exception.
                box.rollback()
                llog.info("Couldn't upgrade schema to %s: '%s'. Revert to %s",
                          next_version, err1, prev_version)
                ok, err2 = pcall(function()
                    local exports = schema_find_exports_for_vshard_version(
                        prev_version)
                    exports = lvexports.compile(exports)
                    handler.downgrade()
                    if M.management_mode == 'auto' then
                        lvexports.deploy_funcs(exports)
                        lvexports.deploy_privs(exports, username)
                    end
                end)
                if not ok then
                    llog.info("Couldn't downgrade schema to %s - fatal error: "..
                              "'%s'", prev_version, err2)
                    os.exit(-1)
                end
                error(err1)
            end
            ok, err1 = pcall(_schema.replace, _schema,
                             {'vshard_version', unpack(next_version)})
            if not ok then
                llog.info("Upgraded schema to %s but couldn't update _schema "..
                          "'vshard_version' - fatal error: '%s'", next_version,
                          err1)
                os.exit(-1)
            end
            llog.info("Successful vshard schema upgrade to %s", next_version)
        end
        prev_version = next_version
    end
    schema_upgrade_core_features()
end

--
-- Find spaces with index having the specified (in cfg) name. The function
-- result is cached using `schema_version`.
-- @retval Map of type {space_id = <space object>}.
--
local sharded_spaces_cache_schema_version = nil
local sharded_spaces_cache = nil
local function find_sharded_spaces()
    local schema_version = lutil.schema_version()
    if sharded_spaces_cache_schema_version == schema_version then
        return sharded_spaces_cache
    end
    local spaces = {}
    local idx = M.shard_index
    for k, space in pairs(box.space) do
        if type(k) == 'number' and space.index[idx] ~= nil then
            local parts = space.index[idx].parts
            local p = parts[1].type
            if p == 'unsigned' or p == 'integer' or p == 'number' then
                spaces[k] = space
            end
        end
    end
    sharded_spaces_cache_schema_version = schema_version
    sharded_spaces_cache = spaces
    return spaces
end

local function schema_cfg(cfg)
    M.shard_index = cfg.shard_index
    M.management_mode = cfg.schema_management_mode
end

M.current_version = schema_current_version
M.find_sharded_spaces = find_sharded_spaces
M.latest_version = schema_latest_version
M.upgrade = schema_upgrade
M.upgrade_handlers = schema_upgrade_handlers
M.version_make = schema_version_make
M.bootstrap_first_version = schema_init_0_1_15_0
M.cfg = schema_cfg

return M
