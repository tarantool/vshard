local llog = require('log')
local lutil = require('vshard.util')

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
    box.schema.user.create(username, {
        password = password,
        if_not_exists = true,
    })
    box.schema.user.grant(username, 'replication', nil, nil,
                          {if_not_exists = true})

    local bucket = box.schema.space.create('_bucket')
    bucket:format({
        {'id', 'unsigned'},
        {'status', 'string'},
        {'destination', 'string', is_nullable = true}
    })
    bucket:create_index('pk', {parts = {'id'}})
    bucket:create_index('status', {parts = {'status'}, unique = false})

    local storage_api = {
        'vshard.storage.sync',
        'vshard.storage.call',
        'vshard.storage.bucket_force_create',
        'vshard.storage.bucket_force_drop',
        'vshard.storage.bucket_collect',
        'vshard.storage.bucket_send',
        'vshard.storage.bucket_recv',
        'vshard.storage.bucket_stat',
        'vshard.storage.buckets_count',
        'vshard.storage.buckets_info',
        'vshard.storage.buckets_discovery',
        'vshard.storage.rebalancer_request_state',
        'vshard.storage.rebalancer_apply_routes',
    }
    for _, name in ipairs(storage_api) do
        box.schema.func.create(name, {setuid = true})
        box.schema.user.grant(username, 'execute', 'function', name)
    end
    box.space._schema:replace({'vshard_version', 0, 1, 15, 0})
end

local function schema_upgrade_to_0_1_16_0(username)
    -- Since 0.1.16.0 the old versioning by 'oncevshard:storage:<number>' is
    -- dropped because it is not really extendible nor understandable.
    llog.info("Insert 'vshard_version' into _schema")
    box.space._schema:replace({'vshard_version', 0, 1, 16, 0})
    box.space._schema:delete({'oncevshard:storage:1'})

    -- vshard.storage._call() is supposed to replace some internal functions
    -- exposed in _func; to allow introduction of new functions on replicas; to
    -- allow change of internal functions without touching the schema.
    local func = 'vshard.storage._call'
    llog.info('Create function %s()', func)
    box.schema.func.create(func, {setuid = true})
    box.schema.user.grant(username, 'execute', 'function', func)
    -- Don't drop old functions in the same version. Removal can happen only
    -- after 0.1.16. Or there should appear support of rebalancing from too old
    -- versions. Drop of these functions now would immediately make it
    -- impossible to rebalance from old instances.
end

local function schema_downgrade_from_0_1_16_0()
    llog.info("Remove 'vshard_version' from _schema")
    box.space._schema:replace({'oncevshard:storage:1'})
    box.space._schema:delete({'vshard_version'})

    local func = 'vshard.storage._call'
    llog.info('Remove function %s()', func)
    box.schema.func.drop(func, {if_exists = true})
end

--
-- Make vshard.storage.bucket_recv() understand raw args as msgpack object. It
-- doesn't necessarily make it faster, but is essential to preserve the original
-- tuples as is, without their contortion through Lua tables. It would break
-- field types like MP_BIN (varbinary).
--
local function schema_upgrade_bucket_recv_raw()
    if not lutil.feature.msgpack_object then
        return
    end
    if box.func == nil then
        return
    end
    local name = 'vshard.storage.bucket_recv'
    local func = box.func[name]
    if func == nil then
        return
    end
    if func.takes_raw_args then
        return
    end
    llog.info("Upgrade %s to accept args as msgpack", name)
    box.begin()
    local ok, err = pcall(function()
        -- Preserve all the possible grants which could be given to the
        -- function. Don't just drop and re-create.
        local priv_space = box.space._priv
        local func_space = box.space._func
        local privs = priv_space.index.object:select{'function', func.id}
        local priv_pk_parts = priv_space.index.primary.parts

        -- Extract the old function.
        for _, p in pairs(privs) do
            priv_space:delete(lutil.tuple_extract_key(p, priv_pk_parts))
        end
        local tuple = func_space:delete(func.id)

        -- Insert the updated version. Can't update it in-place, _func doesn't
        -- support that.
        tuple = tuple:update({{'=', 'opts.takes_raw_args', true}})
        func_space:insert(tuple)
        for _, p in pairs(privs) do
            priv_space:insert(p)
        end
    end)
    if ok then
        ok, err = pcall(box.commit)
        if ok then
            return
        end
    end
    box.rollback()
    llog.error("Couldn't upgrade %s to accept args as msgpack: %s", name, err)
end

--
-- Upgrade schema features which depend on Tarantool version and are not bound
-- to a certain vshard version.
--
local function schema_upgrade_core_features()
    -- Core features can only change with Tarantool exe update, which means it
    -- won't happen on reconfig nor on hot reload. Enough to do it when
    -- configured first time.
    if M.is_core_up_to_date then
        return
    end
    -- In future it could make sense to store core feature set version in
    -- addition to vshard own schema version, but it would be an overkill while
    -- the feature is just one.
    schema_upgrade_bucket_recv_raw()
    M.is_core_up_to_date = true
end

local function schema_current_version()
    local version = box.space._schema:get({'vshard_version'})
    if version == nil then
        return schema_version_make({0, 1, 15, 0})
    else
        return schema_version_make(version:totable(2))
    end
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
                ok, err1 = pcall(handler.upgrade, username)
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
                ok, err2 = pcall(handler.downgrade)
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
