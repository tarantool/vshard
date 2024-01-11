--
-- Exports module provides the export log and means to do various things with
-- the exports. For example, to deploy them, or their grants, or other things
-- around this theme.
--
local ljson = require('json')
local llog = require('log')
local lversion = require('vshard.version')
local lvexport_log = require('vshard.storage.export_log')
local lvutil = require('vshard.util')

--
-- Given the exports of a specific vshard version, build the final exports
-- suitable for the current core version. The result format:
--
-- {
--   vshard_version = <str>,
--   core_version = <str>,
--   funcs = [
--     <func_name> = {
--         since_core = <str>,
--         def = <table to give to box.schema.func.create(name, def)>,
--     },
--     <next functions>
--     ...
--   ]
-- }
--
local function exports_compile(exports)
    local compiled_funcs = {}
    for name, func_versions in pairs(exports.funcs) do
        local target_version
        for _, func_version in pairs(func_versions) do
            local since_core = lversion.parse(func_version.since_core)
            if lvutil.core_version < since_core then
                break
            end
            target_version = func_version
        end
        if target_version then
            compiled_funcs[name] = target_version
        end
    end
    return {
        vshard_version = exports.version,
        core_version = tostring(lvutil.core_version),
        funcs = compiled_funcs,
    }
end

--
-- The exports must be already compiled. Then create the newly exported
-- functions; delete the vshard functions not listed in the exports; modify the
-- updated exported functions. The entire set of vshard functions becomes up to
-- date with these exports like if they were deployed from scratch on an empty
-- storage.
--
local function exports_deploy_funcs(exports)
    llog.info('Deploying exports for vshard %s, core %s',
              exports.vshard_version, exports.core_version)
    local _func = box.space._func
    local existing_funcs = {}
    local to_drop_funcs = {}
    local to_update_funcs = {}
    for _, func_tuple in _func.index.name:pairs({'vshard.'},
                                                {iterator = 'GE'}) do
        local name = func_tuple.name
        if not name:startswith('vshard') then
            break
        end
        local func_export = exports.funcs[name]
        if not func_export then
            table.insert(to_drop_funcs, name)
        else
            existing_funcs[name] = true
            local current_def = {
                setuid = func_tuple.setuid == 1 and true or false,
                takes_raw_args = (func_tuple.opts or {}).takes_raw_args,
            }
            local target_def = func_export.def
            if not lvutil.table_equals(target_def, current_def) then
                to_update_funcs[func_tuple.id] = {
                    target = target_def,
                    current = current_def,
                    name = name,
                }
            end
        end
    end
    --
    -- Create.
    --
    local to_add_funcs = {}
    for name, func_export in pairs(exports.funcs) do
        if not existing_funcs[name] then
            to_add_funcs[name] = func_export.def
        end
    end
    for name, func_def in pairs(to_add_funcs) do
        llog.info("Create function '%s': %s", name, ljson.encode(func_def))
        box.schema.func.create(name, func_def)
    end
    --
    -- Update.
    --
    for func_id, update_data in pairs(to_update_funcs) do
        llog.info("Modify function '%s': from %s to %s", update_data.name,
                  ljson.encode(update_data.current),
                  ljson.encode(update_data.target))
        -- Preserve all the possible grants which could be given to the
        -- function. Don't just drop and re-create.
        local _priv = box.space._priv
        local privs = _priv.index.object:select{'function', func_id}
        local priv_pk_parts = _priv.index.primary.parts

        -- Extract the old function.
        for _, p in pairs(privs) do
            _priv:delete(lvutil.tuple_extract_key(p, priv_pk_parts))
        end
        local tuple = _func:delete(func_id)

        -- Insert the updated version. Can't update it in-place, _func doesn't
        -- support that.
        for key, value in pairs(update_data.target) do
            local location
            if tuple[key] ~= nil then
                location = key
            else
                location = ('opts.%s'):format(key)
            end
            -- In tuple this flag is expected to be an unsigned. Legacy which
            -- has to be maintained now.
            if key == 'setuid' then
                value = value and 1 or 0
            end
            tuple = tuple:update({{'=', location, value}})
            llog.info('Key update %s ok', key)
        end
        _func:insert(tuple)
        for _, p in pairs(privs) do
            _priv:insert(p)
        end
    end
    --
    -- Delete.
    --
    for _, name in pairs(to_drop_funcs) do
        llog.info("Drop function '%s'", name)
        box.schema.func.drop(name)
    end
end

local function exports_deploy_privs(exports, username)
    local user = box.space._user.index.name:get(username)
    if user == nil then
        llog.error("User or role with name %q does not exists", username)
        return
    end
    assert(user.type == 'role' or user.type == 'user')
    local grant = box.schema[user.type].grant
    for name, _ in pairs(exports.funcs) do
        grant(username, 'execute', 'function', name, {if_not_exists = true})
    end
end

return {
    log = lvexport_log,
    compile = exports_compile,
    deploy_funcs = exports_deploy_funcs,
    deploy_privs = exports_deploy_privs,
}
