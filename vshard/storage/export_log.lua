--
-- Export log module provides all exports for all vshard schema versions. This
-- is used by vshard itself to deploy the exports and can be used by external
-- tools to do the same.
--
-- Note that it contains only the exports. Not the complete schema. I.e. there
-- is no info about users and spaces and whatever else is not for direct usage
-- by users and routers.
--
-- The export log has the format:
--
-- [
--   {
--     version = <vshard schema version str>,
--     funcs = {
--       <func name> = [
--         {
--           since_core = <since which core version the definition is used>,
--           def = <argument for box.schema.func.create(def),
--         },
--         {
--           since_core = <next core version str when the previous
--                         definition changed>,
--           ...
--         },
--         <next func defs for newer cores>
--         ...
--       ]
--   },
--   <next versions>
--   ...
--

--------------------------------------------------------------------------------
-- Version 0.1.15.0
--------------------------------------------------------------------------------
local version_0_1_15_0_func_names = {
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
local version_0_1_15_0_funcs = {}
for _, name in pairs(version_0_1_15_0_func_names) do
    version_0_1_15_0_funcs[name] = {
        {since_core = '1.10.0', def = {setuid = true}}
    }
end
--
-- Make vshard.storage.bucket_recv() understand raw args as msgpack object. It
-- doesn't necessarily make it faster, but is essential to preserve the original
-- tuples as is, without their contortion through Lua tables. It would break
-- field types like MP_BIN (varbinary).
--
table.insert(version_0_1_15_0_funcs['vshard.storage.bucket_recv'], {
    since_core = '2.10.0-beta2',
    def = {setuid = true, takes_raw_args = true}
})
local version_0_1_15_0 = {
    version = '0.1.15.0',
    funcs = version_0_1_15_0_funcs
}

--------------------------------------------------------------------------------
-- Version 0.1.16.0
--------------------------------------------------------------------------------
local version_0_1_16_0 = table.deepcopy(version_0_1_15_0)
version_0_1_16_0.version = '0.1.16.0'
-- vshard.storage._call() is supposed to replace some internal functions exposed
-- in _func; to allow introduction of new functions on replicas; to allow change
-- of internal functions without touching the schema.
version_0_1_16_0.funcs['vshard.storage._call'] = {
    {since_core = '1.10.0', def = {setuid = true}}
}
-- Don't drop old functions in the same version. Removal can happen only after
-- 0.1.16. Or there should appear support of rebalancing from too old versions.
-- Drop of these functions now would immediately make it impossible to rebalance
-- from old instances.

--------------------------------------------------------------------------------
return {
    version_0_1_15_0,
    version_0_1_16_0,
}
