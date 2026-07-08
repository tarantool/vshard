local lcfg = require('vshard.cfg')
local lrouter = require('vshard.router')
local consts = require('vshard.consts')

local BACKEND_TYPE = 'vshard-router'

--------------------------------------------------------------------------------
-- Config
--------------------------------------------------------------------------------

local function validate(cfg)
    local ok, config = pcall(require, 'config')
    if not ok then
        -- Should not happen, but who knows.
        error('the "config" module is not available')
    end
    if not config:is_router() then
        error(('the "router" sharding role is required for the %q recovery ' ..
               'point backend'):format(BACKEND_TYPE))
    end
    if cfg and cfg.router_name ~= nil then
        lcfg.check_option('router_name', 'string', nil, cfg.router_name)
    end
end

--------------------------------------------------------------------------------
-- Backend instance
--------------------------------------------------------------------------------

--
-- Create a cluster recovery point, tagged with opts.label, on every master via
-- map_callrw. Returns {[replicaset_id] = {point}} on success, or nil + err on
-- failure.
--
local function backend_create_point(backend, opts)
    lcfg.check_option('opts', 'table', nil, opts)
    -- The label is compulsory - point that cannot be referenced is useless.
    lcfg.check_option('opts.label', 'string', nil, opts.label)
    local call_opts = table.copy(opts)
    call_opts.label = nil
    return backend.router:create_cluster_recovery_point(opts.label, call_opts)
end

--
-- The router alerts, which lead to error on recovery point creation. Any other
-- alert the router reports is dropped.
--
local alert_whitelist = {
    MISSING_MASTER = true,
    UNREACHABLE_MASTER = true,
    UNREACHABLE_REPLICASET = true,
    UNKNOWN_BUCKETS = true,
    INVALID_CFG = true,
}

--
-- The info that will be shown under manager:info().backend. The alerts are
-- merged into manager self alerts and pushed to the box.info.config.alerts.
--
local function backend_info(backend)
    local router_info = backend.router:info()
    -- Vshard alerts are {code, message} sequences, but the manager and the
    -- config alert namespace expect {message = ...}, so convert them.
    local alerts = {}
    for _, alert in ipairs(router_info.alerts or {}) do
        if alert_whitelist[alert[1]] then
            table.insert(alerts, {message =
                string.format('%s: %s', alert[1], alert[2])})
        end
    end
    return {
        backend_type = BACKEND_TYPE,
        -- A disabled router can not run create_cluster_recovery_point() at all.
        is_enabled = router_info.is_enabled,
        -- Unknown/unreachable buckets block the ref map_callrw takes.
        bucket = router_info.bucket,
        -- map_callrw calls every master, so their reachability matters.
        replicasets = router_info.replicasets,
        -- The whitelisted alerts in the manager-expected form.
        alerts = alerts,
    }
end

local backend_mt = {
    __index = {
        create_point = backend_create_point,
        info = backend_info,
    },
}

--------------------------------------------------------------------------------
-- Module
--------------------------------------------------------------------------------

local function new(cfg)
    cfg = cfg or {}
    local router_name = cfg.router_name or consts.STATIC_ROUTER_NAME
    local router = lrouter.internal.routers[router_name]
    if router == nil then
        error(('%q recovery point backend: router %q not found')
              :format(BACKEND_TYPE, router_name))
    end
    return setmetatable({router = router}, backend_mt)
end

return {
    new = new,
    config = {
        validate = validate,
    },
}
