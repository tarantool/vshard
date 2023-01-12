--
-- Every vshard's background service (e.g. rebalancer or discovery)
-- should have an associated instance of the service_info, which
-- stores the status and the current activity of the service.
--
-- It may be accessed explicitly through the internals of the storage
-- or the router, which is done for the testing of the background fibers
-- using luatest. For monitoring purposes the info can be accessed via
-- vshard.router/storage.info({with_services = true}).
--

local SERVICE_TEMPLATE = {
    -- For logging purposes
    name = nil,
    -- Intended to show whether everything is all right.
    status = 'ok',
    -- Increasing number of status changes.
    status_idx = 0,
    -- Intended to show what the service is doing right now.
    activity = 'unknown',
    -- Shows whether error was already set on the current iteration.
    -- Should be dropped on every iteration to false with info:next_iter().
    is_error_set = false,
    -- Active error
    error = '',
}

local function service_next_iter(service)
    service.is_error_set = false
end

local function service_set_status(service, status)
    service.status_idx = service.status_idx + 1
    service.status = status
end

-- Just for consistency with set_status
local function service_set_activity(service, activity)
    service.activity = activity
end

local function service_set_status_ok(service)
    service_set_status(service, 'ok')
    service_next_iter(service)
    service.error = ''
end

local function service_set_status_error(service, err, ...)
    local err_str = string.format(err, ...)
    if not service.is_error_set then
        -- Error is supposed to be saved only if one has not already been
        -- set. New error can be set only after info:set_status_ok() or
        -- after resetting iteration with next_iter().
        service_set_status(service, 'error')
        service.is_error_set = true
        service.error = err_str
    end
    return err_str
end

-- Get a copy of all data contained in the service
local function service_info(service)
    local data = table.deepcopy(service)
    -- Implementation detail
    data.is_error_set = nil
    return data
end

local service_mt = {
    __index = {
        -- Low-level setters
        set_status = service_set_status,
        set_activity = service_set_activity,
        next_iter = service_next_iter,
        -- High-level wrappers
        set_status_ok = service_set_status_ok,
        set_status_error = service_set_status_error,
        -- Misc
        info = service_info,
    }
}

local function service_new(name)
    local service = table.deepcopy(SERVICE_TEMPLATE)
    setmetatable(service, service_mt)
    service.name = name or 'default'
    return service
end

return {
    new = service_new,
}
