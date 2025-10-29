--
-- The module is intended for limiting the number of messages, thrown to
-- logs, so that there's no more than 1 per consts.LOG_RATELIMIT_INTERVAL
-- is logged.
--

local MODULE_INTERNALS = '__module_vshard_log_ratelimit'
-- Update when behaviour of anything in the file is changed.
local MODULE_VERSION = 1

local consts = require('vshard.consts')
local lheap = require('vshard.heap')
local util = require('vshard.util')
local fiber = require('fiber')
local log = require('log')

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {
        module_version = MODULE_VERSION,
        -- Fiber to flush the created ratelimiters.
        flush_fiber = nil,
        --
        -- The table of all ratelimiters, which are flushed automatically.
        -- Note, that the limiters are saved as weak limiters, so they're
        -- garbage collected by Lua, when no strong reference in other module
        -- present.
        --
        limiters = setmetatable({}, {__mode = 'v'}),
    }
end

-- Returns true, if message can be printed in logs, false otherwise
local function ratelimit_can_log(limiter, signed_entry)
    assert(signed_entry.type and signed_entry.code)
    local map = limiter.map
    local entry_type, code = signed_entry.type, signed_entry.code
    return map[entry_type] == nil or map[entry_type][code] == nil
end

local function ratelimit_suppress_entry(limiter, signed_entry)
    assert(signed_entry.type and signed_entry.code)
    local entry_type, code = signed_entry.type, signed_entry.code
    local existing = limiter.map[entry_type][code]
    existing.suppressed = existing.suppressed + 1
end

local function ratelimit_add_entry(limiter, signed_entry)
    assert(consts.LOG_RATELIMIT_INTERVAL > 0)
    assert(signed_entry.src and signed_entry.type and signed_entry.code)
    local entry_type, code = signed_entry.type, signed_entry.code
    local new_map_entry = {
        entry = signed_entry.src,
        suppressed = 0,
    }
    local map = limiter.map
    local map_type = map[entry_type] or {}
    map_type[code] = new_map_entry
    map[entry_type] = map_type
    local new_heap_entry = {
        deadline = fiber.clock() + consts.LOG_RATELIMIT_INTERVAL,
        type = entry_type,
        code = code,
    }
    limiter.heap:push(new_heap_entry)
end

local function ratelimit_flush(limiter)
    local heap = limiter.heap
    if heap:count() == 0 then
        return
    end
    local map = limiter.map
    local current_ts = fiber.clock()
    while heap:top() and heap:top().deadline <= current_ts do
        local top = heap:pop()
        assert(map[top.type] and map[top.type][top.code])
        local map_entry = map[top.type][top.code]
        map[top.type][top.code] = nil
        if not next(map[top.type]) then
            map[top.type] = nil
        end
        if map_entry.suppressed > 0 then
            local e = map_entry.entry
            -- Some errors can have no names (e.g. `SocketError`).
            log.info("Suppressed %d '%s' messages from '%s'",
                     map_entry.suppressed,
                     e.name or string.format('%s.%s', e.type, e.code),
                     limiter.name)
        end
    end
end

local function ratelimit_get_signature(new_entry)
    if not new_entry then
        return
    end
    return new_entry.type, new_entry.code
end

local function ratelimit_sign_entry(entry)
    local entry_type, code = ratelimit_get_signature(entry)
    return {
        src = entry,
        type = entry_type,
        code = code,
    }
end

local function ratelimit_log_template(log_lvl)
    return function (limiter, entry, format, ...)
        ratelimit_flush(limiter)
        if consts.LOG_RATELIMIT_INTERVAL <= 0 then
            -- Ratelimiter is disabled.
            log[log_lvl](format, ...)
            return
        end
        local level = 'verbose'
        local signed_entry = ratelimit_sign_entry(entry)
        if signed_entry.type and signed_entry.code then
            if ratelimit_can_log(limiter, signed_entry) then
                -- The entry doesn't exist in the limiter, print in requested
                -- level and add it to the limiter, so that the consequent
                -- entries are not printed until it's flushed.
                ratelimit_add_entry(limiter, signed_entry)
                level = log_lvl
            else
                -- The entry should be suppressed, it's in the limiter already.
                ratelimit_suppress_entry(limiter, signed_entry)
            end
        end
        log[level](format, ...)
    end
end

local ratelimit_mt = {
    __index = {
        log_info = ratelimit_log_template('info'),
        log_warn = ratelimit_log_template('warn'),
        log_error = ratelimit_log_template('error'),
    }
}

local function heap_min_deadline_cmp(entry1, entry2)
    return entry1.deadline < entry2.deadline
end

local function ratelimit_create(cfg)
    assert(cfg and cfg.name)
    local ratelimit = {
        --
        -- Name of the limiter is used in the 'Suppressed' message in order
        -- to distinguish in logs, from where the messages were suppressed.
        --
        name = cfg.name,
        --
        -- Map has the following structure: {
        --     <type1, string> = {
        --         <code1, int> = entry,
        --         <code2, int> = entry,
        --         <...>
        --     },
        --     <...>
        -- }
        --
        map = {},
        --
        -- Heap is used for sorting the entries by their deadline and flushing
        -- the map and heap.
        --
        heap = lheap.new(heap_min_deadline_cmp),
    }
    setmetatable(ratelimit, ratelimit_mt)
    M.limiters[ratelimit.name] = ratelimit
    if M.flush_fiber == nil then
        M.flush_fiber = util.reloadable_fiber_new(
            'vshard.ratelimit_flush', M, 'ratelimit_flush_f')
    end
    return ratelimit
end

local function ratelimit_flush_f()
    local module_version = M.module_version
    while module_version == M.module_version do
        for _, limiter in pairs(M.limiters) do
            ratelimit_flush(limiter)
        end
        fiber.sleep(consts.LOG_RATELIMIT_INTERVAL)
    end
end

if not rawget(_G, MODULE_INTERNALS) then
    rawset(_G, MODULE_INTERNALS, M)
else
    M.module_version = MODULE_VERSION
    util.module_unload_functions(M)
end

M.ratelimit_flush_f = ratelimit_flush_f

return {
    create = ratelimit_create,
    internal = M,
}
