local t = require('luatest')
local server = require('test.luatest_helpers.server')
local verror = require('vshard.error')
local vratelimit = require('vshard.log_ratelimit')

local test_group = t.group('log_ratelimit')

test_group.before_all(function(g)
    g.server = server:new({alias = 'node'})
    g.server:start()
    g.server:exec(function()
        rawset(_G, 'ivratelimit', require('vshard.log_ratelimit'))
        rawset(_G, 'iverror', require('vshard.error'))
    end)
end)

test_group.after_all(function(g)
    g.server:drop()
end)

test_group.test_can_log = function()
    local name = 'test_can_log'
    local limiter = vratelimit.create({name = name})

    local err = verror.box(box.error.new(box.error.NO_CONNECTION))
    limiter:log_info(err)
    t.assert(limiter.map[err.type][err.code])

    err = verror.vshard(verror.code.NO_SUCH_REPLICASET, 'rs1')
    limiter:log_warn(err)
    t.assert(limiter.map[err.type][err.code])

    err = verror.make('Some error')
    limiter:log_error(err)
    t.assert(limiter.map[err.type][err.code])

    -- The same message is suppressed, no matter what level is.
    limiter:log_info(err)
    t.assert_equals(limiter.map[err.type][err.code].suppressed, 1)
end

test_group.test_log = function(g)
    g.server:exec(function()
        local consts = require('vshard.consts')
        rawset(_G, 'old_interval', consts.LOG_RATELIMIT_INTERVAL)
        consts.LOG_RATELIMIT_INTERVAL = 0.1
        rawset(_G, 'limiter', _G.ivratelimit.create({name = 'test_log'}))
    end)

    g.server:assert_log_exactly_once('Some error', function()
        local err = iverror.make('Some error')
        _G.limiter:log_info(err, 'Some error')
        ilt.assert(_G.limiter.map[err.type][err.code])
        ilt.assert_equals(_G.limiter.heap:count(), 1)
    end)

    t.helpers.retrying({}, function()
        t.assert(g.server:grep_log("Suppressed 1 .* messages"))
    end)

    g.server:exec(function()
        require('vshard.consts').LOG_RATELIMIT_INTERVAL = _G.old_interval
    end)
end

test_group.test_garbage_collection = function()
    --
    -- The limiters are saved in the module as weak refs, so they're garbage
    -- collected as soon as strong ref is dropped.
    --
    local name = 'gc'
    -- luacheck: ignore 231/limiter
    local limiter = vratelimit.create({name = name})
    t.assert(vratelimit.internal.limiters[name])
    limiter = nil
    collectgarbage()
    t.assert_not(vratelimit.internal.limiters[name])
end
