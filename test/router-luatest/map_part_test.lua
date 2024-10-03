local fiber = require('fiber')
local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local g = t.group('router')
local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
                replica_1_b = {},
            },
        },
        {
            replicas = {
                replica_2_a = {
                    master = true,
                },
                replica_2_b = {},
            },
        },
        {
            replicas = {
                replica_3_a = {
                    master = true,
                },
                replica_3_b = {},
            },
        },
    },
    bucket_count = 30,
    test_user_grant_range = 'super',
}
local global_cfg = vtest.config_new(cfg_template)

g.before_all(function(cg)
    vtest.cluster_new(cg, global_cfg)
    t.assert_equals(cg.replica_1_a:exec(function()
        return #ivshard.storage.info().alerts
    end), 0, 'no alerts after boot')
    local _
    local router = vtest.router_new(cg, 'router', global_cfg)
    cg.router = router
    local res, err = router:exec(function()
        local res, err = ivshard.router.bootstrap({timeout = iwait_timeout})
        rawset(_G, 'do_map', function(args, opts)
            local old_opts = table.copy(opts)
            local val, err, err_uuid = ivshard.router.map_callrw(
                'do_map', args, opts)
            -- Make sure the options aren't changed by vshard.
            ilt.assert_equals(old_opts, opts)
            local val_type
            if opts.return_raw and val ~= nil then
                -- Src+value. The src is plain Lua data. The value is raw.
                local _, one_map = next(val)
                val_type = type(one_map)
            else
                val_type = type(val)
            end
            return {
                val = val,
                val_type = val_type,
                err = err,
                err_uuid = err_uuid,
            }
        end)
        return res, err
    end)
    t.assert(res and not err, 'bootstrap buckets')
    _, err = vtest.cluster_exec_each(cg, function()
        rawset(_G, 'do_map', function(res)
            ilt.assert_gt(require('vshard.storage.ref').count, 0)
            return {ivutil.replicaset_uuid(), res}
        end)
        rawset(_G, 'bucket_send', function(bid, dst)
            local _, err = ivshard.storage.bucket_send(
                bid, dst, {timeout = iwait_timeout})
            ilt.assert_equals(err, nil)
        end)
    end)
    t.assert_equals(err, nil)
    cg.rs1_uuid = cg.replica_1_a:replicaset_uuid()
    cg.rs2_uuid = cg.replica_2_a:replicaset_uuid()
    cg.rs3_uuid = cg.replica_3_a:replicaset_uuid()
end)

g.after_all(function(cg)
    cg.cluster:drop()
end)

local function router_do_map(router, args, opts)
    return router:exec(function(args, opts)
        return _G.do_map(args, opts)
    end, {args, opts})
end

g.test_map_part_single_rs = function(cg)
    local bids = vtest.storage_get_n_buckets(cg.replica_1_a, 4)
    local res = router_do_map(cg.router, {123}, {
        timeout = vtest.wait_timeout,
        bucket_ids = {bids[3], bids[2]},
    })
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    t.assert_equals(res.val, {
        [cg.rs1_uuid] = {{cg.rs1_uuid, 123}},
    })
end

g.test_map_part_multi_rs = function(cg)
    local bid1 = vtest.storage_first_bucket(cg.replica_1_a)
    local bid2 = vtest.storage_first_bucket(cg.replica_2_a)
    local res = router_do_map(cg.router, {123}, {
        timeout = vtest.wait_timeout,
        bucket_ids = {bid1, bid2},
    })
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    t.assert_equals(res.val, {
        [cg.rs1_uuid] = {{cg.rs1_uuid, 123}},
        [cg.rs2_uuid] = {{cg.rs2_uuid, 123}},
    })
end

g.test_map_part_all_rs = function(cg)
    local bid1 = vtest.storage_first_bucket(cg.replica_1_a)
    local bid2 = vtest.storage_first_bucket(cg.replica_2_a)
    local bid3 = vtest.storage_first_bucket(cg.replica_3_a)
    local res = router_do_map(cg.router, {123}, {
        timeout = vtest.wait_timeout,
        bucket_ids = {bid1, bid2, bid3},
    })
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    t.assert_equals(res.val, {
        [cg.rs1_uuid] = {{cg.rs1_uuid, 123}},
        [cg.rs2_uuid] = {{cg.rs2_uuid, 123}},
        [cg.rs3_uuid] = {{cg.rs3_uuid, 123}},
    })
end

g.test_map_part_ref = function(cg)
    -- First move some buckets from rs1 to rs2 and then pause gc on rs1.
    -- As a result, the buckets will be in the SENT state on rs1 and
    -- in the ACTIVE state on rs2.
    local bids1 = vtest.storage_get_n_buckets(cg.replica_1_a, 3)
    cg.replica_1_a:exec(function(bid1, bid2, to)
        _G.bucket_gc_pause()
        _G.bucket_send(bid1, to)
        _G.bucket_send(bid2, to)
    end, {bids1[1], bids1[2], cg.rs2_uuid})
    -- The buckets are ACTIVE on rs2, so the partial map should succeed.
    local res = router_do_map(cg.router, {42}, {
        timeout = vtest.wait_timeout,
        bucket_ids = {bids1[1], bids1[2]},
    })
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    t.assert_equals(res.val, {
        [cg.rs2_uuid] = {{cg.rs2_uuid, 42}},
    })
    -- But if we use some active bucket from rs1, the partial map should fail.
    -- The reason is that the moved buckets are still in the SENT state and
    -- we can't take a ref.
    res = router_do_map(cg.router, {42}, {
        timeout = 0.1,
        bucket_ids = {bids1[3]},
    })
    t.assert_equals(res.val, nil)
    t.assert(res.err)
    t.assert_equals(res.err_uuid, cg.rs1_uuid)
    -- The moved buckets still exist on the rs1 with non-active status.
    -- Let's remove them and re-enable gc on rs1.
    cg.replica_1_a:exec(function()
        _G.bucket_gc_continue()
        _G.bucket_gc_wait()
    end)
    -- Now move the buckets back to rs1 and pause gc on rs2.
    -- The buckets will be ACTIVE on rs1 and SENT on rs2,
    -- so the partial map should succeed.
    cg.replica_2_a:exec(function(bid1, bid2, to)
        _G.bucket_gc_pause()
        _G.bucket_send(bid1, to)
        _G.bucket_send(bid2, to)
    end, {bids1[1], bids1[2], cg.rs1_uuid})

    res = router_do_map(cg.router, {42}, {
        timeout = vtest.wait_timeout,
        bucket_ids = {bids1[1], bids1[2]},
    })
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    t.assert_equals(res.val, {
        [cg.rs1_uuid] = {{cg.rs1_uuid, 42}},
    })
    -- Re-enable gc on rs2.
    cg.replica_2_a:exec(function()
        _G.bucket_gc_continue()
        _G.bucket_gc_wait()
    end)
end

g.test_map_part_double_ref = function(cg)
    local bid1 = vtest.storage_first_bucket(cg.replica_1_a)
    local bid2 = vtest.storage_first_bucket(cg.replica_2_a)
    -- First, disable discovery on the router to disable route cache update.
    cg.router:exec(function(bid, uuid)
        ivshard.router.internal.errinj.ERRINJ_LONG_DISCOVERY = true
        -- Make sure the location of the bucket is known.
        local rs, err = ivshard.router.bucket_discovery(bid)
        ilt.assert_equals(err, nil)
        ilt.assert_equals(rs.uuid, uuid)
    end, {bid1, cg.rs1_uuid})
    -- Then, move the bucket form rs1 to rs2. Now the router has an outdated
    -- route cache.
    cg.replica_1_a:exec(function(bid, to)
        _G.bucket_send(bid, to)
        _G.bucket_gc_wait()
    end, {bid1, cg.rs2_uuid})
    -- Call a partial map for the moved bucket and some bucket
    -- from rs2. The ref stage should be done in two steps:
    -- 1. ref rs2 and returns the moved bucket;
    -- 2. discover the moved bucket on rs2 and avoid double reference;
    local res = router_do_map(cg.router, {42}, {
        timeout = vtest.wait_timeout,
        bucket_ids = {bid1, bid2},
    })
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    t.assert_equals(res.val, {
        [cg.rs2_uuid] = {{cg.rs2_uuid, 42}},
    })
    -- Make sure there are no references left.
    local _, err = vtest.cluster_exec_each(cg, function()
        ilt.assert_equals(require('vshard.storage.ref').count, 0)
    end)
    t.assert_equals(err, nil)
    -- Return the bucket back and re-enable discovery on the router.
    cg.replica_2_a:exec(function(bid, to)
        _G.bucket_send(bid, to)
        _G.bucket_gc_wait()
    end, {bid1, cg.rs1_uuid})
    cg.router:exec(function()
        ivshard.router.internal.errinj.ERRINJ_LONG_DISCOVERY = false
    end)
end

g.test_map_part_ref_timeout = function(cg)
    local bids = vtest.storage_get_n_buckets(cg.replica_1_a, 2)
    local bid1 = bids[1]
    local bid2 = bids[2]

    bids = vtest.storage_get_n_buckets(cg.replica_2_a, 2)
    local bid3 = bids[1]
    local bid4 = bids[2]

    -- First, disable discovery on the router to disable route cache update.
    cg.router:exec(function(bids)
        ivshard.router.internal.errinj.ERRINJ_LONG_DISCOVERY = true
        -- Make sure the location of the bucket is known.
        for _, bid in pairs(bids) do
            local _, err = ivshard.router.bucket_discovery(bid)
            ilt.assert_equals(err, nil)
        end
    end, {{bid1, bid2, bid3, bid4}})

    -- Send bucket so the router thinks:
    --     rs1: {b1, b2}, rs2: {b3, b4}
    -- and actually the state is:
    --     rs1: {b1},     rs2: {b2, b3, b4}
    cg.replica_1_a:exec(function(bid, to)
        _G.bucket_send(bid, to)
        _G.bucket_gc_wait()
    end, {bid2, cg.rs2_uuid})

    -- Partial map goes with the outdated mapping to the storages, successfully
    -- refs rs1. Then gets a bit stuck in rs2. Rs1 ref in the meantime time is
    -- lost. Due to restart or timeout or whatever.
    cg.replica_2_a:exec(function()
        local lref = require('vshard.storage.ref')
        rawset(_G, 'old_ref_add', lref.add)
        lref.add = function(rid, sid, ...)
            ilt.assert_equals(rawget(_G, 'test_ref'), nil)
            rawset(_G, 'test_ref', {rid = rid, sid = sid})
            local ok, err = _G.old_ref_add(rid, sid, ...)
            ilt.helpers.retrying({timeout = iwait_timeout}, function()
                if rawget(_G, 'test_ref') then
                    error('Test refs is not picked up')
                end
            end)
            return ok, err
        end
    end)
    local f = fiber.new(function()
        return router_do_map(cg.router, {42}, {
            timeout = vtest.wait_timeout,
            bucket_ids = {bid1, bid2, bid3, bid4},
        })
    end)
    f:set_joinable(true)
    cg.replica_2_a:exec(function()
        ilt.helpers.retrying({timeout = iwait_timeout}, function()
            if not rawget(_G, 'test_ref') then
                error('Test refs is not set')
            end
        end)
        local lref = require('vshard.storage.ref')
        local _, err = lref.del(_G.test_ref.rid, _G.test_ref.sid)
        ilt.assert_equals(err, nil)
        -- Cleanup.
        lref.add = _G.old_ref_add
        _G.old_ref_add = nil
        _G.test_ref = nil
    end)

    -- The whole request must fail now.
    local ok, res = f:join()
    t.assert(ok)
    t.assert(res)
    t.assert_not_equals(res.err, nil)
    t.assert_equals(res.err_uuid, cg.rs2_uuid)

    -- Make sure there are no references left.
    local _, err = vtest.cluster_exec_each(cg, function()
        ilt.assert_equals(require('vshard.storage.ref').count, 0)
    end)
    t.assert_equals(err, nil)

    -- Return the bucket back and re-enable discovery on the router.
    cg.replica_2_a:exec(function(bid, to)
        _G.bucket_send(bid, to)
        _G.bucket_gc_wait()
    end, {bid2, cg.rs1_uuid})
    cg.router:exec(function()
        ivshard.router.internal.errinj.ERRINJ_LONG_DISCOVERY = false
    end)
end

g.test_map_part_map = function(cg)
    local bid1 = vtest.storage_first_bucket(cg.replica_1_a)
    local bid2 = vtest.storage_first_bucket(cg.replica_2_a)
    cg.replica_2_a:exec(function()
        rawset(_G, 'old_do_map', _G.do_map)
        _G.do_map = function()
            return box.error(box.error.PROC_LUA, "map_err")
        end
    end)
    local res = router_do_map(cg.router, {3}, {
        timeout = vtest.wait_timeout,
        bucket_ids = {bid1, bid2},
    })
    t.assert_equals(res.val, nil)
    t.assert_covers(res.err, {
        code = box.error.PROC_LUA,
        type = 'ClientError',
        message = 'map_err'
    })
    t.assert_equals(res.err_uuid, cg.rs2_uuid)
    -- Check that there is no dangling references after the error.
    local _, err = vtest.cluster_exec_each(cg, function()
        ilt.assert_equals(require('vshard.storage.ref').count, 0)
    end)
    t.assert_equals(err, nil)
    cg.replica_2_a:exec(function()
        _G.do_map = _G.old_do_map
        _G.old_do_map = nil
    end)
    res = router_do_map(cg.router, {3}, {
        timeout = vtest.wait_timeout,
        bucket_ids = {bid1, bid2},
    })
    t.assert_equals(res.err, nil, res.err)
    t.assert_equals(res.err_uuid, nil)
    t.assert_equals(res.val, {
        [cg.rs1_uuid] = {{cg.rs1_uuid, 3}},
        [cg.rs2_uuid] = {{cg.rs2_uuid, 3}},
    })
end

g.test_map_part_callrw_raw = function(cg)
    t.run_only_if(vutil.feature.netbox_return_raw)
    --
    -- Successful map.
    --
    local bid1 = vtest.storage_first_bucket(cg.replica_1_a)
    local bid2 = vtest.storage_first_bucket(cg.replica_2_a)
    local res = router_do_map(cg.router, {3}, {
        timeout = vtest.wait_timeout,
        return_raw = true,
        bucket_ids = {bid1, bid2},
    })
    t.assert_equals(res.val, {
        [cg.rs1_uuid] = {{cg.rs1_uuid, 3}},
        [cg.rs2_uuid] = {{cg.rs2_uuid, 3}},
    })
    t.assert_equals(res.val_type, 'userdata')
    t.assert(not res.err)
    --
    -- Successful map, but one of the storages returns nothing.
    --
    cg.replica_2_a:exec(function()
        rawset(_G, 'old_do_map', _G.do_map)
        _G.do_map = function()
            return
        end
    end)
    res = router_do_map(cg.router, {}, {
        timeout = vtest.wait_timeout,
        return_raw = true,
        bucket_ids = {bid1, bid2},
    })
    t.assert_equals(res.val, {
        [cg.rs1_uuid] = {{cg.rs1_uuid}},
    })
    --
    -- Error at map stage.
    --
    cg.replica_2_a:exec(function()
        _G.do_map = function()
            return box.error(box.error.PROC_LUA, "map_err")
        end
    end)
    res = router_do_map(cg.router, {}, {
        timeout = vtest.wait_timeout,
        return_raw = true,
        bucket_ids = {bid1, bid2},
    })
    t.assert_equals(res.val, nil)
    t.assert_covers(res.err, {
        code = box.error.PROC_LUA,
        type = 'ClientError',
        message = 'map_err'
    }, 'error object')
    t.assert_equals(res.err_uuid, cg.rs2_uuid, 'error uuid')
    --
    -- Cleanup.
    --
    cg.replica_2_a:exec(function()
        _G.do_map = _G.old_do_map
        _G.old_do_map = nil
    end)
end

g.test_map_all_callrw_raw = function(cg)
    t.run_only_if(vutil.feature.netbox_return_raw)
    --
    -- Successful map.
    --
    local res = router_do_map(cg.router, {3}, {
        timeout = vtest.wait_timeout,
        return_raw = true,
    })
    t.assert_equals(res.val, {
        [cg.rs1_uuid] = {{cg.rs1_uuid, 3}},
        [cg.rs2_uuid] = {{cg.rs2_uuid, 3}},
        [cg.rs3_uuid] = {{cg.rs3_uuid, 3}},
    })
    t.assert_equals(res.val_type, 'userdata')
    t.assert(not res.err)
    --
    -- Successful map, but one of the storages returns nothing.
    --
    cg.replica_2_a:exec(function()
        rawset(_G, 'old_do_map', _G.do_map)
        _G.do_map = function()
            return
        end
    end)
    res = router_do_map(cg.router, {}, {
        timeout = vtest.wait_timeout,
        return_raw = true,
    })
    t.assert_equals(res.val, {
        [cg.rs1_uuid] = {{cg.rs1_uuid}},
        [cg.rs3_uuid] = {{cg.rs3_uuid}},
    })
    --
    -- Error at map stage.
    --
    cg.replica_2_a:exec(function()
        _G.do_map = function()
            return box.error(box.error.PROC_LUA, "map_err")
        end
    end)
    res = router_do_map(cg.router, {}, {
        timeout = vtest.wait_timeout,
        return_raw = true,
    })
    t.assert_equals(res.val, nil)
    t.assert_covers(res.err, {
        code = box.error.PROC_LUA,
        type = 'ClientError',
        message = 'map_err'
    }, 'error object')
    t.assert_equals(res.err_uuid, cg.rs2_uuid, 'error uuid')
    --
    -- Cleanup.
    --
    cg.replica_2_a:exec(function()
        _G.do_map = _G.old_do_map
        _G.old_do_map = nil
    end)
end
