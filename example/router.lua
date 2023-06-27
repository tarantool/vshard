#!/usr/bin/env tarantool

require('strict').on()
fiber = require('fiber')

-- Check if we are running under test-run
if os.getenv('ADMIN') then
    test_run = require('test_run').new()
    require('console').listen(os.getenv('ADMIN'))
end

replicasets = {'cbf06940-0790-498b-948d-042b62cf3d29',
               'ac522f65-aa94-4134-9f64-51ee384f1a54'}

-- Call a configuration provider
cfg = dofile('localcfg.lua')
if arg[1] == 'discovery_disable' then
    cfg.discovery_mode = 'off'
end
cfg.listen = 3300

-- Start the database with sharding
vshard = require('vshard')
vshard.router.cfg(cfg)
