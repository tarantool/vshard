local common = require('common')
local fio = require('fio')
local log = require('log')
require('strict').on()
vshard = require('vshard')
netbox = require('net.box')

local instance_name = fio.basename(arg[0], '.lua')
log.info('Start router %s', instance_name)

common.config.listen = common.router_listen[instance_name]
vshard.router.cfg(common.config)
bootres = vshard.router.bootstrap({if_not_bootstrapped = true})

box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

log.info('Router %s is started', instance_name)

--local this_router_id = tonumber(string.sub(instance_name, 8, 9))
--local test_stor_id = (this_router_id - 1) % 4 + 1
--test_stor = netbox.connect('127.0.0.1:330' .. test_stor_id)
num_stors = 4
test_stors = {}
for i = 1,num_stors do test_stors[i] = netbox.connect('127.0.0.1:330' .. i) end
for i = 1,num_stors do test_stors['storage_'..i..'_a'] = test_stors[i] end

function vshard_router_callrw(vbucket, f, args, opts)
    return test_stors[vshard.router.internal.routers._static_router.route_map[vbucket].master.name]:call(f, args)
    --return test_stors[vbucket % num_stors + 1]:call(f, args)
end
