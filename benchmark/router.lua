local common = require('common')
local fio = require('fio')
local log = require('log')
require('strict').on()
vshard = require('vshard')

local instance_name = fio.basename(arg[0], '.lua')
log.info('Start router %s', instance_name)

common.config.listen = common.router_listen[instance_name]
vshard.router.cfg(common.config)

box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

log.info('Router %s is started', instance_name)
