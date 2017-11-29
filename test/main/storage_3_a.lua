test_run = require('test_run').new()
require('console').listen(os.getenv('ADMIN'))

local fio = require('fio')
local NAME = fio.basename(arg[0], '.lua')

-- Call a configuration provider
cfg = require('devcfg')
cfg.sharding[1][2] = nil
cfg.sharding[2][1].master = nil
cfg.sharding[2][2].master = true
cfg.sharding[3] = {{uri = "storage:storage@127.0.0.1:3306", name = 'storage_3_a', master = true}}

-- Start the database with sharding
vshard = require('vshard')
vshard.storage.cfg(cfg, NAME)
