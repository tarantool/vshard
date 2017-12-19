test_run = require('test_run').new()
require('console').listen(os.getenv('ADMIN'))

local fio = require('fio')
local NAME = fio.basename(arg[0], '.lua')

-- Call a configuration provider
cfg = require('devcfg')
cfg.sharding['cbf06940-0790-498b-948d-042b62cf3d29'].replicas['3de2e3e1-9ebe-4d0d-abb1-26d301b84633'] = nil
cfg.sharding['ac522f65-aa94-4134-9f64-51ee384f1a54'].replicas['1e02ae8a-afc0-4e91-ba34-843a356b8ed7'].master = nil
cfg.sharding['ac522f65-aa94-4134-9f64-51ee384f1a54'].replicas['001688c3-66f8-4a31-8e19-036c17d489c2'].master = true
cfg.sharding['910ee49b-2540-41b6-9b8c-c976bef1bb17'] = {replicas = {['ee34807e-be5c-4ae3-8348-e97be227a305'] = {uri = "storage:storage@127.0.0.1:3306", name = 'storage_3_a', master = true}}}

-- Start the database with sharding
vshard = require('vshard')
vshard.storage.cfg(cfg, 'ee34807e-be5c-4ae3-8348-e97be227a305')
