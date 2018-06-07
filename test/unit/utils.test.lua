test_run = require('test_run').new()
utils = require('vshard.util')
localcfg = require('localcfg')
json = require('json')

cfg = table.deepcopy(localcfg)
cfg.B = localcfg
cfg.A = localcfg
cfg.C = localcfg
cfg_string = utils.consistent_json(cfg)
cfg_string
restored_cfg = json.decode(cfg_string)
cfg_string == utils.consistent_json(restored_cfg)
