local consts = require('vshard.consts')

return {
    _VERSION = consts.VERSION,
    router = require('vshard.router'),
    storage = require('vshard.storage'),
    consts = consts,
    error = require('vshard.error'),
}
