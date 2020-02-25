ffi = require('ffi')
util = require('util')
vshard = require('vshard')
vshard.router.cfg({sharding = {}})

--
-- gh-207: vshard.router.bucket_id() was not consistent when
-- values were cdata numbers.
--

function check_values(bid, values)                                              \
    local result = {}                                                           \
    for _, v in pairs(values) do                                                \
        local v1 = bid(v)                                                       \
        local v2 = bid(v)                                                       \
        local t = type(v)                                                       \
        if t == 'cdata' then                                                    \
            t = ffi.typeof(v)                                                   \
        end                                                                     \
        if v1 ~= v2 then                                                        \
            table.insert(result, {'not stable', {t, v, v1, v2}})                \
        else                                                                    \
            table.insert(result, {t, v, v1})                                    \
        end                                                                     \
    end                                                                         \
    return result                                                               \
end

util.check_error(vshard.router.bucket_id)

values = {1, 1LL, 1ULL, ffi.cast('uint64_t', 1), ffi.cast('int64_t', 1), '1'}

check_values(vshard.router.bucket_id, values)
check_values(vshard.router.bucket_id_strcrc32, values)

-- Floating point cdata is not tested in strcrc32, because
-- tostring() is not stable on them, and returns a pointer string.
table.insert(values, ffi.cast('double', 1))
table.insert(values, ffi.cast('float', 1))
check_values(vshard.router.bucket_id_mpcrc32, values)

-- Decimal is not available in 1.10, but vshard and its tests
-- should work on 1.10. Decimal test is optional.
has_decimal, decimal = pcall(require, 'decimal')

not has_decimal or vshard.router.bucket_id_mpcrc32(decimal.new(1)) == 1696 or   \
    vshard.router.bucket_id_mpcrc32(decimal.new(1))

vshard.router.bucket_count()
