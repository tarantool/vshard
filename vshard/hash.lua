-- hash.lua
local ldigest = require('digest')
local mpencode = require('msgpackffi').encode

--
-- Fast and simple hash. However it works incorrectly with
-- floating point cdata values. Also hash of an integer value
-- depends on its type: Lua number, cdata int64, cdata uint64.
--
local function strcrc32(shard_key)
    if type(shard_key) ~= 'table' then
        return ldigest.crc32(tostring(shard_key))
    else
        local crc32 = ldigest.crc32.new()
        for _, v in ipairs(shard_key) do
            crc32:update(tostring(v))
        end
        return crc32:result()
    end
end

local function mpcrc32_one(value)
    if type(value) ~= 'string' then
        return mpencode(value)
    else
        -- Despite the function called 'mp', strings are not
        -- encoded. This is because it does not make much sense to
        -- copy the whole string onto a temporary buffer just to
        -- add a small MessagePack header. Such 'hack' makes
        -- hashing of strings several magnitudes of order faster.
        return value
    end
end

--
-- Stable hash providing the correct values for integers not
-- depending on their size. However may return different hashes
-- for the same floating point value if it is cdata float or cdata
-- double.
--
local function mpcrc32(shard_key)
    if type(shard_key) ~= 'table' then
        return ldigest.crc32(mpcrc32_one(shard_key))
    else
        local crc32 = ldigest.crc32.new()
        for _, v in ipairs(shard_key) do
            crc32:update(mpcrc32_one(v))
        end
        return crc32:result()
    end
end

return {
    strcrc32 = strcrc32,
    mpcrc32 = mpcrc32,
}
