--
-- This module is used to upgrade the vshard.storage on the fly.
-- It updates internal Lua structures in case they are changed
-- in a commit.
--
local log = require('log')
local fiber = require('fiber')

--
-- Array of upgrade functions.
-- migrations[version] = function which upgrades module version
-- from `version` to `version + 1`.
--
local migrations = {}

-- Initialize reload_upgrade mechanism
migrations[#migrations + 1] = function(_)
    -- Code to update Lua objects.
end

migrations[#migrations + 1] = function(M)
    local bucket = box.space._bucket
    if bucket then
        assert(M.bucket_on_replace == nil)
        M.bucket_on_replace = bucket:on_replace()[1]
    end
end

migrations[#migrations + 1] = function(M)
    if not M.route_map then
        M.bucket_generation_cond = fiber.cond()
        M.route_map = {}
    end
end

--
-- Perform an update based on a version stored in `M` (internals).
-- @param M Old module internals which should be updated.
--
local function upgrade(M)
    local start_version = M.reload_version or 1
    if start_version > #migrations then
        local err_msg = string.format(
            'vshard.storage.reload_evolution: ' ..
            'auto-downgrade is not implemented; ' ..
            'loaded version is %d, upgrade script version is %d',
            start_version, #migrations
        )
        log.error(err_msg)
        error(err_msg)
    end
    for i = start_version + 1, #migrations do
        local ok, err = pcall(migrations[i], M)
        if ok then
            log.info('vshard.storage.reload_evolution: upgraded to %d version',
                     i)
        else
            local err_msg = string.format(
                'vshard.storage.reload_evolution: ' ..
                'error during upgrade to %d version: %s', i, err
            )
            log.error(err_msg)
            error(err_msg)
        end
        -- Update the version just after upgrade to have an
        -- actual version in case of an error.
        M.reload_version = i
    end
end

return {
    version = #migrations,
    upgrade = upgrade,
}
