--
-- Semver parser adopted to Tarantool's versions.
-- Almost everything is the same as in https://semver.org.
--
-- Tarantool's version has format:
--
--     x.x.x-typen-commit-ghash
--
-- * x.x.x - major, middle, minor release numbers;
-- * typen - release type and its optional number: alpha1, beta5, rc10.
--   Optional;
-- * commit - commit count since the latest release. Optional;
-- * ghash - latest commit hash in format g<hash>. Optional.
--
-- Differences with the semver docs:
--
-- * No support for nested releases like x.x.x-alpha.beta. Only x.x.x-alpha.
-- * Release number is written right after its type. Not 'alpha.1' but 'alpha1'.
--

local release_type_weight = {
    -- This release type is an invention of tarantool, is not documented in
    -- semver.
    entrypoint = 10,
    alpha = 20,
    beta = 30,
    rc = 40,
}

local function release_type_cmp(t1, t2)
    t1 = release_type_weight[t1]
    t2 = release_type_weight[t2]
    -- 'No release type' means the greatest.
    if not t1 then
        if not t2 then
            return 0
        end
        return 1
    end
    if not t2 then
        return -1
    end
    return t1 - t2
end

local function version_cmp(ver1, ver2)
    if ver1.id_major ~= ver2.id_major then
        return ver1.id_major - ver2.id_major
    end
    if ver1.id_middle ~= ver2.id_middle then
        return ver1.id_middle - ver2.id_middle
    end
    if ver1.id_minor ~= ver2.id_minor then
        return ver1.id_minor - ver2.id_minor
    end
    if ver1.rel_type ~= ver2.rel_type then
        return release_type_cmp(ver1.rel_type, ver2.rel_type)
    end
    if ver1.rel_num ~= ver2.rel_num then
        return ver1.rel_num - ver2.rel_num
    end
    if ver1.id_commit ~= ver2.id_commit then
        return ver1.id_commit - ver2.id_commit
    end
    return 0
end

local version_mt = {
    __eq = function(l, r)
        return version_cmp(l, r) == 0
    end,
    __lt = function(l, r)
        return version_cmp(l, r) < 0
    end,
    __le = function(l, r)
        return version_cmp(l, r) <= 0
    end,
    __tostring = function(v)
        local str = v.id_major .. '.' .. v.id_middle .. '.' .. v.id_minor
        if v.rel_type then
            str = str .. '-' .. v.rel_type
            if v.rel_num ~= 0 then
                str = str .. v.rel_num
            end
        end
        str = str .. '-' .. v.id_commit
        return str
    end,
}

local function version_new(id_major, id_middle, id_minor, rel_type, rel_num,
                           id_commit)
    -- There is no any proper validation - the API is not public.
    assert(id_major and id_middle and id_minor)
    return setmetatable({
        id_major = id_major,
        id_middle = id_middle,
        id_minor = id_minor,
        rel_type = rel_type,
        rel_num = rel_num,
        id_commit = id_commit,
    }, version_mt)
end

local function version_parse(version_str)
    --  x.x.x-name<num>-<num>-g<commit>
    -- \____/\___/\___/\_____/
    --   P1   P2   P3    P4
    local id_major, id_middle, id_minor
    local rel_type
    local rel_num = 0
    local id_commit = 0
    local pos

    -- Part 1 - version ID triplet.
    id_major, id_middle, id_minor = version_str:match('^(%d+)%.(%d+)%.(%d+)')
    if not id_major or not id_middle or not id_minor then
        error(('Could not parse version: %s'):format(version_str))
    end
    id_major = tonumber(id_major)
    id_middle = tonumber(id_middle)
    id_minor = tonumber(id_minor)

    -- Cut to 'name<num>-<num>-g<commit>'.
    pos = version_str:find('-')
    if not pos then
        goto finish
    end
    version_str = version_str:sub(pos + 1)

    -- Part 2 and 3 - release name, might be absent.
    rel_type, rel_num = version_str:match('^(%a+)(%d+)')
    if not rel_type then
        rel_type = version_str:match('^(%a+)')
        rel_num = 0
    else
        rel_num = tonumber(rel_num)
    end

    -- Cut to '<num>-g<commit>'.
    if rel_type then
        pos = version_str:find('-')
        if not pos then
            goto finish
        end
        version_str = version_str:sub(pos + 1)
    end

    -- Part 4 - commit count since latest release, might be absent.
    id_commit = version_str:match('^(%d+)')
    if not id_commit then
        id_commit = 0
    else
        id_commit = tonumber(id_commit)
    end

::finish::
    return version_new(id_major, id_middle, id_minor, rel_type, rel_num,
                       id_commit)
end

return {
    parse = version_parse,
    new = version_new,
}
