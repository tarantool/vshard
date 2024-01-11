local t = require('luatest')
local lversion = require('vshard.version')

local g = t.group('version')

local function assert_version_str_equals(actual, expected)
    -- Remove commit hash, if it exists. It's not saved in version.
    local hash_pos = expected:find('-g')
    if hash_pos then
        expected = expected:sub(1, hash_pos - 1)
    end
    -- Remove trailing dash.
    if expected:sub(-1) == '-' then
        expected = expected:sub(1, -2)
    end
    -- Add 0 number of commits to expected.
    if actual:sub(-2) == '-0' and not expected:find('-0') then
        expected = expected .. '-0'
    end
    t.assert_equals(actual, expected)
end

g.test_order = function()
    -- Example of a full version: 2.10.0-beta2-86-gc9981a567.
    local versions = {
        {
            str = '1.2.3-entrypoint',
            ver = lversion.new(1, 2, 3, 'entrypoint', 0, 0),
        },
        {
            str = '1.2.3-entrypoint-30',
            ver = lversion.new(1, 2, 3, 'entrypoint', 0, 30),
        },
        {
            str = '1.2.3-entrypoint-45',
            ver = lversion.new(1, 2, 3, 'entrypoint', 0, 45),
        },
        {
            str = '1.2.3-entrypoint1',
            ver = lversion.new(1, 2, 3, 'entrypoint', 1, 0),
        },
        {
            str = '1.2.3-entrypoint1-45',
            ver = lversion.new(1, 2, 3, 'entrypoint', 1, 45),
        },
        {
            str = '1.2.3-entrypoint2',
            ver = lversion.new(1, 2, 3, 'entrypoint', 2, 0),
        },
        {
            str = '1.2.3-entrypoint2-45',
            ver = lversion.new(1, 2, 3, 'entrypoint', 2, 45),
        },
        {
            str = '1.2.3-alpha',
            ver = lversion.new(1, 2, 3, 'alpha', 0, 0),
        },
        {
            str = '1.2.3-alpha-30',
            ver = lversion.new(1, 2, 3, 'alpha', 0, 30),
        },
        {
            str = '1.2.3-alpha-45',
            ver = lversion.new(1, 2, 3, 'alpha', 0, 45),
        },
        {
            str = '1.2.3-alpha1',
            ver = lversion.new(1, 2, 3, 'alpha', 1, 0),
        },
        {
            str = '1.2.3-alpha1-45',
            ver = lversion.new(1, 2, 3, 'alpha', 1, 45),
        },
        {
            str = '1.2.3-alpha2',
            ver = lversion.new(1, 2, 3, 'alpha', 2, 0),
        },
        {
            str = '1.2.3-alpha2-45',
            ver = lversion.new(1, 2, 3, 'alpha', 2, 45),
        },
        {
            str = '1.2.3-beta',
            ver = lversion.new(1, 2, 3, 'beta', 0, 0),
        },
        {
            str = '1.2.3-beta-45',
            ver = lversion.new(1, 2, 3, 'beta', 0, 45),
        },
        {
            str = '1.2.3-beta1',
            ver = lversion.new(1, 2, 3, 'beta', 1, 0),
        },
        {
            str = '1.2.3-beta1-45',
            ver = lversion.new(1, 2, 3, 'beta', 1, 45),
        },
        {
            str = '1.2.3-beta2',
            ver = lversion.new(1, 2, 3, 'beta', 2, 0),
        },
        {
            str = '1.2.3-beta2-45',
            ver = lversion.new(1, 2, 3, 'beta', 2, 45),
        },
        {
            str = '1.2.3-rc',
            ver = lversion.new(1, 2, 3, 'rc', 0, 0),
        },
        {
            str = '1.2.3-rc-45',
            ver = lversion.new(1, 2, 3, 'rc', 0, 45),
        },
        {
            str = '1.2.3-rc1',
            ver = lversion.new(1, 2, 3, 'rc', 1, 0),
        },
        {
            str = '1.2.3-rc1-45',
            ver = lversion.new(1, 2, 3, 'rc', 1, 45),
        },
        {
            str = '1.2.3-rc2',
            ver = lversion.new(1, 2, 3, 'rc', 2, 0),
        },
        {
            str = '1.2.3-rc2-45',
            ver = lversion.new(1, 2, 3, 'rc', 2, 45),
        },
        {
            str = '1.2.3-rc3',
            ver = lversion.new(1, 2, 3, 'rc', 3, 0),
        },
        {
            str = '1.2.3-rc4',
            ver = lversion.new(1, 2, 3, 'rc', 4, 0),
        },
        {
            str = '1.2.3',
            ver = lversion.new(1, 2, 3, nil, 0, 0),
        },
        {
            str = '1.2.4',
            ver = lversion.new(1, 2, 4, nil, 0, 0),
        },
        {
            str = '1.2.4-1',
            ver = lversion.new(1, 2, 4, nil, 0, 1),
        },
        {
            str = '1.2.4-2',
            ver = lversion.new(1, 2, 4, nil, 0, 2),
        },
        {
            str = '1.2.5-entrypoint',
            ver = lversion.new(1, 2, 5, 'entrypoint', 0, 0),
        },
        {
            str = '1.2.5-entrypoint1-45-gc9981a567',
            ver = lversion.new(1, 2, 5, 'entrypoint', 1, 45),
        },
        {
            str = '1.2.5-alpha',
            ver = lversion.new(1, 2, 5, 'alpha', 0, 0),
        },
        {
            str = '1.2.5-alpha1-45-gc9981a567',
            ver = lversion.new(1, 2, 5, 'alpha', 1, 45),
        },
        {
            str = '1.2.6-',
            ver = lversion.new(1, 2, 6, nil, 0, 0),
        },
        {
            str = '1.2.7-entrypoint-',
            ver = lversion.new(1, 2, 7, 'entrypoint', 0, 0),
        },
        {
            str = '1.2.7-entrypoint1-',
            ver = lversion.new(1, 2, 7, 'entrypoint', 1, 0),
        },
        {
            str = '1.2.7-entrypoint1-45',
            ver = lversion.new(1, 2, 7, 'entrypoint', 1, 45),
        },
        {
            str = '1.2.7-entrypoint1-46-',
            ver = lversion.new(1, 2, 7, 'entrypoint', 1, 46),
        },
        {
            str = '1.2.7-alpha-',
            ver = lversion.new(1, 2, 7, 'alpha', 0, 0),
        },
        {
            str = '1.2.7-alpha1-',
            ver = lversion.new(1, 2, 7, 'alpha', 1, 0),
        },
        {
            str = '1.2.7-alpha1-45',
            ver = lversion.new(1, 2, 7, 'alpha', 1, 45),
        },
        {
            str = '1.2.7-alpha1-46-',
            ver = lversion.new(1, 2, 7, 'alpha', 1, 46),
        },
        {
            str = '1.2.8-entrypoint',
            ver = lversion.new(1, 2, 8, 'entrypoint', 0, 0),
        },
        {
            str = '1.2.8-alpha',
            ver = lversion.new(1, 2, 8, 'alpha', 0, 0),
        },
        {
            str = '1.2.8-beta',
            ver = lversion.new(1, 2, 8, 'beta', 0, 0),
        },
        {
            str = '1.2.8-rc',
            ver = lversion.new(1, 2, 8, 'rc', 0, 0),
        },
        {
            str = '1.2.9',
            ver = lversion.new(1, 2, 9, nil, 0, 0),
        },
    }
    for i, v in pairs(versions) do
        local ver = lversion.parse(v.str)
        t.assert(ver == v.ver, ('versions ==, %d'):format(i))
        t.assert(not (ver ~= v.ver), ('versions not ~=, %d'):format(i))
        t.assert(not (ver < v.ver), ('versions not <, %d'):format(i))
        t.assert(not (ver > v.ver), ('versions not >, %d'):format(i))
        t.assert(ver <= v.ver, ('versions <=, %d'):format(i))
        t.assert(ver >= v.ver, ('versions <=, %d'):format(i))
        assert_version_str_equals(tostring(ver), v.str)
        if i > 1 then
            local prev = versions[i - 1].ver
            t.assert(prev < ver, ('versions <, %d'):format(i))
            t.assert(prev <= ver, ('versions <=, %d'):format(i))
            t.assert(not (prev > ver), ('versions not >, %d'):format(i))
            t.assert(not (prev >= ver), ('versions not >=, %d'):format(i))

            t.assert(not (ver < prev), ('versions not <, %d'):format(i))
            t.assert(not (ver <= prev), ('versions not <=, %d'):format(i))
            t.assert(ver > prev, ('versions >, %d'):format(i))
            t.assert(ver >= prev, ('versions >=, %d'):format(i))

            t.assert(ver ~= prev, ('versions ~=, %d'):format(i))
            t.assert(not (ver == prev), ('versions not ==, %d'):format(i))
        end
    end
end

g.test_error = function()
    t.assert_error_msg_contains('Could not parse version', lversion.parse,
                                'bad version')
    t.assert_error_msg_contains('Could not parse version', lversion.parse,
                                '1.x.x')
    t.assert_error_msg_contains('Could not parse version', lversion.parse,
                                '1.2.x')
end
