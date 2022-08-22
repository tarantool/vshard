std = 'luajit'
globals = {'box', '_TARANTOOL', 'tonumber64', 'utf8', 'table'}
ignore = {
    -- Unused argument <self>.
    '212/self',
    -- Shadowing a local variable.
    '421',
    -- Shadowing an upvalue.
    '431',
    -- Shadowing an upvalue argument.
    '432',
}

include_files = {
    'vshard/**/*.lua',
    'test/**/*_test.lua',
    'test/luatest_helpers/vtest.lua',
    'test/instances/*.lua',
}

exclude_files = {
    'test/var/*',
}

local test_rules = {
    ignore = {
        -- Accessing an undefined variable.
        '113/ifiber',
        '113/ilt',
        '113/imsgpack',
        '112/ivconst',
        '113/ivconst',
        '113/iverror',
        '112/ivshard',
        '113/ivshard',
        '113/ivtest',
        '113/ivutil',
        '113/iwait_timeout',
    }
}

files['test/**/*_test.lua'] = test_rules
files['test/luatest_helpers/vtest.lua'] = test_rules
files['test/unit-luatest/version_test.lua'] = {
    ignore = {
        -- Replace comparison sign
        '581'
    }
}
