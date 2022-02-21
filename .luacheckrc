std = "luajit"
globals = {"box", "_TARANTOOL", "tonumber64", "utf8", "table"}
ignore = {
    -- Unused argument <self>.
    "212/self",
    -- Shadowing a local variable.
    "421",
    -- Shadowing an upvalue.
    "431",
    -- Shadowing an upvalue argument.
    "432",
}

include_files = {
    'vshard/**/*.lua',
    'test/**/*_test.lua',
}

exclude_files = {
    'test/var/*',
}

files["**/*_test.lua"] = {
    ignore = {
        -- Accessing an undefined variable.
        "113/msgpack",
        "113/vshard",
    }
}
