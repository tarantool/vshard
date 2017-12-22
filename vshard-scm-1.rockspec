package = 'shard'
version = 'scm-1'
source  = {
    url    = 'git://github.com/tarantool/vshard.git',
    branch = 'master',
}
description = {
    summary  = 'The new generation of sharding based on virtual buckets',
    homepage = 'https://github.com/tarantool/vshard.git',
    license  = 'BSD',
}
dependencies = {
    'tarantool >= 1.7.6.116';
}
build = {
    type = 'cmake';
    variables = {
        CMAKE_BUILD_TYPE="RelWithDebInfo";
        TARANTOOL_INSTALL_LIBDIR="$(LIBDIR)";
        TARANTOOL_INSTALL_LUADIR="$(LUADIR)";
    };
}

-- vim: syntax=lua
