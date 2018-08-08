if(APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -undefined suppress -flat_namespace")
endif(APPLE)

add_subdirectory(storage)
add_subdirectory(router)

# Install module
install(FILES cfg.lua error.lua consts.lua hash.lua init.lua replicaset.lua
        util.lua lua_gc.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR}/vshard)
