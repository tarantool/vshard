if(POLICY CMP0037)
    cmake_policy(SET CMP0037 OLD)
endif(POLICY CMP0037)

add_custom_target(test
    COMMAND ${PROJECT_SOURCE_DIR}/test/test-run.py -j -1
        --builddir=${PROJECT_BINARY_DIR}
        --vardir=${PROJECT_BINARY_DIR}/test/var
        --replication-sync-timeout=300
        --test-timeout=310
        --no-output-timeout=320)

add_custom_target(test-force
    COMMAND ${PROJECT_SOURCE_DIR}/test/test-run.py -j -1
        --builddir=${PROJECT_BINARY_DIR}
        --vardir=${PROJECT_BINARY_DIR}/test/var
        --replication-sync-timeout=300
        --test-timeout=310
        --no-output-timeout=320
        --force)
