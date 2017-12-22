if(POLICY CMP0037)
    cmake_policy(SET CMP0037 OLD)
endif(POLICY CMP0037)

add_custom_target(test
    COMMAND ${PROJECT_SOURCE_DIR}/test/test-run.py -j -1
        --builddir=${PROJECT_BINARY_DIR}
        --vardir=${PROJECT_BINARY_DIR}/test/var)
