add_custom_target(test
    COMMAND ${PROJECT_SOURCE_DIR}/test/test-run.py -j -1
        --builddir=${PROJECT_BINARY_DIR}
        --vardir=${PROJECT_BINARY_DIR}/test/var)

add_custom_target(test-force
    COMMAND ${PROJECT_SOURCE_DIR}/test/test-run.py -j -1
        --builddir=${PROJECT_BINARY_DIR}
        --vardir=${PROJECT_BINARY_DIR}/test/var
        --force)

add_custom_target(test-flaky
    COMMAND ${CMAKE_COMMAND} -E env RANGE=$$RANGE
        git diff --relative=test --name-only $$\{RANGE:-master..HEAD\}
        -- ./*/*test.lua
        | paste -sd' ' - | awk '{for (i=0\; i<32\; i++) print $$0}'
        | xargs ${PROJECT_SOURCE_DIR}/test/test-run.py -j -1 --retries=0
        --builddir=${PROJECT_BINARY_DIR} --vardir=${PROJECT_BINARY_DIR}/test/var)
