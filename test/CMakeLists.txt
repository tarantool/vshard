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
    COMMAND git diff --relative=test/ --name-only
        $$\{RANGE:-master..HEAD\}
        -- "./*/*test.lua" |
        xargs -L 128 -r ${PROJECT_SOURCE_DIR}/test/test-run.py
        --force --retries=0
        --repeat=$$\{N_TRIALS:-32\}
        --builddir=${PROJECT_BINARY_DIR}
        --vardir=${PROJECT_BINARY_DIR}/test/var)
