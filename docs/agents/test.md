# Testing in VShard

This document explains how the tests in VShard are organized, written, and run.

Tests are entirely located in `test/` and are run by `test-run.py` from inside
that folder (the runner comes from the `test-run/` git submodule).

## Luatest tests

The main helper is `test/luatest_helpers/vtest.lua`. It helps to build vshard
clusters for testing, and provides lots of helpers to access the nodes of a test
cluster for many different purposes, such as getting a bucket ID, pausing and
resuming services such as bucket garbage collection, and many more.

A typical test looks like this:

- Declares a config template and turns it into a real config with
  `vtest.config_new(template)`.

- Creates a cluster with `vtest.cluster_new(g, cfg)` - the servers become
  accessible as `g.replica_<i>_<a|b|c>`, plus `g.cluster`.

- Creates routers with `vtest.router_new()` when needed.

The frequently used convention for the file names is
`<topic>_<n1>_<n2>..._test.lua`, where the numbers are the replica counts of
each replicaset in the used cluster. For example, `router_2_2_test.lua` runs 2
replicasets with 2 replicas each, `storage_1_test.lua` - a single replicaset
with a single instance.

Such naming suits test files which plan to run multiple test cases which not
necessarily are related to each other, but want to share the test cluster
created once, to save time by avoiding building and destroying clusters on each
test case. This works particularly well for test cases which have no
after-effects on the cluster nodes, or the effects are revertible.

The instance-side code is in `test/instances/` (`storage.lua`, `router.lua`).
These define a lot of helper functions and constants that the tests can reuse.

For example, `ilt` (luatest), `ivshard`, `ifiber`, `ivconst`, `iverror`,
`ivutil`, `ivtest`, and more. The letter `i` is used for globals which the
test cases later can access directly, without `_G.<...>`.

Helpers functions have no such rule about `i` and are accessed in tests via
`_G`, like `get_uuid()`, `get_first_bucket()`, `get_n_buckets()`,
`bucket_gc_wait()`, `master_search_pause()/continue()`, `wal_sync()` and many
more.

When the tested behavior depends on the data storage, the test group is
parameterized over the engines (`memtx`, `vinyl`) and
`memtx_use_mvcc_engine`. Features missing on old Tarantool versions are gated
with `vshard.util.feature` flags, enabling such test cases depending on which
Tarantool binary runs the tests.

## Error injection

VShard has its own error injections, written in plain Lua and working in all
builds: the `errinj` tables in various modules. Examples: `ERRINJ_CFG_DELAY`,
`ERRINJ_RECOVERY_PAUSE`, `ERRINJ_MASTER_SEARCH_DELAY`, `ERRINJ_BUCKET_GC_PAUSE`,
and more. Use them to simulate certain possible but rare behaviours, such as
fiber stalls or unlikely errors.

## Robustness

The tests start real multi-instance clusters communicating over the network,
which makes them the prime source of flakiness. The rules:

- Never assume that something is guaranteed to happen in N seconds. All
  waiting must be in loops - `t.helpers.retrying(...)`, the
  `vtest.service_wait_for_*` and `wait_for_*` helpers.

- The background services sleep for long intervals. A test waiting for a
  service must explicitly wake it up in the retry loop (`recovery_wakeup`,
  `garbage_collector_wakeup`, `failover_wakeup`, ...), like the instance
  helpers such as `bucket_gc_wait()` do.

- Set huge timeouts for things which aren't expected to fail -
  `vtest.wait_timeout` / `iwait_timeout` is the default. A small timeout in a
  must-succeed wait is a flaky test waiting to happen.

- When a test needs to cover a sequence of events, it must explicitly observe
  each of them, not run through hoping the timing works out. Freeze the services
  with the pause/continue helpers and the error injections, step through the
  sequence, check the state after each step.

- Tests must be fast. Each test ideally must run in sub-second time. Longer
  duration needs to be very well justified and be unavoidable for some reason.

## Running

From inside `test/`:
```Bash
python3 test-run.py \
  [--builddir <build_dir>] \
  [--var <artifacts_dir>] \
  <param_i>...
```

The `--builddir` parameter is optional. It can be used to point where Tarantool
was built from sources. Its build directory.

The `--var` argument is optional (unless several test-run invocations are
working in parallel). It might be convenient to control, if you want to
see and analyze the artifacts in specific easily reachable location. It can
also help to reduce the path length to the artifacts, which can be important
because the servers in the tests will use Unix sockets. Their absolute path is
limited by approximately 107 chars on Linux and 103 chars on Mac OS.

The `<param_i>...` is what the test runner will be looking for in the folder
and file names recursively in `./test`, and will run all the tests whose path
contains at least one of the positional params.

The user's individual way of running the tests is recorded in
`docs/agents/local_setup.md` - load it before running anything.

---

If anything in this document seems outdated from how the code actually works,
then it must be immediately flagged to the user.
