## VShard

VShard is the sharding module for Tarantool. It is written entirely in Lua and
runs inside Tarantool instances, turning a set of replicasets into one
horizontally scalable cluster.

The sharding is based on the virtual buckets concept. The whole dataset is
split into a fixed number of buckets. A space to be sharded gets defined a
special index which indexes one integer field in the tuples of this space,
storing the `bucket_id` values.

The buckets are atomicity units of the sharding. Indivisible small datasets.
Data of a bucket are all the tuples from all the sharded spaces, which have the
ID of this bucket.

A bucket is always entirely stored and activated for reads and writes on at most
one replicaset. And a bucket always moves between replicasets atomically.

The buckets are also a unit of routing. The requests are targeting either
specific buckets, or can be working with all buckets on one or more replicasets.

VShard supports a wide range of Tarantool versions, including very old ones.
This is possible because internally VShard maintains a compatibility layer which
re-implements and/or simulates features that VShard might need, agnostic of
the actual Tarantool core version used at the moment.

### Modules

There are two roles in VShard instances, which are also the two main modules.

#### Storage

Storage (`vshard.storage`) runs on every instance of every data replicaset.

It owns the buckets and their data, moves the buckets, recovers them in case of
problems, performs garbage collection, maintains the internal schema, and more.

The storage keeps track of its owned buckets and their states in `_bucket`
system space, which stores bucket IDs and their states such as ACTIVE, READONLY,
PINNED, GARBAGE, etc.

For all the storages in the entire cluster for each bucket ID VShard guarantees
that the bucket will be ACTIVE on at most one replicaset at a time. And can
become non-active for the time of moving to another replicaset or nodes
downtime.

#### Router

Router (`vshard.router`) is the entry point for an application.

Its job is to keep track of which bucket is stored on which replicaset, what
are the master-nodes of replicasets, which nodes are available, which are
faulty.

On top of this discovered cluster state the router forwards requests to specific
nodes based on specified request bucket IDs, balancing mode, read- or write-mode
of the request, number of errors the node has returned recently, and more.

The router is entirely stateless, and multiple routers can run on a single
instance, or even in the same process with the `vshard.storage`, on one of data
nodes.

### Structure

- `vshard/` is all the sources.
- `vshard/storage/` is the storage.
- `vshard/router/` is the router.
- `test/` are all the tests.

Individual sources outside of subfolders directly in `vshard/` are shared
independent modules which are used by the storage and router.

- `vshard/replicaset.lua` implements connector for accessing one whole
  replicaset for reads, writes, master discovery, balancing, map-reduce calls,
  and more.

- `vshard/util.lua` is a module which smoothens and wraps many bumps that vshard
  has observed in various Tarantool versions and in the Lua runtime. It gives a
  convenient thin compatibility layer to the other modules, agnostic of specific
  Tarantool versions as much as possible.

Other modules are simple enough for their names to be self-speaking.

### Testing

Tests are entirely located in `test/` and run by `test-run` from inside that
folder.

- Lua Luatest tests. The majority and the only way new tests are written. Files
  end with `_test.lua` in `test/*-luatest/` folders. They start real clusters
  using the helpers from Luatest itself and a layer of VShard's own tricks
  defined in `test/luatest_helpers/`, mainly the `vtest` module.

- Lua Diff tests. These are legacy tests, their files end with `.test.lua`. The
  code is executed like in an interactive console and the output is compared to
  the committed `.result` file. They must match. New tests do not use this
  method anymore.

- Lua TAP app tests. Legacy, located in `test/unit-tap/`. The test file is
  executed by tarantool directly and prints a TAP13 result. New tests do not use
  this method anymore.

Testing usually tries to cover 100% of new code. For tricky cases VShard has its
own error injections, written in plain Lua and working in all builds. They can
be found by `errinj` name in various modules, and allow to inject a fiber sleep
somewhere or return an error at a certain place.

### Task-specific guidance

The files under `docs/agents/` hold deeper, task-specific knowledge - the
agent-agnostic equivalent of Claude Code "skills". Each has a description
below.

**Do not read these files up front.** Load a file into context only once your
current task matches its description, and only the one(s) that match. You can
make this decision without opening the files first, by looking at the
descriptions here.

- `docs/agents/general-dev.md` - Enables standards for writing and reviewing
  VShard patches: commit structure and message format, rules for modularity,
  compatibility, hot reload, performance, comments, and code style. MUST use
  when writing or editing code and committing.

- `docs/agents/review.md` - Enables code reviewing: validation of commits and
  code for correctness and against the project standards. MUST use when the
  user asks to review a patch, a branch, a pull request.

- `docs/agents/review-gh.md` - Enables code reviewing on GitHub. This extends
  the regular review skill. MUST use when the user asks for a review to be
  posted on GitHub.

- `docs/agents/router-dev.md` - Enables knowledge for router-specific tasks.
  MUST use when developing or reasoning about `vshard.router`: request routing,
  bucket discovery, router failover, router master search, map-reduce.

- `docs/agents/setup-dev.md` - Enables access to user-specific development
  setup: environment, dependencies, running tests, static analysis. MUST use
  when `docs/agents/local_setup.md` is missing or seems outdated, and you
  need to do any of the previously listed activities.

- `docs/agents/storage-dev.md` - Enables knowledge for storage-specific
  tasks. MUST use when developing or reasoning about `vshard.storage`: buckets
  and their states, bucket transfer, rebalancer, recovery, garbage collection,
  storage refs, storage scheduler, schema.

- `docs/agents/test.md` - Enables writing and running tests for VShard. MUST
  use when editing/adding tests under `test/`.

These descriptions are copied from the `description` field of the matching
Claude skill files in `.claude/skills/*/SKILL.md`. If at some point you open
one such skill file and notice its description differs from the copy above, it
must be flagged to the user.

---

If anything in the overview seems outdated from how the code actually works,
then it must be immediately flagged to the user.
