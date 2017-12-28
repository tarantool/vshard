# Sharding for Tarantool

[![Tarantool][tarantool-badge]][Tarantool]

Sharding module for **Tarantool** based on Virtual Buckets concept.

## Prerequisites

- Tarantool 1.7.7 (use 1.7-next branch from GitHub).

## Architecture

WIP

https://docs.google.com/document/d/1DXkQEfpFHcEZJ42jLzdBlVf7fbpK5jPPGd2V7DZ-SNw/edit#heading=h.6irvd1jaina

## Configuration

A Tarantool sharded cluster consists of the following components:

- **Storage** - a storage node which stores a subset of the sharded data.
   Each shard is deployed as a set of replicated storages (a **replicaset**).
- **Router** - a query router which provides an interface between
  sharded cluster and clients.

A minimal viable sharded cluster should consists of:

- Two or more replicaset of two or more **Storage** instances
- One or more **Router** instances

The number of **Storage** instances in a replicaset defines the redundancy
factor of the data. Recommended value is 3 or more. The number of routers
are not limited, because routers are completely stateless. We recommend to
increase the number of routers when existing instance become CPU or I/O bound.

**Router** and **Storage** applications perform completely differents set of
functions and they should be deployed to different Tarantool instances.
Despite the fact that it is technically possible to place `router` application
to every Storage node, this approach is highly discouraged and should be
avoided on the productional deployments.

All **Storage** instances can be deployed with absolutely identical instance
(configuration) file. A **Storage** application automatically self-identifies
the running instance in the configuration and determines a replicaset to
which it belongs to. Due to limitation of Tarantool 1.7.x, self-identification
is currently performed by the instance name used by tarantoolctl, i.e. a name
of file used to start Tarantool instance without `.lua` script. Please ensure
that all storage nodes have globally unique instance names. It makes sense to
use some convention for naming storages in the cluster.
For example:

- `storage_1_a` - storage node #1 for replicaset#1
- `storage_1_b` - storage node #2 for replicaset#1
- `storage_1_c` - storage node #2 for replicaset#1
- `storage_1_a` - storage node #1 for replicaset#2
- ...

All Router instances are also can be deployed with absolutely identical
instance (configuration) file. Instance names are not important
for routers because routers are stateless and know nothing about each other.

All cluster nodes must have identical cluster topology for proper operation.
It is your obligation to ensure that this configuration is indetical.
We suggest to use some configuration management tool, like Ansible or Puppet
to deploy the cluster.

A sample cluster configuration for **Storage** and **Router** can look like:

```
local cfg = {
    memtx_memory = 100 * 1024 * 1024,
    sharding = {
        ['cbf06940-0790-498b-948d-042b62cf3d29'] = { -- replicaset #1
            replicas = {
                ['8a274925-a26d-47fc-9e1b-af88ce939412'] = {
                    uri = 'storage:storage@127.0.0.1:3301',
                    name = 'storage_1_a',
                    master = true
                },
                ['3de2e3e1-9ebe-4d0d-abb1-26d301b84633'] = {
                    uri = 'storage:storage@127.0.0.1:3302',
                    name = 'storage_1_b'
                }
            },
        }, -- replicaset #2
        ['ac522f65-aa94-4134-9f64-51ee384f1a54'] = { -- replicaset #2
            replicas = {
                ['1e02ae8a-afc0-4e91-ba34-843a356b8ed7'] = {
                    uri = 'storage:storage@127.0.0.1:3303',
                    name = 'storage_2_a',
                    master = true
                },
                ['001688c3-66f8-4a31-8e19-036c17d489c2'] = {
                    uri = 'storage:storage@127.0.0.1:3304',
                    name = 'storage_2_b'
                }
            },
        }, -- replicaset #1
    }, -- sharding
}
```

Field `sharding` defines logical topology of sharded Tarantool cluster.
All other fields are passed to box.cfg() as is without any modifications.

Replicaset Parameters:

- `[UUID]` - replaceset unique identifier, generate random one
   using `uuidgen(1)`.

Instance Parameters:

- `[UUID]` - instance unique identifier, generate random one
   using `uuidgen(1)`.
- `uri` - Uniform Resource Identifier of remote instance
          with optional login and password.
- `name` - unique identifier of remote instance from filename

(e.g. the name of your instance file without `.lua` suffix).
- `master` - if true then replica is *active* and should accept
             WRITE` requests

On routers call `vshard.router.cfg(cfg)`:

```
cfg.listen = 3300

-- Start the database with sharding
vshard = require('vshard')
vshard.router.cfg(cfg)
```

On storages call `vshard.storage.cfg(cfg, <INSTANCE_UUID>)`:

```
-- Get instance name
local MY_UUID = "de0ea826-e71d-4a82-bbf3-b04a6413e417"

-- Call a configuration provider
local cfg = require('devcfg')

-- Start the database with sharding
vshard = require('vshard')
vshard.storage.cfg(cfg, MY_UUID)
```

vshard.storage.cfg() will **automatically** call box.cfg() and configure
listen port and replication.

See `router.lua` and `storage.lua` at the root directory of this project
for sample configuration.

## Defining Schema

Database Schema are stored on storages and routers know nothing about
spaces and tuples.

Spaces should be defined your storage application using `box.once()`:

```
box.once("testapp:schema:1", function()
    local customer = box.schema.space.create('customer')
    customer:format({
        {'customer_id', 'unsigned'},
        {'bucket_id', 'unsigned'},
        {'name', 'string'},
    })
    customer:create_index('customer_id', {parts = {'customer_id'}})
    customer:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})

    local account = box.schema.space.create('account')
    account:format({
        {'account_id', 'unsigned'},
        {'customer_id', 'unsigned'},
        {'bucket_id', 'unsigned'},
        {'balance', 'unsigned'},
        {'name', 'string'},
    })
    account:create_index('account_id', {parts = {'account_id'}})
    account:create_index('customer_id', {parts = {'customer_id'}, unique = false})
    account:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})
    box.snapshot()
end)
```

Every space you plan to shard must have `bucket_id` unsigned field indexed
by `bucket_id` TREE index. Spaces without `bucket_id` index doesn't
participate in sharded Tarantool cluster and can be used as a regular
spaces if needed.

## Adding Data

All DML operations with data should be performed via `router`. The
only operation is supported by `router` is `CALL` via `bucket_id`:

```
result = vshard.router.call(bucket_id, mode, func, args)
```

vshard.router.call() routes result = func(unpack(args)) call to a shard
which serves `bucket_id`.

`bucket_id` is just a regular number in range 1..`BUCKET_COUNT`, where
`BUCKET_COUNT` is defined in `vshard.consts.BUCKET_COUNT`. This number
can be assigned in arbitrary way by client application. Sharded Tarantool
cluster uses this number as an opaque unique identifier to distribute
data across replicasets. It is guaranteed that all records with the same
`bucket_id` will be stored on the same replicaset.

## Router public API

All client's requests should be send to routers.

#### `vshard.router.bootstrap()`

Perform initial distribution of buckets across replicasets.

#### `result = vshard.router.call(bucket_id, mode, func, args)`

Call function `func` on a shard which serves `bucket_id`,

#### `netbox, err = vshard.router.route(bucket_id)`

Return netbox object for specified `bucket_id`.

**Parameters:**

* `bucket_id` - bucket identifier
* `mode` - `read` or `write`
* `func` - function name
* `args` - array of arguments to func

**Returns:** original return value from `func`

#### `info = vshard.router.info()`

Returns the current router status.

**Example:**

```
vshard.router.info()
---
- replicasets:
  - master:
      state: active
      uri: storage:storage@127.0.0.1:3301
      uuid: 2ec29309-17b6-43df-ab07-b528e1243a79
  - master:
      state: active
      uri: storage:storage@127.0.0.1:3303
      uuid: 810d85ef-4ce4-4066-9896-3c352fec9e64
...
```

## Storage public API

#### `info = vshard.storage.cfg(cfg, name)`

Configure the database and start sharding for instance `name`.

- `cfg` - configuration table, see examples above.
- `name` - unique instance name to identify the instance in cfg.sharding
   table

See examples above.

#### `info = vshard.storage.info()`

Returns the current storage status.

**Example:**

```
vshard.storage.info()
---
- buckets:
    2995:
      status: active
      id: 2995
    2997:
      status: active
      id: 2997
    2999:
      status: active
      id: 2999
  replicasets:
    2dd0a343-624e-4d3a-861d-f45efc571cd3:
      uuid: 2dd0a343-624e-4d3a-861d-f45efc571cd3
      master:
        state: active
        uri: storage:storage@127.0.0.1:3301
        uuid: 2ec29309-17b6-43df-ab07-b528e1243a79
    c7ad642f-2cd8-4a8c-bb4e-4999ac70bba1:
      uuid: c7ad642f-2cd8-4a8c-bb4e-4999ac70bba1
      master:
        state: active
        uri: storage:storage@127.0.0.1:3303
        uuid: 810d85ef-4ce4-4066-9896-3c352fec9e64
...
```

## Storage internal API

#### `status, result = bucket.stat(bucket_id)`

Returns information about `bucket_id`:

```
unix/:./data/storage_1_a.control> vshard.storage.bucket_stat(1)
---
- 0
- status: active
  id: 1
...
```

#### `vshard.storage.bucket_delete_garbage(bucket_id)`

Force garbage collection for `bucket_id`.

#### `status, result = bucket_collect(bucket_id)`

Collect all data logically stored in `bucket_id`:

```
vshard.storage.bucket_collect(1)
---
- 0
- - - 514
    - - [10, 1, 1, 100, 'Account 10']
      - [11, 1, 1, 100, 'Account 11']
      - [12, 1, 1, 100, 'Account 12']
      - [50, 5, 1, 100, 'Account 50']
      - [51, 5, 1, 100, 'Account 51']
      - [52, 5, 1, 100, 'Account 52']
  - - 513
    - - [1, 1, 'Customer 1']
      - [5, 1, 'Customer 5']
```

#### `status = bucket_force_create(bucket_id)`

Force creation of `bucket_id` on this replicaset.
Use only for manual recovery or initial redistribution.

#### `status = bucket_force_drop(bucket_id)`

Force removal of `bucket_id` fro, this replicaset.
Use only for manual recovery or initial redistribution.

#### `status = bucket_send(bucket_id, to)`

Transfer `bucket_id` from the current replicaset to remote replicaset.

**Parameters:**

- `bucket_id` - bucket identifier
- `to` - remote replicaset UUID

### `status = bucket_recv(bucket_id, from, data)`

Receive `bucket_id` from remote replicaset.

**Parameters:**

- `bucket_id` - bucket identifier
- `from` - UUID of original replicaset
- `data` - buckets data in the same format as `bucket_collect()` returns

## Local Development

### Quick Start

Type `make` to run the development cluster:

```bash
# make
tarantoolctl start storage_1_a
Starting instance storage_1_a...
Starting cluster for replica 'storage_1_a'
Successfully found myself in the configuration
Calling box.cfg()...
[CUT]
tarantoolctl enter router_1
connected to unix/:./data/router_1.control
unix/:./data/router_1.control>

unix/:./data/router_1.control> vshard.router.info()
---
- replicasets:
  - master:
      state: active
      uri: storage:storage@127.0.0.1:3301
      uuid: 2ec29309-17b6-43df-ab07-b528e1243a79
  - master:
      state: active
      uri: storage:storage@127.0.0.1:3303
      uuid: 810d85ef-4ce4-4066-9896-3c352fec9e64
...

unix/:./data/router_1.control> vshard.router.call(1, 'read', 'no_such_func')
---
- error: Procedure 'no_such_func' is not defined
...

```

### Details

Repository includes a pre-configured development cluster of 1 router
and 2 replicasets of 2 nodes each (=5 Tarantool instances totally):

- `router_1` - a router instance
- `storage_1_a` - a storage instance, master of replicaset #1
- `storage_1_b` - a storage instance, slave of replicaset #1
- `storage_2_a` - a storage instance, master of replicaset #2
- `storage_2_b` - a storage instance, slave of replicaset #2

All instances are managed by `tarantoolctl` utility from the root directory
of the project. Use `tarantoolctl start router_1` to start `router_1`,
`tarantoolctl enter router_1` to enter admin console and so on.
`make start` starts all instances:

```bash
# make start
# ps x|grep tarantoo[l]
15373 ?        Ssl    0:00 tarantool storage_1_a.lua <running>
15379 ?        Ssl    0:00 tarantool storage_1_b.lua <running>
15403 ?        Ssl    0:00 tarantool storage_2_a.lua <running>
15418 ?        Ssl    0:00 tarantool storage_2_b.lua <running>
15433 ?        Ssl    0:00 tarantool router_1.lua <running>
```

Essential commands you need to known:

* `make start` - start all Tarantool instances
* `make stop` - stop all Tarantool instances
* `make logcat` - show logs from all instances
* `make enter` - enter into admin console on router\_1
* `make clean` - clean all persistent data
* `make test` or `cd test && ./test-run.py` - run the test suite
* `make` = stop + clean + start + enter

See Also
--------

Feel free to contact us on [Telegram] channel or send a pull request.

* [Tarantool]
* [Maillist]
* [Telegram]

[tarantool-badge]: https://img.shields.io/badge/Tarantool-1.7-blue.svg?style=flat
[Tarantool]: https://tarantool.org/
[Telegram]: http://telegram.me/tarantool
[Maillist]: https://groups.google.com/forum/#!forum/tarantool
