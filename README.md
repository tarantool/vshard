# Sharding for Tarantool

[![Tarantool][tarantool-badge]][Tarantool]

Sharding module for **Tarantool** based on Virtual Buckets concept.

![alt text](https://github.com/tarantool/vshard/blob/master/sharding_arch.png)

## Prerequisites

- Tarantool version 1.9+.

## Install

Install **vshard** as module `tarantoolctl rocks install https://raw.githubusercontent.com/tarantool/vshard/master/vshard-scm-1.rockspec`

## Contribution

In order to contribute you might want to avoid installation into regular paths.
You need to fetch the source code to patch it in a local folder.

* `git clone <this repo or your fork>`;
* `git submodule update --init --recursive`;
* VShard requires Tarantool being in `PATH`. So either you install one into the
  system or you fetch Tarantool's main repository source code, build it, and
  add to `PATH` manually these paths: `<path to tarantool build>/src` and
  `<path to tarantool build>/extra`.

Now vshard should be functional. You can try it in `example` folder, see its
Makefile.

Your patch should pass all the existing tests (unless it is necessary to change
them) and have its own test usually. To run the tests this should work:
  * `cd test`;
  * `python test-run.py` or `./test-run.py`;

## Configuration

A Tarantool sharded cluster consists of the following components:

- **Storage** - a storage node which stores a subset of the sharded data.
   Each shard is deployed as a set of replicated storages (a **replicaset**).
- **Router** - a query router which provides an interface between
  sharded cluster and clients.

A minimal viable sharded cluster should consists of:

- One or more replication sets consisted of two or more **Storage** instances;
- One or more **Router** instances.

The number of **Storage** instances in a replicaset defines the redundancy
factor of the data. Recommended value is 3 or more. The number of routers
are not limited, because routers are completely stateless. We recommend to
increase the number of routers when existing instance become CPU or I/O bound.

**Router** and **Storage** applications perform completely different set of
functions and they should be deployed to different Tarantool instances.
Despite the fact that it is technically possible to place `router` application
to every Storage node, this approach is highly discouraged and should be
avoided on the production deployments.

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
- `storage_1_c` - storage node #3 for replicaset#1
- `storage_2_a` - storage node #1 for replicaset#2
- ...

All Router instances also can be deployed with absolutely identical
instance (configuration) file. Instance names are not important
for routers because routers are stateless and know nothing about each other.

All cluster nodes must have identical cluster topology for proper operation.
It is your obligation to ensure that this configuration is identical.
We suggest to use some configuration management tool, like Ansible or Puppet
to deploy the cluster.

A sample cluster configuration for **Storage** and **Router** can look like:

```Lua
local cfg = {
    memtx_memory = 100 * 1024 * 1024,
    bucket_count = 10000,
    rebalancer_disbalance_threshold = 10,
    rebalancer_max_receiving = 100,
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
        }, -- replicaset #1
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
        }, -- replicaset #2
    }, -- sharding
    weights = ... -- See details below.
}
```

* `sharding` defines logical topology of sharded Tarantool cluster;
* `bucket_count` total bucket count in a cluster. **It can not be changed after bootstrap!**;
* `rebalancer_disbalance_threshold` maximal bucket disbalance percents. Disbalance for each replicaset is calculated by formula: `|etalon_bucket_count - real_bucket_count| / etalon_bucket_count * 100`.
* `rebalancer_max_receiving` maximal bucket count that can be received in parallel by single replicaset. This count must be limited, because else, when a new replicaset is added to a cluster, the rebalancer would send to it very big amount of buckets from existing replicasets - it produces heavy load on a new replicaset to apply all these buckets.

Example of usage `rebalancer_max_receiving`:<br>
Suppose it to be equal to 100, total bucket count is 1000 and there are
3 replicasets with 333, 333 and 334 buckets. When a new replicaset is
added, each replicaset's etalon bucket count becomes 250. And the new
replicaset does not receive 250 buckets at once - it receives 100, 100
and 50 sequentially instead.

### Replicas weight configuration

A router sends all read-write request to a master replica only (with master = true in config). For read-only requests the sharding can use weights, if they are specified. The weights are used for failovering and for sending read-only requests not only to master replica, but to the 'nearest' available replica. Weights are used exactly to define distances between replicas in scope of a replicaset.

You can use weights, for example, to define physical distance between
router and each replica in each replicaset - in such a case read-only
requests are being sent to the literally nearest replica.<br>
Or by weights you can define, which replicas are more powerful and can
process more requests per second.

The idea is to specify for each router and replica their zone, and fill matrix of relative zone weights. It allows to use different weights in different zones for the same zone.

To define weights you can set `zone` attribute for each replica in the config above. For example:
```Lua
local cfg = {
   sharding = {
      ['...replicaset_uuid...'] = {
         replicas = {
            ['...replica_uuid...'] = {
                 ...,
                 zone = <number or string>
            }
         }
      }
   }
}
```
And in `weights` attribute of `vshard.router.cfg` argument you can specify relative weights for each zone pair. Example:
```Lua
weights = {
    [1] = {
        [2] = 1, -- Zone 1 routers sees weight of zone 2 as 1.
        [3] = 2, -- Weight of zone 3 as 2.
        [4] = 3, -- ...
    },
    [2] = {
        [1] = 10,
        [2] = 0,
        [3] = 10,
        [4] = 20,
    },
    [3] = {
        [1] = 100,
        [2] = 200, -- Zone 3 routers sees weight of zone 2 as 200. Note
                   -- that it is not equal to weight of zone 2 visible from
                   -- zone 1.
        [4] = 1000,
    }
}

local cfg = vshard.router.cfg({weights = weights, sharding = ...})
```
The last requirement to allow weighted routing is specification `zone` parameter in `vshard.router.cfg`.

### Rebalancer configuration

The sharding has builtin rebalancer, which periodically wakes up and moves data from one node to another by buckets. It takes all tuples from all spaces on a node with the same bucket id and moves to a more free node.

To help rebalancer with its work you can specify replicaset weights. The
weights are not the same weights as replica ones, defined in the section
above. The bigger replicaset weight, the more buckets it can store. You
can consider weights as relative data amount on a replicaset. For
example, if one replicaset has weight 100 and another has 200, then the
second will store twice more buckets then the first one.

By default, all weights of all replicasets are equal.

You can use weights, for example, to store more data on a replicasets with more memory space, or to store more data on hot replicasets. It depends on your application.

All other fields are passed to box.cfg() as is without any modifications.

**Replicaset Parameters**:

* `[UUID] - string` - replicaset unique identifier, generate random one
  using `uuidgen(1)`;
* `replicas - table` - a map of replicas with key = replica UUID and
  value = instance (see details below);
* `weight - number` - rebalancing weight - the less it is, the less buckets it stores.

**Instance Parameters**:

- `[UUID] - string` - instance unique identifier, generate random one using `uuidgen(1)`;
- `uri - string` - Uniform Resource Identifier of remote instance with **required** login and password;
- `name - string` - identifier of remote instance from filename (can be not unique, but it is recommended to use unique names);
- `zone - string or number` - replica zone (see weighted routing in the section 'Replicas weight configuration');
- `master - boolean` - true, if a replica is master in its replicaset. You can define 0 or 1 masters for each replicaset. It accepts all write requests.

On routers call `vshard.router.cfg(cfg)`:

```Lua
cfg.listen = 3300

-- Start the database with sharding
vshard = require('vshard')
vshard.router.cfg(cfg)
```

On storages call `vshard.storage.cfg(cfg, <INSTANCE_UUID>)`:

```Lua
-- Get instance name
local MY_UUID = "de0ea826-e71d-4a82-bbf3-b04a6413e417"

-- Call a configuration provider
local cfg = dofile('localcfg.lua')

-- Start the database with sharding
vshard = require('vshard')
vshard.storage.cfg(cfg, MY_UUID)
```

vshard.storage.cfg() will **automatically** call box.cfg() and configure
listen port and replication.

See `router.lua` and `storage.lua` at the root directory of this project
for sample configuration.

## Defining Schema

Database Schema is stored on storages and routers know nothing about
spaces and tuples.

Spaces should be defined in your storage application using `box.once()`:

```Lua
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
by `bucket_id` TREE index. Spaces without `bucket_id` index don't
participate in the sharded Tarantool cluster and can be used as regular
spaces if needed.

## Adding Data

All DML operations with data should be performed via `router`. The
only operation is supported by `router` is `CALL` via `bucket_id`:

```Lua
result = vshard.router.call(bucket_id, mode, func, args)
```

vshard.router.call() routes result = func(unpack(args)) call to a shard
which serves `bucket_id`.

`bucket_id` is just a regular number in range 1..`bucket_count`, where
`bucket_count` is configuration parameter. This number can be assigned in
arbitrary way by client application. Sharded Tarantool cluster uses this
number as an opaque unique identifier to distribute data across replicasets. It
is guaranteed that all records with the same `bucket_id` will be stored on the
same replicaset.

## Router public API

All client's requests should be sent to routers.

#### `vshard.router.bootstrap()`

Perform initial distribution of buckets across replicasets.

#### `result = vshard.router.call(bucket_id, mode, func, args, opts)`

Call function `func` on a shard which serves `bucket_id`,

#### `netbox, err = vshard.router.route(bucket_id)`

Return replicaset object for specified `bucket_id`.

#### `replicaset, err = vshard.router.routeall()`

Return all available replicaset objects in the map of type: `{UUID = replicaset}`.

#### `bucket_id = vshard.router.bucket_id(key)`

Calculate `bucket_id` using a simple built-in hash function:

#### `bucket_count = vshard.router.bucket_count()`

Return the bucket count configured by vshard.router.cfg().

#### `vshard.router.sync(timeout)`

Wait until all data are synchronized on replicas.

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

**Parameters:**

* `bucket_id` - bucket identifier
* `mode` - `read` or `write`
* `func` - function name
* `args` - array of arguments to func
* `opts` - call options. Can contain only one parameter - `timeout` in seconds.

**Returns:** original return value from `func` or nil and error object.
Error object has type attribute equal to 'ShardingError' or one of error types from tarantool ('ClientError', 'OutOfMemory', 'SocketError' ...).
* `ShardingError` - returned on errors, specific for sharding:
  replicaset unavailability, master absence, wrong bucket id etc. It has
  attribute `code` with one of values from vshard.error.code, optional
  `message` with human readable error description, and other attributes,
  specific for concrete error code;
* Other errors: see tarantool errors.

`route()` and `routeall()` returns replicaset objects. Replicaset has two methods:

#### `replicaset.callro(func, args, opts)`

Call a function `func` on a nearest available replica (distances are
defined using `replica.zone` and `cfg.weights` matrix - see sections
above) with a specified arguments. It is recommended to call only
read-only functions using `callro()`, because the function can be
executed not on a master.

#### `replicaset.callrw(func, args, opts)`

Same as `callro()`, but a call guaranteed to be executed on a master.

## Storage public API

#### `vshard.storage.cfg(cfg, name)`

Configure the database and start sharding for instance `name`.

- `cfg` - configuration table, see examples above.
- `name` - unique instance name to identify the instance in cfg.sharding
   table

See examples above.

#### `vshard.storage.sync(timeout)`

Wait until all data are synchronized on replicas.

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

Force garbage collection for `bucket_id` (in case the bucket was
transferred to a different replicaset).

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

Force removal of `bucket_id` from this replicaset.
Use only for manual recovery or initial redistribution.

#### `status = bucket_send(bucket_id, to)`

Transfer `bucket_id` from the current replicaset to a remote replicaset.

**Parameters:**

- `bucket_id` - bucket identifier
- `to` - remote replicaset UUID

### `status = bucket_recv(bucket_id, from, data)`

Receive `bucket_id` from a remote replicaset.

**Parameters:**

- `bucket_id` - bucket identifier
- `from` - UUID of original replicaset
- `data` - buckets data in the same format as `bucket_collect()` returns



### Sharding architecture
#### Overview

Consider a distributed Tarantool cluster that consists of subclusters called shards, each storing some part of data. Each shard, in its turn, constitutes a replicaset consisting of several replicas, one of which serves as a master node that processes all read and write requests.

The whole dataset is logically partitioned into a predefined number of virtual buckets (vbuckets), each assigned a unique number ranging from 1 to N, where N is the total number of vbuckets. The number of vbuckets is specifically chosen to be several orders of magnitude larger than the potential number of cluster nodes, even given future cluster scaling. For example, with M projected nodes the dataset may be split into 100 * M or even 1,000 * M vbuckets. Care should be taken when picking the number of vbuckets: if too large, it may require extra memory for storing the routing information; if too small, it may decrease the granularity of rebalancing.

Each shard stores a unique subset of vbuckets, which means that a vbucket cannot belong to several shards at once, as illustrated below:

```
vb1 vb2 vb3 vb4 vb5 vb6 vb7 vb8 vb9 vb10 vb11
    sh1         sh2        sh3       sh4
```

This shard-to-vbucket mapping is stored in a table in one of Tarantool’s system spaces, with each shard holding only a specific part of the mapping that covers those vbuckets that were assigned to this shard.

Apart from the mapping table, the bucket id is also stored in a special field of every tuple of every table participating in sharding.

Once a shard receives any request (except for SELECT) from an
application, this shard checks the bucket id specified in the request
against the table of bucket ids that belong to a given node. If the
specified bucket id is invalid, the request gets terminated with the
following error: “wrong bucket”. Otherwise the request is executed, and
all the data created in the process is assigned the bucket id specified
in the request. Note that the request should only modify the data that
has the same bucket id as the request itself.

Storing bucket ids both in the data itself and the mapping table ensures data consistency regardless of the application logic and makes rebalancing transparent for the application. Storing the mapping table in a system space ensures sharding is performed consistently in case of a failover, as all the replicas in a shard share a common table state.

#### Router

On their way from the application to the sharded cluster, all the requests pass through a separate program component called a router. Its main function is to hide the cluster topology from the application, namely:

* the number of shards and their placement;
* the rebalancing process;
* the occurrence of a failover caused by the shutdown of a replica.

A router can also calculate a bucket id on its own provided that the application clearly defines rules for calculating a bucket id based on the request data. To do it, a router needs to be aware of the data schema.

A router is stateless and doesn’t store the cluster topology. Nor does it rebalance data.
A router is a separate program component that can be implemented both in the storage and application layers, and its placement is application-driven.

A router maintains a constant pool of connections to all the storages that is created at startup. Creating it this way helps avoid configuration errors. Once a pool is created, a router caches the current state of the \_vbucket table to speed up the routing. In case a bucket id is moved to another storage as a result of data rebalancing or one of the shards fails over to a replica, a router updates the routing table in a way that's transparent for the application.

Sharding is not integrated into any centralized configuration storage system. It is assumed that the application itself handles all the interactions with such systems and passes sharding parameters. That said, the configuration can be changed dynamically - for example, when adding or deleting one or several shards:

1. to add a new shard to the cluster, a system administrator first changes the configuration of all the routers and then the configuration of all the storages;
2. the new shard becomes available to the storage layer for rebalancing;
3. as a result of rebalancing, one of the vbuckets is moved to the new shard;
4. when trying to access the vbucket, a router receives a special error code that specifies the new vbucket location.
##### CRUD (create, replace, update, delete) operations
CRUD operations can either be executed in a stored procedure inside a storage or initialized by the application. In any case, the application must include the operation bucket id in a request. When executing an INSERT request, the operation bucket id is stored in a newly created tuple. In other cases, it is checked if the specified operation bucket id matches the bucket id of a tuple being modified.
##### SELECT requests
Since a storage is not aware of the mapping between a bucket id and a primary key, all the SELECT requests executed in stored procedures inside a storage are only executed locally. Those SELECT requests that were initialized by the application are forwarded to a router. Then, if the application has passed a bucket id, a router uses it for shard calculation.

##### Calling stored procedures
There are several ways of calling stored procedures in cluster replicasets. Stored procedures can be called on a specific vbucket located in a replicaset or without specifying any particular vbucket. In the former case, it is necessary to differentiate between read and write procedures, as write procedures are not applicable to vbuckets that are being migrated. All the routing validity checks performed for sharded DML operations hold true for vbucket-bound stored procedures as well.

#### Replicaset balancing algorithm

The main objective of balancing is to add and delete replicasets as well as to even out the load based of physical capacities of certain replicasets.

For balancing to work, each replicaset can be assigned a weight that is proportional to its capacity for data storage. The simplest capacity metric is the percentage of vbuckets stored by a replicaset. Another possible metric is the total size of all sharded spaces in a replicaset.

Balancing is performed as follows: all the replicasets are ordered based on the value of the M/W ratio, where M is a replicaset capacity metric and W is a replicaset weight. If the difference between the smallest and the largest value exceeds a predefined threshold, balancing is launched for the corresponding replicasets. Once done, the process can be repeated for the next pair of replicasets with different policies, depending on how much the calculated metric deviates from the cluster mean (for example, the minimum value is compared with the maximum one, then with the second largest and so on).

With this approach, assigning a zero weight to a replicaset would allow evenly distributing all of its vbuckets among other cluster nodes, and adding a new replicaset with a zero load would result in vbuckets being moved to it from other replicasets.

In the migration process, a vbucket goes through two stages at the source and the receiver. At the source, the vbucket is put into the sending state, in which it accepts all the read requests, but declines any write requests. Once the vbucket is activated at the receiver, it is marked as moved at the source and declines all requests from here on.

At the receiver, the vbucket is created in the receiving state, and then data copying starts, with all the requests getting declined. Once all the data is copied over, the vbucket is activated and starts accepting all requests.

If a node assumes the role of a master, all the vbuckets in the sending state are checked first. For such vbuckets, a request is sent to the destination replicaset, and if a vbucket is active there, it is deleted locally. All the vbuckets in the receiving state are simply deleted.

#### Bootstrapping and restarting a storage cluster

The main problem when setting up a cluster is that the identifiers of its components (replicasets and Tarantool instances) are unknown. Adding or removing a replicaset one instance at a time creates a risk of data loss. The optimal way of setting up a cluster and adding new nodes to it is to independently create a fully functional replicaset and then add it to the cluster configuration. In this case, all the parameters necessary for updating the configuration are known prior to adding any new members to the cluster.

Once a new replicaset is fully set up in the cluster and all of its nodes are notified of the configuration changes, vbuckets are migrated to the new node.

If a replicaset master fails, it is recommended to:

1. Switch one of the replicas into master mode for all the replicaset instances, which would allow the new master to process all the incoming requests.
2. Update the configuration of all the cluster members, which would result in all connection requests being forwarded to the new master.

Monitoring the master and switching instance modes can be handled by any external utility.

To perform a planned outage of a replicaset master, it is recommended to:

1. Update the configuration of the master and wait for its replicas to get into sync. The master will be forwarding all the requests to a new master.
2. Switch a new instance into master mode.
3. Update the configuration of all the nodes.
4. Shut down the old master.

To perform a planned outage of a cluster replicaset, it is recommended to:

1. Migrate all the vbuckets to other cluster storages.
2. Update the configuration of all the nodes.
3. Shut down the replicaset.

In case a whole replicaset fails, some part of the dataset becomes inaccessible. Meanwhile, a router tries to reconnect to the master of the failed replicaset on a regular basis. This way, once the replicaset is up and running again, the cluster is automatically restored to its full working condition.

## Local Development

### Quick Start

Change directory to `example/` and type `make` to run the development cluster:

```bash
# cd example/
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

## Terms and definitions

This section contains definitions of key terms used throughout the document.

**Cluster** - Set of nodes that form a single group<br>
**Horizontal scaling** - Partitioning data into several servers and adding more servers as necessary<br>
**Node** - Physical or virtual server instance<br>
**Rebalancing** - Moving some part of data to new servers added to the cluster<br>
**Replicaset** - Container for storing data. Each replicaset stores a unique subset of vbuckets (one vbucket cannot belong to several replicasets at once)<br>
**Router** - Server responsible for routing requests from the system to certain cluster nodes<br>
**Sharding** - Database architecture that allows splitting data between two or more database instances by some key. Sharding is a special case of horizontal scaling.<br>
**Virtual bucket (vbucket)** - Sharding key that determines which replicaset stores certain data<br>

## See Also

Feel free to contact us on [Telegram (eng)]  channel or send a pull request.

* [Tarantool]
* [Maillist]
* [Telegram (rus)]

[tarantool-badge]: https://img.shields.io/badge/Tarantool-1.9-blue.svg?style=flat
[Tarantool]: https://tarantool.org/
[Telegram (eng)]: http://telegram.me/tarantool
[Telegram (rus)]: http://telegram.me/tarantoolru
[Maillist]: https://groups.google.com/forum/#!forum/tarantool
