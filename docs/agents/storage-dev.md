# Development of the storage

This document explains more details about the storage part of VShard. It gives
more context when working specifically with `vshard.storage` code and its logic.

## Overview

The storage code is located in `vshard/storage/`, with `init.lua` being the
core and the submodules covering separate subsystems, such as `ref.lua`,
`sched.lua`, `schema.lua`, and more.

The storage module is responsible for managing buckets: tracking and modifying
their states, reference counting, recovery, rebalancing, pinning. On top of the
buckets the storage provides a public API for safely accessing data of the
local buckets, giving certain guarantees about availability and consistency.

## Buckets

The central concept is the `_bucket` system space. It stores which buckets are
present on this replicaset and in which state:
`{id, status, destination, opts}`.

Each bucket has an ID by which all of its tuples from all the sharded spaces
can be found.

The `destination` is used by the transfer states to remember the peer
replicaset. The `opts` contain `generation` - a counter incremented each time
the bucket starts being sent somewhere. The space is replicated inside the
replicaset like any other space, so all replicas share the same bucket
knowledge in an eventually consistent way.

Bucket states are listed in `consts.BUCKET`:
- `ACTIVE` - the bucket is fully here, all requests are allowed.
- `PINNED` - same as active, but the rebalancer is not allowed to move it.
- `READONLY` - the bucket accepts only reads.
- `SENDING` - the bucket is being copied to another replicaset. Reads are still
  allowed, writes are not.
- `RECEIVING` - the bucket is being received. No requests are allowed.
- `SENT` - the move has finished, the destination has activated the bucket.
  Waiting for the read-requests to end before the garbage collection wipes the
  bucket.
- `GARBAGE` - the bucket data can be deleted.

### Bucket transfer

Buckets can move. Their relocation might span multiple transactions both on the
sender and on the receiver, with data sent in chunks.

During a transfer the receiver for some time might have not all the data of the
bucket, and after the transfer the sender's old copy of the bucket might also
not be deleted at once, in a single transaction.

On top of that either replicaset might change its master during the move, and
there might be various failures on the sending and the receiving sides. This
and many other complex nuances make bucket transfer one of the most complicated
and sensitive areas of the storage's logic.

`vshard.storage.bucket_send()` is the function encapsulating all the work on
the sender side, and its counterpart `.bucket_recv()` does the second half of
the work on the receiving side.

The sender turns the bucket into `SENDING`, the destination keeps the bucket in
`RECEIVING`. On success the bucket becomes `ACTIVE` on the receiver, and `SENT`
on the sender. Eventually the sender's copy is deleted entirely.

### Bucket refs

Bucket relocation, especially working in chunks, means that without additional
protection the user requests could sometimes observe inconsistent buckets:
half-constructed / half-destroyed, effectively breaking the application.

To protect from that the storage keeps track of which user requests are working
with which local buckets at any moment.

The storage makes sure that an in-use bucket won't suddenly begin its deletion;
an in-move bucket won't accept any write-requests during the move; and that a
half-received/destroyed bucket won't accept any requests at all.

The protection is implemented as bucket refs. Buckets simply count read and
write in-progress requests on them, and the storage can decide which type of
new requests can be accepted, and what operations are safe to do on the bucket
(for example, the storage won't start deleting a bucket which still has
read-requests on it).

The central API implementing bucket refs is in `vshard.storage.bucket_ref()`.

## Storage refs and the scheduler

Certain requests might want to access the entire dataset of the storage, all of
its buckets potentially. For instance, `vshard.router.map_callrw()`.

This means every single bucket on the storage must be usable, with no garbage
collection or bucket receipt in progress.

For that the storage offers storage refs. When a storage is referenced, it works
effectively the same as if every single bucket on the storage is referenced and
no new buckets can arrive.

`ref.lua` implements the storage refs.

Storage refs aren't bound to a single request and can exist as their own
entities. This requires protection to prevent the storage refs from piling up
without limits. For that the refs are session-bound (die with the client's
connection) and have deadlines, so a lost client can't block the rebalancing
forever.

Without additional protection storage refs could potentially block any bucket
moves to and from this storage, causing rebalancing starvation.

To prevent this the storage implements a scheduler which orchestrates the two
incompatible operations: storage refs and bucket moves. Both have quotas, and
when one operation type makes too many requests in a row while the other has
queued requests, the first one stops accepting new requests until the other
executes.

`sched.lua` implements the scheduler.

The header comments in both files (ref and sched) explain the design in depth.

## Background services

Much of the storage logic runs in background fibers called services. Their
state is reported by `vshard.storage.info()` via the `service_info` module.

### Rebalancer

Levels the number of buckets between the replicasets according to their weights.

Exactly one instance in the whole cluster runs it (`rebalancer_f`). But if zero
or more than one run, it will not break the storages.

The selection of the rebalancer node is by default automatic, but can be
overridden in the config.

The rebalancer computes the etalon bucket count for each replicaset from the
weights (excluding pinned buckets), and when the disbalance exceeds
`rebalancer_disbalance_threshold`, builds routes - how many buckets which
replicaset must send where - and applies them to the overloaded replicasets.
Those spawn worker fibers which take buckets from the `route_dispenser` and
send them. Receiving is throttled by `rebalancer_max_receiving`, sending
parallelism - by `rebalancer_max_sending`.

### Recovery

Resolves the buckets stuck in the transfer states after failed moves.

When a transfer fails in the middle, the bucket might get stuck `SENDING` on one
side and/or `RECEIVING` on the other. Other cases of various state combinations
are also possible.

The recovery service (`recovery_f`, runs on masters) finds such buckets and asks
the other one or more replicasets about their state, to decide whether the
transfer must be rolled forward (bucket became active on the destination) or
back.

It can be manually triggered via `vshard.storage.recovery_wakeup()` - the tests
use that a lot.

### Garbage collector

Deletes the data of the buckets which were moved away.

The service (`gc_bucket_f`, runs on masters) walks the sharded spaces and
deletes the data of `SENT` and `GARBAGE` buckets, and eventually drops the
buckets themselves. `SENT` buckets are collected after a delay and after all the
replicas of the replicaset have caught up.

It can be manually triggered via `vshard.storage.garbage_collector_wakeup()` -
the tests use that a lot.

### Master sync

Runs on a freshly promoted master (`master_sync_f`).

It collects the vclocks of the other replicas of its own replicaset and waits
until this instance has caught up with all of them, so the new master doesn't
make any decisions based on a stale storage state (like `_bucket` being
outdated).

### Instance watch

It watches the instance's own read-only state (`box.info.ro`) and switches the
storage's master role on and off accordingly.

Runs when the replicaset is configured with automatic master discovery
(`master = 'auto'`), as `instance_watch_f`. Starts/stops the master-only
services and runs the `on_master_enable/disable` triggers.

This is how the storage follows the leader elections and failover coordinators
of Tarantool's core or other upper-level modules without VShard reconfiguration.

### Master search

For the replicasets with `master = 'auto'` the `replicaset_master_search`
service of the replicaset's worker (see `vshard/replicaset.lua`) locates their
current masters.

The storage keeps replicaset objects for the other replicasets, to call them
for bucket transfers and recovery. On the storage it runs in the lazy mode -
only when a call actually needs the master and it is unknown.

## Schema and exports

`schema.lua` manages VShard's own schema on the storage: the `_bucket` space,
the users and functions, the schema upgrades between VShard versions, and
discovery of sharded spaces.

`exports.lua` and `export_log.lua` manage the set of service functions the
storage publishes (like `vshard.storage._call` - the internal RPC entry point
used by routers and other storages). The export log is versioned: for each
VShard version it knows which functions must exist and with which definitions,
so the deployed schema can be upgraded incrementally by the masters.

---

If anything in this document seems outdated from how the code actually works,
then it must be immediately flagged to the user.
