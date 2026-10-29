# Development of the router

This document explains more details about the router part of VShard. It gives
more context when working specifically with `vshard.router` code and its logic.

## Overview

The router code is located in `vshard/router/`, its core logic in `init.lua`
file. It heavily relies on `vshard/replicaset.lua` - the module implementing the
replicaset and replica connectors.

A router is stateless - it stores no data and can be restarted at any moment.

An instance can host several routers: the static one, configured via
`vshard.router.cfg()` and used by the module-level API, plus any number of
extra ones created by `vshard.router.new(name, cfg)`. Internally all of them
are kept in `M.routers`, and the static one is `M.static_router`.

The router instance has a bucket routing table - `router.route_map`, mapping a
bucket id to a replicaset object. It is a cache. It is filled and fixed:
- by the bucket discovery service, which walks the storages and downloads their
  bucket sets in chunks;
- by `bucket_resolve()` asking the storages about a specific unknown bucket;
- from the storage errors like `WRONG_BUCKET` which can carry the destination
  replicaset of the moved bucket.

## Background services

Much of the router logic runs in background fibers called services. Their state
is reported by `vshard.router.info()` via the `service_info` module.

### Bucket discovery

Walks the storages and downloads their bucket sets to fill the routing table.

### Failover

Per-replicaset and per-replica services (`replicaset_failover`,
`replica_failover`) which ping the connections, track the replica health and
replication lag, and redirect the requests to the best available replica by
priority (zone weights, health, and load balancing choice). When the current
replica degrades, the requests switch to the next one, and back when it
recovers.

Temporarily failing replicas are put into a backoff state so the requests aren't
hammering known-to-be-dead nodes.

### Master search

The master search service (`replicaset_master_search`) locates the current
master of a replicaset when the config chooses automatic master discovery
(`master = 'auto'`) instead of naming the master explicitly. The routers then
follow the master changes done on the storages (for example, by the failover
coordinators or automatic leader elections from Tarantool's core) without a
reconfiguration.

## Calls

`vshard.router.call(bucket_id, mode, func, args, opts)` and its shortcuts
`callro`, `callrw`, `callre`, `callbro`, `callbre` are the main API of the
router.

These calls resolve the bucket ID into a replicaset and execute the function
there safely, making sure the bucket won't move or get deleted while the
request is being executed.

Write requests always go to the master. Read requests go to the highest-priority
available replica chosen by whatever the specific variation of the call-function
requires (balancing, only-replicas, replicas or master, etc).

The calls retry on the retriable errors (moved bucket, no connection, timeout
too small, and so on) until the user timeout expires, updating the route map
on the way.

The storage-side counterpart of the calls is `vshard.storage.call` -
see `docs/agents/storage-dev.md` if need a dive.

## Map-Reduce

`vshard.router.map_callrw(func, args, opts)` is the consistent map-reduce. It
is used when the user doesn't know which bucket exactly needs to be accessed, or
wants to access very many or even all of them on multiple replicasets.

It works in stages:
- Ref - take storage refs on all (or the involved) replicasets, so no bucket can
  move while the request is in progress;
- Map - execute the function on the masters;
- Reduce - collect the results into a map of replicaset id -> result.

With various options, like `bucket_ids`, the call gives a variety of choices
which replicasets to access, and whether all or a subset of buckets will be
accessed.

The storage-side counterpart of the refs is `vshard.storage.ref` -
see `docs/agents/storage-dev.md` if need a dive.

---

If anything in this document seems outdated from how the code actually works,
then it must be immediately flagged to the user.
