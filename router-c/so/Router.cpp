/*
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "module.h"
#include "Router.hpp"

Router staticRouter("static_router");

///////////////////////////// *** Replica *** //////////////////////////////////

Replica::Replica(Connector<Buf_t, Net_t> &connector, const std::string &name,
		 const std::string &uuid, const std::string &address,
		 size_t port, size_t weight) : m_Connector(connector),
					       m_Name(name), m_Uuid(uuid),
					       m_Address(address), m_Port(port),
					       m_Weight(weight),
					       m_Timeout(DEFAULT_NETWORK_TIMEOUT),
					       m_Master(this), m_Connection(m_Connector)
{
}

Replica::~Replica()
{
	if (isConnected())
		m_Connector.close(m_Connection);
}

void
Replica::setMaster(Replica *master)
{
	m_Master = master;
}

bool
Replica::isMaster() const
{
	return m_Master == this;
}

int
Replica::connect()
{
	return m_Connector.connect(m_Connection, m_Address, m_Port);
}

bool
Replica::isConnected()
{
	rid_t ping = m_Connection.ping();
	if (m_Connector.wait(m_Connection, ping, m_Timeout) != 0)
		return false;
	assert(m_Connection.futureIsReady(ping));
	return true;
}

template <class T>
void
Replica::callAsync(const std::string &func, const T &args, rid_t *future)
{
	*future = m_Connection.call(func, args);
}

template <class T>
int
Replica::callSync(const std::string &func,  const T &args, Response<Buf_t> &result)
{
	rid_t future = m_Connection.call(func, args);
	if (m_Connector.wait(m_Connection, future, m_Timeout) != 0)
		return -1;
	assert(m_Connection.futureIsReady(future));
	result = *m_Connection.getResponse(future);
	return 0;
}

std::string
Replica::toString()
{
	return m_Name + "(" + m_Address + ")";
}

///////////////////////////// *** ReplicaSet *** ///////////////////////////////

ReplicaSet::ReplicaSet(const std::string &uuid, size_t weight) :
	m_Uuid(uuid), m_Weight(weight), m_BucketCount(0), m_Master(nullptr)
{
}

ReplicaSet::~ReplicaSet()
{
}

void
ReplicaSet::setMaster(Replica *master)
{
	for (auto r : m_Replicas) {
		r.second->setMaster(master);
	}
	m_Master = master;
}

void
ReplicaSet::addReplica(Replica *replica)
{
	assert(replica != nullptr);
	m_Replicas.insert({replica->m_Uuid, replica});
}

int
ReplicaSet::connectAll()
{
	for (auto itr : m_Replicas) {
		Replica *r = itr.second;
		if (r->connect() != 0) {
			say_error("Failed to connect to replica %s",
				  r->toString().c_str());
		}
		say_info("Connected replica %s", r->toString().c_str());
	}
	return 0;
}

template <class T>
int
ReplicaSet::replicaCall(const std::string &uuid,
			const std::string &func, const T &args,
			Response<Buf_t> &res)
{
	auto replica = m_Replicas.find(uuid);
	if (replica == m_Replicas.end()) {
		say_error("Can't invoke call: wrong replica uuid %s", uuid.c_str());
		return -1;
	}
	return replica->second->callSync(func, args, res);
}

template <class T>
int
ReplicaSet::call(const std::string &func, const T &args, Response<Buf_t> &res)
{
	assert(m_Master != nullptr && "Master hasn't been assigned!");
	return m_Master->callSync(func, args, res);
}

std::string
ReplicaSet::toString()
{
	return std::string("ReplicaSet(") + m_Uuid + ")";
}

///////////////////////////// *** Router *** ///////////////////////////////////

Router::Router(const std::string &name) : m_Name(name), m_TotalBucketCount(0)
{
}

Router::~Router()
{
	reset();
}

/**
 * GOD PLEASE FIXME
 */
int
parseURI(const char *uri, size_t uri_len, std::pair<std::string, int> &res)
{
	char delim[] = ":";
	char *uri_copy = (char *) malloc(uri_len + 1);
	if (uri_copy == NULL)
		return -1;
	memcpy(uri_copy, uri, uri_len);
	uri_copy[uri_len] = '\0';
	char *addr = strtok(uri_copy, delim);
	if (addr == NULL) {
		free(uri_copy);
		return -1;
	}
	res.first = std::string(addr);
	char *port = strtok(NULL, delim);
	if (port == NULL) {
		free(uri_copy);
		return -1;
	}
	res.second = atoi(port);
	free(uri_copy);
	return 0;
}

int
Router::addReplicaSet(struct ReplicaSetCfg *replicaSetCfg)
{
	std::vector<Replica *> replicas;
	Replica *master = nullptr;
	auto old_rs = m_ReplicaSets.find(std::string(replicaSetCfg->uuid,
						     replicaSetCfg->uuid_len));
	if (old_rs != m_ReplicaSets.end()) {
		say_error("Replicaset with given uuid (%.*s) already exists..",
			  replicaSetCfg->uuid_len, replicaSetCfg->uuid);
		say_error("Re-creating %s", old_rs->second->toString().c_str());
		removeReplicaSet(old_rs->second);
	}
	for (uint32_t i = 0; i < replicaSetCfg->replica_count; ++i) {
		ReplicaCfg *replicaCfg = &replicaSetCfg->replicas[i];
		std::pair<std::string, int> addr;
		if (parseURI(replicaCfg->uri, replicaCfg->uri_len, addr) != 0) {
			say_error("Failed to parse URI %.*s",
				  replicaCfg->uri_len, replicaCfg->uri);
			for (auto r : replicas) {
				delete r;
			}
			return -1;
		}
		say_error("Parsed URI %s %d", addr.first.c_str(), addr.second);
		Replica *replica = new (std::nothrow) Replica(m_Connector,
			std::string(replicaCfg->name, replicaCfg->name_len),
			std::string(replicaCfg->uuid, replicaCfg->uuid_len),
			addr.first, addr.second, 0);
		if (replica == nullptr) {
			for (auto r : replicas) {
				delete r;
			}
			return -1;
		}
		if (replicaCfg->is_master) {
			master = replica;
		}
		replicas.push_back(replica);
	}
	assert(master != nullptr);
	ReplicaSet *rs = new (std::nothrow) ReplicaSet(
		std::string(replicaSetCfg->uuid, replicaSetCfg->uuid_len), 0);
	if (rs == nullptr) {
		for (auto r : replicas) {
			delete r;
		}
		return -1;
	}
	for (auto r : replicas) {
		if (rs->m_Replicas.find(r->m_Uuid) != rs->m_Replicas.end()) {
			say_error("Failed to add replica %s to %s");
			return -1;
		}
		say_info("Added replica %s to %s", r->toString().c_str(),
			 rs->toString().c_str());
		rs->addReplica(r);
	}
	say_info("Set master %s for %s", master->toString().c_str(),
		 rs->toString().c_str());
	rs->setMaster(master);
	m_ReplicaSets.insert({rs->m_Uuid, rs});
	return 0;
}


void
Router::removeReplicaSet(ReplicaSet *rs)
{
	for (auto itr : rs->m_Replicas) {
		Replica *r = itr.second;
		rs->m_Replicas.erase(itr.first);
		delete r;
		r = nullptr;
	}
	m_ReplicaSets.erase(rs->m_Uuid);
	delete rs;
}

void
Router::connectAll()
{
	for (auto itr : m_ReplicaSets) {
		ReplicaSet *rs = itr.second;
		(void) rs->connectAll();
		say_info("Connected replicaset %s", rs->toString().c_str());
	}
}

ReplicaSet *
Router::setBucket(size_t bucket_id, const std::string &rs_uuid)
{
	auto rs = m_ReplicaSets.find(rs_uuid);
	if (rs == m_ReplicaSets.end()) {
		/* NO_ROUTE_TO_BUCKET */
		return nullptr;
	}
	auto old_rs = m_Routes.find(bucket_id);
	if (old_rs->second != rs->second) {
		if (old_rs != m_Routes.end()) {
			old_rs->second->m_BucketCount--;
		}
		rs->second->m_BucketCount++;
	}
	m_Routes[bucket_id] = rs->second;
	return rs->second;
}

void
Router::resetBucket(size_t bucket_id)
{
	auto rs = m_Routes.find(bucket_id);
	if (rs != m_Routes.end()) {
		rs->second->m_BucketCount--;
	}
	m_Routes.erase(bucket_id);
}

ReplicaSet *
Router::discoveryBucket(size_t bucket_id)
{
	auto rs = m_Routes.find(bucket_id);
	if (rs != m_Routes.end()) {
		return rs->second;
	}
	for (auto itr : m_ReplicaSets) {
		ReplicaSet *replicaSet = itr.second;
		Response<Buf_t> response;
		if (replicaSet->call("vshard.storage.bucket_stat",
				    std::make_tuple<int>(bucket_id),
				    response) != 0) {
			say_error("Failed to invoke call on %s",
				  replicaSet->toString());
			continue;
		}
		if (! isResponseError(response))
			return setBucket(bucket_id, replicaSet->m_Uuid);
	}
	say_error("No route to bucket %d has been found", bucket_id);
	/* NO_ROUTE_TO_BUCKET */
	return nullptr;
}

int
Router::discovery()
{
//	struct iterator_arg {
//		rid_t future;
//		std::tuple arg;
//	};
//	/* rs uuid -- > future request*/
//	std::map<std::string, iterator_arg> iterators;
//	while (true) {
//		for (auto rs : m_ReplicaSets) {
//			auto iter = iterators.find(rs.first);
//			if (iter == iterators.end) {
//				iterators[rs.first] = iterator_arg{0, std::nullopt};
//			}
//		}
//		for (auto iter : iterators) {
//			auto rs = m_ReplicaSets[iter.first];
//			if (rs == m_ReplicaSets.end()) {
//				/* Replicaset was removed during discovery. */
//				iterators.erase(iter);
//				continue;
//			}
//			rs.call("vshard.storage.buckets_discovery", &iter.second.arg);
//		}
//		m_Connector.waitAny();
//	}
	return 0;
}

int
Router::call(size_t bucket_id, /*enum CallMode mode,*/ const char *func,
	     const char *args, const char *args_end)
{
	if (bucket_id > m_TotalBucketCount) {
		say_error("Bucked id %d is out of range (%d)", bucket_id,
			  m_TotalBucketCount);
		return -1;
	}
	ReplicaSet *replicaset = discoveryBucket(bucket_id);
	if (replicaset == nullptr)
		return -1;
	Response<Buf_t> response;
	auto tuple = std::make_tuple(bucket_id, /*CallModeStr[mode],*/ func, args);
	if (replicaset->call("vshard.storage.call", tuple, response) != 0)
		return -1;
	return 0;
}

void
Router::reset()
{
	m_TotalBucketCount = 0;
	m_Routes.clear();
	for (auto itr : m_ReplicaSets)
		removeReplicaSet(itr.second);
}

void
Router::setBucketCount(uint32_t bucketCount)
{
	m_TotalBucketCount = bucketCount;
}
