#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ReplicaCfg {
	const char *uuid;
	uint32_t uuid_len;
	const char *uri;
	uint32_t uri_len;
	const char *name;
	uint32_t name_len;
	bool is_master;
};

struct ReplicaSetCfg {
	uint32_t replica_count;
	struct ReplicaCfg *replicas;
	const char *uuid;
	uint32_t uuid_len;
};

struct Config {
	uint32_t bucket_count;
	uint32_t replicaSets_count;
	struct ReplicaSetCfg *replicaSets;
};

static inline void
config_free(struct Config *config)
{
	for (uint32_t i = 0; i < config->replicaSets_count; ++i) {
		struct ReplicaSetCfg *rs = &config->replicaSets[i];
		free(rs->replicas);
		rs->replicas = NULL;
	}
	free(config->replicaSets);
	config->replicaSets = NULL;
}

#ifdef __cplusplus
}
#endif
