#include "module.h"

#include "router_api.h"
#include "Router.hpp"

#include <string>

int
router_call(int bucket_id, const char *func, const char *args,
	    const char *args_end)
{
	say_info("Bucked id=%d, func=%s\n", bucket_id, func);
	return staticRouter.call(bucket_id, func, args, args_end);
}

void
router_build(struct Config *cfg)
{
	staticRouter.reset();
	for (uint32_t i = 0; i < cfg->replicaSets_count; ++i) {
		struct ReplicaSetCfg *rs = &cfg->replicaSets[i];
		if (staticRouter.addReplicaSet(rs) != 0) {
			say_error("Failed to add replicaset %.*s",
				  rs->uuid_len, rs->uuid);
		}
	}
	say_info("Set bucket count=%u", cfg->bucket_count);
	staticRouter.setBucketCount(cfg->bucket_count);
	staticRouter.connectAll();
}
