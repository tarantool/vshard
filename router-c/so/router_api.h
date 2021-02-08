#pragma once

#include "config.h"

#ifdef __cplusplus
extern "C" {
#endif

void
router_build(struct Config *cfg);

int
router_call(int bucket_id, const char *func, const char *args,
	    const char *args_end);

#ifdef __cplusplus
}
#endif
