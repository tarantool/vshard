#include "router_api.h"

#include <math.h>

#include <module.h>
#include <msgpuck.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include "config.h"

void stackDump(lua_State *L)
{
	int i;
	int top = lua_gettop(L);
	for (i = 1; i <= top; i++)
	{  /* repeat for each level */
		int t = lua_type(L, i);
		switch (t)
		{

			case LUA_TSTRING:  /* strings */
				printf("`%s'", lua_tostring(L, i));
				break;

			case LUA_TBOOLEAN:  /* booleans */
				printf(lua_toboolean(L, i) ? "true" : "false");
				break;

			case LUA_TNUMBER:  /* numbers */
				printf("%g", lua_tonumber(L, i));
				break;

			default:  /* other values */
				printf("%s", lua_typename(L, t));
				break;

		}
		printf("  ");  /* put a separator */
	}
	printf("\n");  /* end the listing */
}

void dumbDumpMp(const char* args, const char *args_end)
{
	char tmp[4096];
	char dig[17] = "0123456789abcdef";
	char *pt = tmp;
	const char* p = args;
	for (; p < args_end; ++p)
	{
		uint8_t c = *p;
		*pt = dig[c / 16];
		pt++;
		*pt = dig[c % 16];
		pt++;
		*pt = ' ';
		pt++;
	}
	*pt = 0;
	FILE* f = fopen("/home/a.lyapunov/Work/vshard/benchmark/out.txt", "a+");
	fprintf(f, "%s\n", tmp);
	fclose(f);
}

struct box_function_ctx {
	struct port *port;
};

typedef struct box_function_ctx box_function_ctx_t;


int lua_call_mp(box_function_ctx_t *ctx, const char *func,
		const char *args, const char *args_end,
		const char **res, const char **res_end)
{
	//thread_local char megabuf[16 * 1024];
	static char megabuf[16 * 1024];
	*res = megabuf;
	*res_end = megabuf;

	lua_State *L = luaT_state();
	if (L == NULL)
		return -1;

	lua_getglobal(L, func);
	uint32_t arg_count = mp_decode_array(&args);
	for (uint32_t i = 0; i < arg_count; i++)
	{
		enum mp_type type = mp_typeof(*args);
		const char* str;
		uint32_t len;
		switch (type)
		{
			case MP_UINT:
				lua_pushnumber(L, mp_decode_uint(&args));
				break;
			case MP_INT:
				lua_pushnumber(L, mp_decode_int(&args));
				break;
			case MP_FLOAT:
				lua_pushnumber(L, mp_decode_float(&args));
				break;
			case MP_DOUBLE:
				lua_pushnumber(L, mp_decode_double(&args));
				break;
			case MP_BOOL:
				lua_pushboolean(L, mp_decode_bool(&args));
				break;
			case MP_STR:
				str = mp_decode_str(&args, &len);
				lua_pushlstring(L, str, len);
				break;
			default:
				fprintf(stderr, "NOT IMPLEMENTED(1)\n");
				abort();
		}
	}

	if (lua_pcall(L, arg_count, arg_count, 0) != 0)
	{
		printf("ERROR: %s\n", lua_tostring(L, -1));
		lua_close(L);
		return -1;
	}

	char* p = megabuf;

	uint32_t real_num_ret = arg_count;
	while (real_num_ret > 0 && lua_isnil(L, -1))
	{
		lua_pop(L, 1);
		real_num_ret--;
	}
	p = mp_encode_array(p, real_num_ret);
	for (uint32_t i = 0; i < real_num_ret; i++)
	{
		int idx = -(int)real_num_ret + i;
		if (lua_isnumber(L, idx))
		{
			double v = lua_tonumber(L, idx);
			if (v != floor(v))
			{
				p = mp_encode_double(p, v);
			}
			else if (v >= 0)
			{
				p = mp_encode_uint(p, (uint64_t)v);
			}
			else
			{
				p = mp_encode_int(p, (int64_t)v);
			}
		}
		else if (lua_isstring(L, idx)) {
			size_t sz;
			const char *str = lua_tolstring(L, idx, &sz);
			p = mp_encode_str(p, str, sz);
		}
		else if (lua_isboolean(L, idx)) {
			bool b = lua_toboolean(L, idx);
			p = mp_encode_bool(p, b);
		}
		else {
			fprintf(stderr, "NOT IMPLEMENTED(2)\n");
			abort();
		}
	}

	lua_settop(L, -(int)real_num_ret - 1);

	//stackDump(L);

	*res_end = p;
	return 0;
}

int echo(box_function_ctx_t *ctx,
	 const char *args, const char *args_end)
{
	const char *res, *res_end;
	int rc = lua_call_mp(ctx, "bench_call_echo",
			     args, args_end,
			     &res, &res_end);
	if (rc != 0)
		return rc;

	return box_return_mp(ctx, res, res_end);
}


//int call(box_function_ctx_t *ctx, const char *args, const char *args_end)
//{
//	verify(mp_typeof(*args) == MP_ARRAY, "WRONG FORMAT(1)");
//	uint32_t arr_size = mp_decode_array(&args);
//	verify(arr_size == 4, "WRONG FORMAT(2)");
//
//	verify(mp_typeof(*args) == MP_UINT, "WRONG FORMAT(3)");
//	uint64_t vbucket = mp_decode_uint(&args);
//
//	verify(mp_typeof(*args) == MP_STR, "WRONG FORMAT(4)");
//	const char* mode;
//	uint32_t mode_len;
//	mode = mp_decode_str(&args, &mode_len);
//
//	bool ro = mode_len == 4 && memcmp(mode, "read", mode_len) == 0;
//	bool rw = mode_len == 5 && memcmp(mode, "write", mode_len) == 0;
//	verify(ro || rw, "WRONG MODE");
//
//	verify(mp_typeof(*args) == MP_STR, "WRONG FORMAT(5)");
//	const char* func;
//	uint32_t func_len;
//	func = mp_decode_str(&args, &func_len);
//	char real_func[256];
//	verify(func_len < sizeof(real_func) - 1, "FUNCNAME TOO LONG");
//	memcpy(real_func, func, func_len);
//	real_func[func_len] = 0;
//
//	verify(mp_typeof(*args) == MP_ARRAY, "WRONG FORMAT(6)");
//	arr_size = mp_decode_array(&args);
//	verify(arr_size == 1, "WRONG FORMAT(7)");
//
//	verify(mp_typeof(*args) == MP_ARRAY, "WRONG FORMAT(8)");
//
//	const char *res, *res_end;
//	if (0) {
//		res = args;
//		res_end = args_end;
//	} else {
//		int rc = lua_call_mp(ctx, real_func,
//				     args, args_end,
//				     &res, &res_end);
//		if (rc != 0)
//			return rc;
//	}
//
//	static char megabuf[16 * 1024];
//	char* p = megabuf;
//	p = mp_encode_array(p, 4);
//	p = mp_encode_bool(p, true);
//	memcpy(p, res, res_end - res);
//	p += res_end - res;
//	p = mp_encode_nil(p);
//	p = mp_encode_nil(p);
//
//	return box_return_mp(ctx, megabuf, p);
//
//};


enum {
	FUNC_NAME_LEN_MAX = 256
};

static void
parseReplicaCfg(const char **msgpack, struct ReplicaCfg *replica)
{
	assert(mp_typeof(**msgpack) == MP_STR);
	replica->uuid = mp_decode_str(msgpack, &replica->uuid_len);
	uint32_t replica_map = mp_decode_map(msgpack);
	say_info("replica map %d", replica_map);
	for (uint32_t i = 0; i < replica_map; ++i) {
		assert(mp_typeof(**msgpack) == MP_STR);
		uint32_t str_len = 0;
		const char *key = mp_decode_str(msgpack, &str_len);
		if (str_len == 3 && strncmp(key, "uri", 3) == 0) {
			replica->uri =
				mp_decode_str(msgpack, &replica->uri_len);
			continue;
		}
		if (str_len == 4 && strncmp(key, "name", 4) == 0) {
			replica->name =
				mp_decode_str(msgpack, &replica->name_len);
			continue;
		}
		if (str_len == 6 && strncmp(key, "master", 5) == 0) {
			replica->is_master =
				mp_decode_bool(msgpack);
			continue;
		}
		say_error("Unknown replica cfg key %.*s",
			  str_len, key);
	}
	say_info("Decoded replica: name=%.*s, uri=%.*s, uuid=%.*s, master=%d",
		 replica->name_len, replica->name,
		 replica->uri_len, replica->uri,
		 replica->uuid_len, replica->uuid,
		 replica->is_master);
}

static int
parseReplicaSetCfg(const char **msgpack, struct ReplicaSetCfg *rs)
{
	assert(mp_typeof(**msgpack) == MP_STR);
	rs->uuid = mp_decode_str(msgpack, &rs->uuid_len);
	assert(mp_typeof(**msgpack) == MP_MAP);
	uint32_t map_size = mp_decode_map(msgpack);
	assert(mp_typeof(**msgpack) == MP_STR);
	uint32_t str_len;
	const char *replicas = mp_decode_str(msgpack, &str_len);
	assert(str_len == 8);/* "replicas" */
	assert(mp_typeof(**msgpack) == MP_MAP);
	rs->replica_count = mp_decode_map(msgpack);
	say_info("replica count %u", rs->replica_count);
	rs->replicas = calloc(rs->replica_count, sizeof(struct ReplicaCfg));
	if (rs->replicas == NULL) {
		say_error("Malloc failed!");
		return -1;
	}
	for (uint32_t j = 0; j < rs->replica_count; ++j)
		parseReplicaCfg(msgpack, &rs->replicas[j]);
	return 0;
}

static int
parseShardingCfg(const char **msgpack, struct Config *cfg)
{
	assert(mp_typeof(**msgpack) == MP_MAP);
	cfg->replicaSets_count = mp_decode_map(msgpack);
	say_info("NUMBER OF REPLICASETS %u", cfg->replicaSets_count);
	cfg->replicaSets = calloc(cfg->replicaSets_count, sizeof(struct ReplicaSetCfg));
	if (cfg->replicaSets == NULL) {
		say_error("Malloc failed!");
		return -1;
	}
	for (uint32_t i = 0; i < cfg->replicaSets_count; ++i) {
		say_info("REPLICASET %u", i);
		if (parseReplicaSetCfg(msgpack, &cfg->replicaSets[i]) != 0)
			return -1;
	}
	return 0;
}

/** C API entry points. */
int
router_cfg(box_function_ctx_t *ctx, const char *args,
	   const char *args_end)
{
	struct Config cfg;
	memset(&cfg, 0, sizeof(struct Config));
	say_info("----- router_cfg() -----");
	//say_info("arg %s", mp_type_strs[mp_typeof(*args)]);
	assert(mp_typeof(*args) == MP_ARRAY);
	uint32_t arr_size = mp_decode_array(&args);
	say_info("NUMBER OF ARRAY ARGS %u", arr_size);
	assert(mp_typeof(*args) == MP_MAP);
	uint32_t map_size = mp_decode_map(&args);
	say_info("NUMBER OF MAP ARGS %u", map_size);
	for (uint32_t i = 0; i < map_size; ++i) {
		assert(mp_typeof(*args) == MP_STR);
		uint32_t str_len;
		const char *key = mp_decode_str(&args, &str_len);
		if (str_len == 8 && strncmp(key, "sharding", 8) == 0) {
			if (parseShardingCfg(&args, &cfg) != 0) {
				config_free(&cfg);
				return -1;
			}
			continue;
		}
		if (str_len == 12 && strncmp(key, "bucket_count", 12) == 0) {
			assert(mp_typeof(*args) == MP_UINT);
			cfg.bucket_count = mp_decode_uint(&args);
			continue;
		}
	}
	router_build(&cfg);
	config_free(&cfg);
	say_info("Router has been configured!");
	return 0;
}

/**
 * Arguments format:
 * @param bucket_id Integer value representing bucket id.
 * @param mode String value representing call mode (read or write).
 * @param function String value representing function to call on storage.
 * @param args Table of arguments to be passed to @function on storage.
 *
 * On client side call of this function looks like:
 * conn:call('storagec.vshard_router_callrw', {bucket_id, func, args})
 *
 * Where conn is casual netbox connection.
 */
int
router_callrw(box_function_ctx_t *ctx, const char *args, const char *args_end)
{
	(void) ctx;
	say_info("----- router_callrw() -----");
	if (mp_typeof(*args) != MP_ARRAY) {
		say_error("Wrong arguments format: array is expected");
		return -1;
	}
	uint32_t arr_size = mp_decode_array(&args);
	if (arr_size != 4) {
		say_error("Wrong arguments format: 4 arguments expected, "\
			  "but got %d", arr_size);
		return -1;
	}

	if (mp_typeof(*args) != MP_UINT) {
		say_error("Wrong arguments format: bucket_id is expected "\
			  "to be integer");
		return -1;
	}
	uint64_t bucket_id = mp_decode_uint(&args);

	if (mp_typeof(*args) != MP_STR) {
		say_error("Wrong arguments format: mode is expected "\
			  "to be string");
		return -1;
	}
	const char* mode;
	uint32_t mode_len;
	mode = mp_decode_str(&args, &mode_len);

	bool ro = mode_len == 4 && memcmp(mode, "read", mode_len) == 0;
	bool rw = mode_len == 5 && memcmp(mode, "write", mode_len) == 0;
	if (!ro && !rw) {
		say_error("Wrong arguments format: mode is expected "\
			  "to be 'read' or 'write'");
		return -1;
	}

	if (mp_typeof(*args) != MP_STR) {
		say_error("Wrong arguments format: function name is expected "\
			  "to be string");
		return -1;
	}
	const char* func;
	uint32_t func_len;
	func = mp_decode_str(&args, &func_len);
	char real_func[FUNC_NAME_LEN_MAX + 1];
	if (func_len > FUNC_NAME_LEN_MAX) {
		say_error("Wrong arguments format: function name is too long %d. "\
			  "Max length is %d", func_len, FUNC_NAME_LEN_MAX);
		return -1;
	}
	memcpy(real_func, func, func_len);
	real_func[func_len] = '\0';

	if (mp_typeof(*args) != MP_ARRAY) {
		say_error("Wrong arguments format: function arguments must "\
			  "constitute an array");
		return -1;
	}
	arr_size = mp_decode_array(&args);
	say_info("----- array size is %d -----", arr_size);

	if (router_call(bucket_id, real_func, args, args_end) != 0) {
		say_error("Router call failed!");
		return -1;
	}
//	if (mp_typeof(*args) == MP_ARRAY, "WRONG FORMAT(8)");
//	uint32_t arr_size = mp_decode_array(&args);
//	verify(arr_size == 4, "WRONG FORMAT(2)");

	return 0;
}


/* internal function */
static int
ckit_func(struct lua_State *L)
{
  if (lua_gettop(L) < 2)
    luaL_error(L, "Usage: ckit_func(a: number, b: number)");

  int a = lua_tointeger(L, 1);
  int b = lua_tointeger(L, 2);

  lua_pushinteger(L, a + b);
  return 1; /* one return value */
}

/* exported function */
LUA_API int
luaopen_ckit_lib(lua_State *L)
{
  /* result returned from require('ckit.lib') */
  lua_newtable(L);
  static const struct luaL_Reg meta [] = {
    {"func", ckit_func},
    {NULL, NULL}
  };
  luaL_register(L, NULL, meta);
  return 1;
}