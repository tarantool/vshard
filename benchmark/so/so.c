#include <math.h>

#include <module.h>

#include <msgpuck.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

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

/*
int
port_c_add_mp(struct port *base, const char *mp, const char *mp_end);

extern void
port_c_dump_lua(struct port *port, struct lua_State *L, bool is_flat);
 */

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
		else if (lua_isstring(L, idx))
		{
			size_t sz;
			const char *str = lua_tolstring(L, idx, &sz);
			p = mp_encode_str(p, str, sz);
		}
		else if (lua_isboolean(L, idx))
		{
			bool b = lua_toboolean(L, idx);
			p = mp_encode_bool(p, b);
		}
		else
		{
			fprintf(stderr, "NOT IMPLEMENTED(2)\n");
			abort();
		}
//		port_c_add_mp(ctx->port, megabuf, p);
//		p = megabuf;

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

static void verify(bool expr, const char *mess)
{
	if (!expr)
	{
		fprintf(stderr, "%s\n", mess);
		abort();
	}
}

int call(box_function_ctx_t *ctx,
         const char *args, const char *args_end)
{
	verify(mp_typeof(*args) == MP_ARRAY, "WRONG FORMAT(1)");
	uint32_t arr_size = mp_decode_array(&args);
	verify(arr_size == 4, "WRONG FORMAT(2)");

	verify(mp_typeof(*args) == MP_UINT, "WRONG FORMAT(3)");
	uint64_t vbucket = mp_decode_uint(&args);

	verify(mp_typeof(*args) == MP_STR, "WRONG FORMAT(4)");
	const char* mode;
	uint32_t mode_len;
	mode = mp_decode_str(&args, &mode_len);

	bool ro = mode_len == 4 && memcmp(mode, "read", mode_len) == 0;
	bool rw = mode_len == 5 && memcmp(mode, "write", mode_len) == 0;
	verify(ro || rw, "WRONG MODE");

	verify(mp_typeof(*args) == MP_STR, "WRONG FORMAT(5)");
	const char* func;
	uint32_t func_len;
	func = mp_decode_str(&args, &func_len);
	char real_func[256];
	verify(func_len < sizeof(real_func) - 1, "FUNCNAME TOO LONG");
	memcpy(real_func, func, func_len);
	real_func[func_len] = 0;

	verify(mp_typeof(*args) == MP_ARRAY, "WRONG FORMAT(6)");
	arr_size = mp_decode_array(&args);
	verify(arr_size == 1, "WRONG FORMAT(7)");

	verify(mp_typeof(*args) == MP_ARRAY, "WRONG FORMAT(8)");

	const char *res, *res_end;
	if (0) {
		res = args;
		res_end = args_end;
	} else {
		int rc = lua_call_mp(ctx, real_func,
				     args, args_end,
				     &res, &res_end);
		if (rc != 0)
			return rc;
	}

	static char megabuf[16 * 1024];
	char* p = megabuf;
	p = mp_encode_array(p, 4);
	p = mp_encode_bool(p, true);
	memcpy(p, res, res_end - res);
	p += res_end - res;
	p = mp_encode_nil(p);
	p = mp_encode_nil(p);

	return box_return_mp(ctx, megabuf, p);

};


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