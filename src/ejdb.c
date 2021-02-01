#include <ejdb2/ejdb2.h>
#include <ejdb2/jbl.h>
#include <ejdb2/jql.h>
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>

#define EJDB_DATABASE_META "EJDB*"
#define EJDB_QUERY_META "JQL*"

#define EJDB_LUA_ASSERT(expr)                                                                                          \
  {                                                                                                                    \
    iwrc rc = (expr);                                                                                                  \
    if (rc) {                                                                                                          \
      lua_pushnil(L);                                                                                                  \
      lua_pushstring(L, iwlog_ecode_explained(rc));                                                                    \
      return 2;                                                                                                        \
    }                                                                                                                  \
  }

#define EJDB_LUA_ERROR(expr)                                                                                           \
  {                                                                                                                    \
    iwrc rc = (expr);                                                                                                  \
    if (rc) {                                                                                                          \
      return luaL_error(L, "ejdb: %s", iwlog_ecode_explained(rc));                                                     \
    }                                                                                                                  \
  }

int ejdb_lua_open(lua_State* L) {
  const char* file = luaL_checkstring(L, 1);
  const char* mode_string = luaL_checkstring(L, 2);
  iwkv_openflags mode = 0;
  while (mode_string[0]) {
    if (mode_string[0] == 'w') {
      mode |= IWKV_TRUNC;
    } else if (mode_string[0] == 'r') {
      mode |= IWKV_RDONLY;
    } else
      luaL_error(L, "invalid character in open mode: %c", mode_string[0]);
  }
  EJDB_OPTS opts = {.kv = {.path = file, .oflags = mode}};
  EJDB* db = lua_newuserdata(L, sizeof(EJDB));
  EJDB_LUA_ASSERT(ejdb_open(&opts, db));

  luaL_getmetatable(L, EJDB_DATABASE_META);
  lua_setmetatable(L, -2);
  return 1;
}

int ejdb_lua_ejdb_gc(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  EJDB_LUA_ERROR(ejdb_close(db));
  return 0;
}

int ejdb_lua_query_gc(lua_State* L) {
  JQL* q = luaL_checkudata(L, 1, EJDB_QUERY_META);
  jql_destroy(q);
  return 0;
}

iwrc ejdb_lua_jbl_from_lua(lua_State* L, int index, JBL* output);
iwrc ejdb_lua_jbl_from_lua_set_field(lua_State* L, int index, const char* key, JBL* output) {
  iwrc rc;
  if (lua_istable(L, index)) {
    JBL nested;
    ejdb_lua_jbl_from_lua(L, index, &nested);
    if ((rc = jbl_set_nested(*output, key, nested)))
      return rc;
    jbl_destroy(&nested);
  } else if (lua_isstring(L, index)) {
    jbl_set_string(*output, key, luaL_checkstring(L, index));
  } else if (lua_isboolean(L, index)) {
    jbl_set_bool(*output, key, lua_toboolean(L, index));
  } else {
    luaL_error(L, "invalid type to serialize: %s", lua_typename(L, lua_type(L, index)));
    return 0;
  }
  return 0;
}

iwrc ejdb_lua_jbl_from_lua(lua_State* L, int index, JBL* output) {
  iwrc rc;
  if (lua_istable(L, index)) {
    lua_geti(L, index, 1);
    bool is_array = !lua_isnil(L, -1);
    lua_pop(L, 1);
    if (is_array) {
      if ((rc = jbl_create_empty_array(output)))
        return rc;
      lua_pushnil(L);
      while (lua_next(L, index) != 0) {
        ejdb_lua_jbl_from_lua_set_field(L, lua_gettop(L), NULL, output);
        lua_pop(L, 1);
      }
    } else {
      if ((rc = jbl_create_empty_object(output)))
        return rc;
      lua_pushnil(L);
      while (lua_next(L, index) != 0) {
        const char* key = luaL_checkstring(L, -2);
        ejdb_lua_jbl_from_lua_set_field(L, lua_gettop(L), key, output);
        lua_pop(L, 1);
      }
    }
  } else {
    luaL_error(L, "invalid type to serialize: %s", lua_typename(L, lua_type(L, index)));
    return 0;
  }
}

iwrc ejdb_lua_jbl_create(lua_State* L, int index, JBL* output) {
  iwrc rc;
  if (lua_istable(L, index)) {
    return ejdb_lua_jbl_from_lua(L, index, output);
  } else if (lua_isstring(L, index)) {
    const char* text = luaL_checkstring(L, index);
    if ((rc = jbl_from_json(output, text)))
      return rc;
  } else {
    JBL* src = luaL_checkudata(L, 1, EJDB_QUERY_META);
    return jbl_clone(*src, output);
  }
  return 0;
}

iwrc ejdb_lua_jql_create(lua_State* L, int index, const char* collection, JQL* output) {
  iwrc rc;
  const char* text = luaL_checkstring(L, index);
  if ((rc = jql_create(output, collection, text)))
    return rc;
  return 0;
}

int ejdb_lua_ejdb_put_new(lua_State* L) {
  iwrc rc;
  JBL jbl;
  int64_t id;
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  const char* collection = luaL_checkstring(L, 2);
  EJDB_LUA_ASSERT(ejdb_lua_jbl_create(L, 3, &jbl));
  if ((rc = ejdb_put_new(*db, collection, jbl, &id))) {
    jbl_destroy(&jbl);
    EJDB_LUA_ASSERT(rc);
  }
  jbl_destroy(&jbl);
  lua_pushinteger(L, id);
  return 1;
}

int ejdb_lua_query(lua_State* L) {
  lua_settop(L, 2);
  const char* collection = luaL_checkstring(L, 1);
  JQL* query = lua_newuserdata(L, sizeof(JQL));
  EJDB_LUA_ASSERT(ejdb_lua_jql_create(L, 2, collection, query));
  luaL_getmetatable(L, EJDB_QUERY_META);
  lua_setmetatable(L, -2);
  return 1;
}

int ejdb_lua_query_set(lua_State* L) {
  lua_settop(L, 4);
  JQL* q = luaL_checkudata(L, 1, EJDB_QUERY_META);
  const char* placeholder = luaL_checkstring(L, 2);
  int index = luaL_checkinteger(L, 3);
  if (lua_isinteger(L, 4)) {
    int64_t value = luaL_checkinteger(L, 4);
    EJDB_LUA_ERROR(jql_set_i64(*q, placeholder, index, value));
    return 0;
  } else {
    return luaL_error(L, "invalid type for query:set: %s", lua_typename(L, lua_type(L, 4)));
  }
  return 0;
}

iwrc ejdb_lua_push_jbl(lua_State* L, JBL* jbl) {
  iwrc rc;
  switch (jbl_type(*jbl)) {
  case JBV_NONE: {
    lua_pushnil(L);
  } break;
  case JBV_NULL: {
    lua_pushnil(L);
  } break;
  case JBV_BOOL: {
    lua_pushboolean(L, jbl_get_i32(*jbl));
  } break;
  case JBV_I64: {
    lua_pushinteger(L, jbl_get_i64(*jbl));
  } break;
  case JBV_F64: {
    lua_pushnumber(L, jbl_get_f64(*jbl));
  } break;
  case JBV_STR: {
    lua_pushstring(L, jbl_get_str(*jbl));
  } break;
  case JBV_OBJECT: {
    lua_newtable(L);
    JBL holder = 0;
    JBL_iterator it;
    char* key;
    int klen;
    if ((rc = jbl_create_iterator_holder(&holder)))
      return rc;
    if ((rc = jbl_iterator_init(*jbl, &it)))
      return rc;
    while (jbl_iterator_next(&it, holder, &key, &klen)) {
      lua_pushlstring(L, key, klen);
      ejdb_lua_push_jbl(L, &holder);
      lua_settable(L, -3);
    }
    jbl_destroy(&holder);
  } break;
  case JBV_ARRAY: {
    lua_newtable(L);
    lua_pushliteral(L, "array");
    lua_setfield(L, -2, "__type");
    JBL holder = 0;
    JBL_iterator it;
    int klen;
    if ((rc = jbl_create_iterator_holder(&holder)))
      return rc;
    if ((rc = jbl_iterator_init(*jbl, &it)))
      return rc;
    while (jbl_iterator_next(&it, holder, NULL, &klen)) {
      lua_pushinteger(L, klen + 1);
      ejdb_lua_push_jbl(L, &holder);
      lua_settable(L, -3);
    }
    jbl_destroy(&holder);
  } break;
  }
  return 0;
}

static iwrc ejdb_lua_visitor(EJDB_EXEC* ctx, const EJDB_DOC doc, int64_t* step) {
  iwrc rc;
  lua_State* L = ctx->opaque;
  lua_pushvalue(L, 3);
  lua_pushinteger(L, doc->id);
  if ((rc = ejdb_lua_push_jbl(L, &(doc->raw))))
    return rc;
  lua_call(L, 2, 0);
  return 0;
}

int ejdb_lua_ejdb_exec(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  JQL* q = luaL_checkudata(L, 2, EJDB_QUERY_META);
  luaL_checktype(L, 3, LUA_TFUNCTION);
  EJDB_EXEC ux = {.db = *db, .q = *q, .visitor = ejdb_lua_visitor, .opaque = L};
  EJDB_LUA_ASSERT(ejdb_exec(&ux));
  lua_pushinteger(L, ux.cnt);
  return 1;
}

int luaopen_ejdb(lua_State* L) {
  luaL_newmetatable(L, EJDB_DATABASE_META);
  lua_pushcfunction(L, ejdb_lua_ejdb_gc);
  lua_setfield(L, -2, "__gc");
  lua_pushcfunction(L, ejdb_lua_ejdb_put_new);
  lua_setfield(L, -2, "put_new");
  lua_pushcfunction(L, ejdb_lua_ejdb_exec);
  lua_setfield(L, -2, "exec");
  lua_setfield(L, -1, "__index");

  luaL_newmetatable(L, EJDB_QUERY_META);
  lua_pushcfunction(L, ejdb_lua_query_gc);
  lua_setfield(L, -2, "__gc");
  lua_pushcfunction(L, ejdb_lua_query_set);
  lua_setfield(L, -2, "set");
  lua_setfield(L, -1, "__index");

  EJDB_LUA_ERROR(ejdb_init());
  lua_newtable(L);
  lua_pushcfunction(L, ejdb_lua_open);
  lua_setfield(L, -2, "open");
  lua_pushcfunction(L, ejdb_lua_query);
  lua_setfield(L, -2, "query");
  return 1;
}
