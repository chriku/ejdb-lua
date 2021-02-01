/// Simple binding for ejdb to lua
// @module ejdb

#include <ejdb2/ejdb2.h>
#include <ejdb2/jbl.h>
#include <ejdb2/jql.h>
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>

#define EJDB_DATABASE_META "EJDB*"
#define EJDB_QUERY_META "JQL*"

#ifdef LUA_VERSION_NUM == 501
#define lua_isinteger(L, idx) false
#endif

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
    lua_rawgeti(L, index, 1);
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

void jql_set_str2_stdfree(void* str, void* op) {
  free(str);
}

/// Open a ejdb database
// @function open
// @tparam string file The database file to open
// @tparam string mode The modestring. `w` stands for truncate, `r` stands for read only. Empty string is normal open
// mode
// @treturn[1] db The opened database
// @return[2] nil
// @treturn[2] string Error Message
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
      return luaL_error(L, "invalid character in open mode: %c", mode_string[0]);
    mode_string++;
  }
  EJDB_OPTS opts = {.kv = {.path = file, .oflags = mode}};
  EJDB* db = lua_newuserdata(L, sizeof(EJDB));
  EJDB_LUA_ASSERT(ejdb_open(&opts, db));

  luaL_getmetatable(L, EJDB_DATABASE_META);
  lua_setmetatable(L, -2);
  return 1;
}

/// Create a new query
// @function query
// @tparam string collection To collection to operate on
// @tparam string query The query to prepare
// @treturn[1] jql The created query
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_query(lua_State* L) {
  lua_settop(L, 2);
  const char* collection = luaL_checkstring(L, 1);
  JQL* query = lua_newuserdata(L, sizeof(JQL));
  EJDB_LUA_ASSERT(ejdb_lua_jql_create(L, 2, collection, query));
  luaL_getmetatable(L, EJDB_QUERY_META);
  lua_setmetatable(L, -2);
  return 1;
}

/// Database handle
// @type db

/// Put a document into a collection
// @function put
// @tparam[opt] integer id The id of the inserted document
// @tparam string collection The collection to insert into
// @tparam string|table document The document to insert. Can be a json string or a table
// @treturn[1] integer ID of new document
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_ejdb_put(lua_State* L) {
  iwrc rc;
  JBL jbl;
  int64_t id;
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  int coll_idx = 2;
  bool use_index = false;
  if (lua_isnumber(L, 2)) {
    use_index = true;
    id = luaL_checkinteger(L, 2);
    coll_idx++;
  }
  const char* collection = luaL_checkstring(L, coll_idx);
  EJDB_LUA_ASSERT(ejdb_lua_jbl_create(L, coll_idx + 1, &jbl));
  if (use_index) {
    if ((rc = ejdb_put(*db, collection, jbl, id))) {
      jbl_destroy(&jbl);
      EJDB_LUA_ASSERT(rc);
    }
  } else {
    if ((rc = ejdb_put_new(*db, collection, jbl, &id))) {
      jbl_destroy(&jbl);
      EJDB_LUA_ASSERT(rc);
    }
  }
  jbl_destroy(&jbl);
  lua_pushinteger(L, id);
  return 1;
}

/// Execute a JQL query
// @function exec
// @tparam jql query The query to execute
// @tparam function(id,data) callback Callback to execute for each found document
// @treturn[1] integer Number of documents matched
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_ejdb_exec(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  JQL* q = luaL_checkudata(L, 2, EJDB_QUERY_META);
  luaL_checktype(L, 3, LUA_TFUNCTION);
  EJDB_EXEC ux = {.db = *db, .q = *q, .visitor = ejdb_lua_visitor, .opaque = L};
  EJDB_LUA_ASSERT(ejdb_exec(&ux));
  lua_pushinteger(L, ux.cnt);
  return 1;
}

/// Close a database handle
// @function close
int ejdb_lua_ejdb_close(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  EJDB_LUA_ERROR(ejdb_close(db));
  return 0;
}

/// Retrieve a document from the database
// @function get
// @tparam string collection The collection to work on
// @tparam integer id ID of the document to get
// @treturn[1] table document content
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_ejdb_get(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  const char* collection = luaL_checkstring(L, 2);
  int64_t index = luaL_checkinteger(L, 3);
  JBL out;
  EJDB_LUA_ASSERT(ejdb_get(*db, collection, index, &out));
  ejdb_lua_push_jbl(L, &out);
  jbl_destroy(&out);
  return 1;
}

/// Delete a document from the database
// @function get
// @tparam string collection The collection to work on
// @tparam integer id ID of the document to get
// @treturn[1] index of the deleted document
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_ejdb_del(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  const char* collection = luaL_checkstring(L, 2);
  int64_t index = luaL_checkinteger(L, 3);
  EJDB_LUA_ASSERT(ejdb_del(*db, collection, index));
  lua_pushinteger(L, index);
  return 1;
}

/// Delete a collection from the database
// @function remove_collection
// @tparam string collection The collection to work on
// @return[1] true
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_ejdb_remove_collection(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  const char* collection = luaL_checkstring(L, 2);
  EJDB_LUA_ASSERT(ejdb_remove_collection(*db, collection));
  lua_pushboolean(L, true);
  return 1;
}

/// Delete a collection from the database
// @function rename_collection
// @tparam string old_name The collection to rename
// @tparam string new_name Name for the collection to have afterwards
// @return[1] true
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_ejdb_rename_collection(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  const char* collection1 = luaL_checkstring(L, 2);
  const char* collection2 = luaL_checkstring(L, 3);
  EJDB_LUA_ASSERT(ejdb_rename_collection(*db, collection1, collection2));
  lua_pushboolean(L, true);
  return 1;
}

/// Ensure a collection on the database
// @function ensure_collection
// @tparam string collection The collection to work on
// @return[1] true
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_ejdb_ensure_collection(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  const char* collection = luaL_checkstring(L, 2);
  EJDB_LUA_ASSERT(ejdb_ensure_collection(*db, collection));
  lua_pushboolean(L, true);
  return 1;
}

/// Ensure a index in a collection
// @function ensure_index
// @tparam string collection The collection to work on
// @tparam string field JSON Pointer to the field
// @tparam string mode List of mode chars: `i`=i64, `s`=str, `f`=double, `u`=unique
// @return[1] true
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_ejdb_ensure_index(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  const char* collection = luaL_checkstring(L, 2);
  const char* path = luaL_checkstring(L, 3);
  const char* mode = luaL_checkstring(L, 4);
  ejdb_idx_mode_t m = 0;
  while (mode[0]) {
    if (mode[0] == 'u') {
      m |= EJDB_IDX_UNIQUE;
    } else if (mode[0] == 's') {
      m |= EJDB_IDX_STR;
    } else if (mode[0] == 'i') {
      m |= EJDB_IDX_I64;
    } else if (mode[0] == 'f') {
      m |= EJDB_IDX_F64;
    } else {
      return luaL_error(L, "Invalid modechar %d", mode[0]);
    }
    mode++;
  }
  EJDB_LUA_ASSERT(ejdb_ensure_index(*db, collection, path, m));
  lua_pushboolean(L, true);
  return 1;
}

/// Remove a index from a collection
// @function remove_index
// @tparam string collection The collection to work on
// @tparam string field JSON Pointer to the field
// @tparam string mode List of mode chars: `i`=i64, `s`=str, `f`=double, `u`=unique
// @return[1] true
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_ejdb_remove_index(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  const char* collection = luaL_checkstring(L, 2);
  const char* path = luaL_checkstring(L, 3);
  const char* mode = luaL_checkstring(L, 3);
  ejdb_idx_mode_t m = 0;
  while (mode[0]) {
    if (mode[0] == 'u') {
      m |= EJDB_IDX_UNIQUE;
    } else if (mode[0] == 's') {
      m |= EJDB_IDX_STR;
    } else if (mode[0] == 'i') {
      m |= EJDB_IDX_I64;
    } else if (mode[0] == 'f') {
      m |= EJDB_IDX_F64;
    } else {
      return luaL_error("Invalid modechar %d", mode[0]);
    }
    mode++;
  }
  EJDB_LUA_ASSERT(ejdb_remove_index(*db, collection, path, m));
  lua_pushboolean(L, true);
  return 1;
}

/// Retrieve database structure
// @function get_meta
// @treturn[1] table Structure of the database
// @return[2] nil
// @treturn[2] string Error Message
int ejdb_lua_ejdb_get_meta(lua_State* L) {
  EJDB* db = luaL_checkudata(L, 1, EJDB_DATABASE_META);
  JBL jbl;
  EJDB_LUA_ASSERT(ejdb_get_meta(*db, &jbl));
  ejdb_lua_push_jbl(L, &jbl);
  jbl_destroy(&jbl);
  return 1;
}

/// JQL Query
// @type jql

/// Set a value in the query
// @function set
// @tparam string placeholder The placeholder to set
// @tparam integer index Index of something (?)
// @param value The value to set this placeholder to
int ejdb_lua_query_set(lua_State* L) {
  lua_settop(L, 4);
  JQL* q = luaL_checkudata(L, 1, EJDB_QUERY_META);
  const char* placeholder = luaL_checkstring(L, 2);
  int index = luaL_checkinteger(L, 3);
  if (lua_isinteger(L, 4)) {
    int64_t value = luaL_checkinteger(L, 4);
    EJDB_LUA_ERROR(jql_set_i64(*q, placeholder, index, value));
    return 0;
  } else if (lua_isnumber(L, 4)) {
    double value = luaL_checknumber(L, 4);
    EJDB_LUA_ERROR(jql_set_f64(*q, placeholder, index, value));
    return 0;
  } else if (lua_isboolean(L, 4)) {
    bool value = lua_toboolean(L, 4);
    EJDB_LUA_ERROR(jql_set_bool(*q, placeholder, index, value));
    return 0;
  } else if (lua_isstring(L, 4)) {
    const char* value = luaL_checkstring(L, 4);
    const char* data = malloc(strlen(value) + 1);
    memcpy(data, value, strlen(value) + 1);
    EJDB_LUA_ERROR(jql_set_str2(*q, placeholder, index, data, jql_set_str2_stdfree, NULL));
    return 0;
  } else if (lua_istable(L, 4)) {
    JBL j;
    ejdb_lua_jbl_from_lua(L, 4, &j);
    jql_set_json_jbl(*q, placeholder, index, j);
    jbl_destroy(&j);
  } else {
    return luaL_error(L, "invalid type for query:set: %s", lua_typename(L, lua_type(L, 4)));
  }
  return 0;
}

/// Set a regex value in the query
// @function set_regex
// @tparam string placeholder The placeholder to set
// @tparam integer index Index of something (?)
// @tparam string value The regex to search for
int ejdb_lua_query_set_regex(lua_State* L) {
  lua_settop(L, 4);
  JQL* q = luaL_checkudata(L, 1, EJDB_QUERY_META);
  const char* placeholder = luaL_checkstring(L, 2);
  int index = luaL_checkinteger(L, 3);
  const char* value = luaL_checkstring(L, 4);
  const char* data = malloc(strlen(value) + 1);
  memcpy(data, value, strlen(value) + 1);
  EJDB_LUA_ERROR(jql_set_regexp2(*q, placeholder, index, data, jql_set_str2_stdfree, NULL));
  return 0;
}

int luaopen_ejdb(lua_State* L) {
  luaL_newmetatable(L, EJDB_DATABASE_META);
  lua_pushcfunction(L, ejdb_lua_ejdb_gc);
  lua_setfield(L, -2, "__gc");
  lua_pushcfunction(L, ejdb_lua_ejdb_put);
  lua_setfield(L, -2, "put");
  lua_pushcfunction(L, ejdb_lua_ejdb_exec);
  lua_setfield(L, -2, "exec");
  lua_pushcfunction(L, ejdb_lua_ejdb_close);
  lua_setfield(L, -2, "close");
  lua_pushcfunction(L, ejdb_lua_ejdb_get);
  lua_setfield(L, -2, "get");
  lua_pushcfunction(L, ejdb_lua_ejdb_del);
  lua_setfield(L, -2, "del");
  lua_pushcfunction(L, ejdb_lua_ejdb_remove_collection);
  lua_setfield(L, -2, "remove_collection");
  lua_pushcfunction(L, ejdb_lua_ejdb_rename_collection);
  lua_setfield(L, -2, "rename_collection");
  lua_pushcfunction(L, ejdb_lua_ejdb_ensure_collection);
  lua_setfield(L, -2, "ensure_collection");
  lua_pushcfunction(L, ejdb_lua_ejdb_ensure_index);
  lua_setfield(L, -2, "ensure_index");
  lua_pushcfunction(L, ejdb_lua_ejdb_remove_index);
  lua_setfield(L, -2, "remove_index");
  lua_pushcfunction(L, ejdb_lua_ejdb_get_meta);
  lua_setfield(L, -2, "get_meta");
  lua_setfield(L, -1, "__index");

  luaL_newmetatable(L, EJDB_QUERY_META);
  lua_pushcfunction(L, ejdb_lua_query_gc);
  lua_setfield(L, -2, "__gc");
  lua_pushcfunction(L, ejdb_lua_query_set);
  lua_setfield(L, -2, "set");
  lua_pushcfunction(L, ejdb_lua_query_set_regex);
  lua_setfield(L, -2, "set_regex");
  lua_setfield(L, -1, "__index");

  EJDB_LUA_ERROR(ejdb_init());
  lua_newtable(L);
  lua_pushcfunction(L, ejdb_lua_open);
  lua_setfield(L, -2, "open");
  lua_pushcfunction(L, ejdb_lua_query);
  lua_setfield(L, -2, "query");
  return 1;
}
