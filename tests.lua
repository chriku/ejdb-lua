package.preload.ejdb = package.loadlib("build/linux/x86_64/release/libejdb.so","luaopen_ejdb")
local test = require 'u-test.u-test'
local ejdb = require"ejdb"
function test.open()
  local db = assert(ejdb.open("test.db", "w"))
  db:close()
end
function test.insert()
  local db = ejdb.open("test.db", "w")
  assert(db:put("docs", {name = "Doc 1", num = 1}))
  assert(db:put("docs", [[{"name" :"Doc 2", "num":2}]]))
  local seen = {}
  local q = assert(ejdb.query("docs","/*"))
  assert(db:exec(q, function(id, data) seen[data.num] = (seen[data.num] or 0) + 1 end))
  test.equal(seen[1], 1)
  test.equal(seen[2], 1)
  db:close()
end
function test.compare()
  local db = ejdb.open("test.db", "w")
  assert(db:put("docs", {name = "Doc 1", num = 1}))
  assert(db:put("docs", [[{"name" :"Doc 2", "num":2}]]))
  local seen = {}
  local q = assert(ejdb.query("docs","/[num = :num]"))
  q:set("num", 0, 2)
  assert(db:exec(q, function(id, data) seen[data.num] = (seen[data.num] or 0) + 1 end))
  test.equal(seen[1] or 0, 0)
  test.equal(seen[2] or 0, 1)
  db:close()
end
test.summary()
