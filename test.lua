local function decode(t,int)
  int=int or 0
  for k,v in pairs(t) do
    if type(v)=="table" then print(string.rep("\t",int),k) decode(v,int+1) else print(string.rep("\t",int),k,v) end
  end
end
package.cpath=package.cpath..";build/linux/x86_64/release/libejdb.so"
local ejdb=require"ejdb"
local db = assert(ejdb.open("test.db","w"))
assert(db:put("parrots",{name="Bianca",age=4}))
assert(db:put(8,"parrots","{\"name\":\"Darko\", \"age\":8}"))
do
  print("Querying parrots")
  local q=assert(ejdb.query("parrots","/*"))
  assert(db:exec(q,function(id,data) print(id) decode(data) end))
  print("Queryed parrots")
end
do
  print("Ageing up Bianca")
  assert(db:exec(ejdb.query("parrots",'/[name = Bianca] | apply {"age": 5}'),function()end))
end
do
  print("Querying parrots")
  local q=assert(ejdb.query("parrots","/*"))
  assert(db:exec(q,function(id,data) print(id) decode(data) end))
  print("Queryed parrots")
end
do
  print("Deleting Darko")
  assert(db:exec(ejdb.query("parrots",'/[name = Darko] | del'),function()end))
end
do
  print("Querying parrots")
  local q=assert(ejdb.query("parrots","/*"))
  assert(db:exec(q,function(id,data) print(id) decode(data) end))
  print("Queryed parrots")
end
