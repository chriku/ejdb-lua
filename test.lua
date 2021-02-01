local function decode(t,int)
  int=int or 0
  for k,v in pairs(t) do
    if type(v)=="table" then print(string.rep("\t",int),k) decode(v,int+1) else print(string.rep("\t",int),k,v) end
  end
end
package.cpath=package.cpath..";build/linux/x86_64/release/libejdb.so"
local ejdb=require"ejdb"
local db = assert(ejdb.open("test.db",""))
--print(db:put_new("parrots","{\"name\":\"Bianca\", \"age\":4,\"xyz\":[true,3,0.5]}"))
print(db:put_new("parrots",{name="Bianca",age=4,xyz={1,2.0,false}}))
local q=assert(ejdb.query("parrots","/[age > :age]"))
q:set("age",0,2)
print(db:exec(q,function(id,data)
print(id)
decode(data)
end))
