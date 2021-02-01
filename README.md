# Basic ejdb lua binding

For documentation see https://chriku.github.io/ejdb-lua/index.html

Simple, not yet complete binding of ejdb2 for lua.

Compiles against lua5.4 and luajit

## Example
```lua
local ejdb = require"ejdb"
local db = assert(ejdb.open("test.db", ""))
assert(db:put("parrots", {name = "Bianca", age = 4}))
assert(db:put(8, "parrots", "{\"name\":\"Darko\", \"age\":8}"))
assert(db:exec(ejdb.query("parrots", '/[name = Bianca] | apply {"age": 5}'), function()end))
local q=assert(ejdb.query("parrots", "/[age > :age]"))
q:set("age", 0, 6)
assert(db:exec(q, function(id, data)
  print("id: "..id)
  for k, v in pairs(data) do
    print(k, v)
  end
end))
```
results in
```
id: 8
name	Darko
age	8
```
