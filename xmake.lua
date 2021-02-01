set_languages("cxx20")
set_policy("check.auto_ignore_flags", false)

target("ejdb")
  set_kind("shared")
  add_defines("_XOPEN_SOURCE=500","_GNU_SOURCE")

  add_files("src/*.c")

  --add_includedirs("/usr/local/include/luajit-2.1/")
  add_links("ejdb2-2","iowow-1","facilio-1","pthread","m","c")
