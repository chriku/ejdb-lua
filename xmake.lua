set_languages("cxx20")
set_policy("check.auto_ignore_flags", false)

target("ejdb")
  set_kind("shared")
  add_defines("_XOPEN_SOURCE=500","_GNU_SOURCE")
  add_links("pthread","m","c")

  add_files("src/*.c")

  add_includedirs("/usr/local/include/luajit-2.1/")
