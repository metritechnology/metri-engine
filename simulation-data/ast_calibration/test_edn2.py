import edn_format
s = "[:between :foo/bar [10.0 20.0]]"
d = edn_format.loads(s)
print(d)
print(type(d[2]))
print(str(d[2]) == "[10.0 20.0]")
print(list(d[2]) == [10.0, 20.0])
