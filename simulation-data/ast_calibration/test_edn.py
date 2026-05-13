import edn_format
s = "{:limit 100, :time-frame nil}"
d = edn_format.loads(s)
print(d)
print(d.keys())
print(d.get(edn_format.Keyword("limit")))
print(d.get(edn_format.Keyword("limit")) == 100)
