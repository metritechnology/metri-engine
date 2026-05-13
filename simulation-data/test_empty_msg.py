import metri_pb2

b = metri_pb2.QueryResponse()
b.data.CopyFrom(metri_pb2.RowSet())
serialized = b.SerializeToString()

print("Serialized empty data:", serialized.hex())

b2 = metri_pb2.QueryResponse()
b2.ParseFromString(serialized)
print("HAS DATA?", b2.HasField("data"))
