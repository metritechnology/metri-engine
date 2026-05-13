import metri_pb2

b = metri_pb2.QueryResponse()
qr = metri_pb2.QueryResponse()
qr.status.success = True
b.batch_results["oltp_table"].CopyFrom(qr)
b.status.success = True
serialized = b.SerializeToString()

print("Serialized batch_results:", serialized.hex())

b2 = metri_pb2.QueryResponse()
b2.ParseFromString(serialized)
print("BATCH RESULTS KEYS:", list(b2.batch_results.keys()))
