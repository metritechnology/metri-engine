import sys, os, json
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPTS_DIR)

from grpc_web_client import GrpcWebStub
import metri_pb2 as pb

stub = GrpcWebStub("127.0.0.1:9090")

req = pb.DiscoveryRequest(tenant_id="golden-tenant-benchmark", type="asset", include_attributes=True)
res = stub.Discover(req)

for schema in res.schemas:
    print(f"Entity: {schema.entity}")
    print(f"FTS fields: {schema.fts_fields}")
    for attr in schema.attributes:
        if attr.fts:
            print(f"  - {attr.name} (fts=True)")
