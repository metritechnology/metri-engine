import grpc
import sys
import metri_pb2
import metri_pb2_grpc

def run():
    channel = grpc.insecure_channel('localhost:9090')
    stub = metri_pb2_grpc.MetriServiceStub(channel)
    req = metri_pb2.TransactionRequest()
    req.tenant_id = "test-tenant-1"
    req.entity_type = "tenant"
    req.action = metri_pb2.OperationAction.CREATE
    req.payload.update({
        "name": "Acme Corp",
        "status": "ACTIVE",
        "tier": "ENTERPRISE",
        "storage_region": "us-east-1",
        "billing_admin_email": "admin@acme.com"
    })
    
    try:
        resp = stub.Transact(req)
        print("Success:", resp)
    except grpc.RpcError as e:
        print("Error:", e.code(), e.details())

if __name__ == '__main__':
    run()
