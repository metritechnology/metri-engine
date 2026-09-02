#!/usr/bin/env python3
import os
import sys
import time
import concurrent.futures

# Setup paths to import the local client
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

from grpc_web_client import GrpcWebStub

PROTO_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "src", "metri", "grpc", "python")
sys.path.insert(0, PROTO_DIR)
import metri_pb2 as pb

# Set HMAC secret for production
os.environ["HMAC_SECRET"] = "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs"
os.environ["ENGINE_HOST"] = os.environ.get("ENGINE_HOST", "engine.metri.one")

def send_invalidation_request(stub, index):
    dummy_id = f"01KS5DUMMYCACHEINVALIDATION{index:03d}"
    del_req = pb.TransactionRequest(
        tenant_id="golden-tenant-real",
        entity_type="location",
        entity_id=dummy_id,
        action=pb.OperationAction.DELETE,
        suppress_events=True
    )
    try:
        resp = stub.Transact(del_req)
        return resp.status.success, dummy_id
    except Exception as e:
        return False, str(e)

def main():
    print("=" * 70)
    print("       INVALIDATING ALL PRODUCTION LAMBDA INSTANCES CACHE      ")
    print("=" * 70)
    
    stub = GrpcWebStub(os.environ.get("ENGINE_HOST", "engine.metri.one"))
    
    # We will send 120 requests in parallel using ThreadPoolExecutor to hit all warm Lambda containers
    num_requests = 120
    print(f"-> Sending {num_requests} invalidation transact requests concurrently...")
    
    success_count = 0
    failure_count = 0
    
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(send_invalidation_request, stub, i) for i in range(num_requests)]
        for fut in concurrent.futures.as_completed(futures):
            success, msg = fut.result()
            if success:
                success_count += 1
            else:
                failure_count += 1
                
    duration = time.time() - start_time
    print(f"\n✓ Completed in {duration:.2f} seconds!")
    print(f"  - Total requests sent: {num_requests}")
    print(f"  - Successes: {success_count}")
    print(f"  - Failures: {failure_count}")
    print("\nCaching should be fully cleared across all warm AWS Lambda containers now.")

if __name__ == "__main__":
    main()
