import grpc
import sys
import os

# Agrega la ruta de modulos protobuf generados
sys.path.append(os.path.join(os.path.dirname(__file__), "tests"))

import metri_pb2
import metri_pb2_grpc

from tests.core.channel import get_client

def run():
    client = get_client("local")
    
    req = metri_pb2.QueryRequest(tenant_id="golden-tenant")
    q = req.queries["q1"]
    q.tenant_id = "golden-tenant"
    q.entity = "meter_reading"
    m = q.metrics.add()
    m.attribute = "reading_value"
    m.aggregation = metri_pb2.AggregationFunction.SUM
    q.time_frame.type = metri_pb2.TimeFrameContext.ALL_TIME
    q.time_frame.timezone = "UTC"
    
    print("Req:")
    print(req)
    print("Enviando request con ALL_TIME...")
    try:
        resp = client.query(req)
        print("Raw Response:", resp)
        if "q1" in resp.batch_results:
            b_status = resp.batch_results["q1"].status
            print("Q1 ErrorCode:", b_status.error_code)
            print("Q1 ErrorMessage:", b_status.error_message)
    except Exception as e:
        print("Exception:", e)

if __name__ == "__main__":
    run()
