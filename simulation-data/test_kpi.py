import grpc
import json
import logging
import google.protobuf.json_format as json_format
from metri_pb2_grpc import MetriServiceStub
import metri_pb2
import time

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def get_kpi():
    channel = grpc.insecure_channel('localhost:9090')
    stub = MetriServiceStub(channel)
    
    req = metri_pb2.QueryRequest()
    req.tenant_id = "golden-tenant"
    
    q_name = "kpi_test"
    q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.KPI))
    
    m = req.queries[q_name].metrics.add()
    m.attribute = "reading_value"
    m.aggregation = metri_pb2.SUM
    
    tf = req.queries[q_name].time_frame
    tf.type = metri_pb2.TimeFrameContext.ALL_TIME
    tf.timezone = "America/Bogota"
    
    responses = stub.Query(req)
    for resp in responses:
        with open("/Users/macuser/.gemini/antigravity/brain/686d10dd-3bf6-4597-a21c-e1372c00ad58/artifacts/kpi_response.txt", "w") as f:
            f.write(str(resp))
        print("Successfully saved kpi_response.txt")
        return

if __name__ == "__main__":
    get_kpi()
