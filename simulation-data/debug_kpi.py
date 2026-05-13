import grpc
import json
import os
from google.protobuf.json_format import MessageToDict
import sys
sys.path.append(os.path.abspath('src/metri/grpc/python'))
import metri_pb2
import metri_pb2_grpc

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = metri_pb2_grpc.MetriServiceStub(channel)
        
        cmp_query = metri_pb2.AnalyticsRequest()
        cmp_query.entity = "meter_reading"
        cmp_query.output_cast = metri_pb2.KPI
        cmp_query.viz = "indicator"
        m4 = cmp_query.metrics.add()
        m4.attribute = "reading_value"
        m4.aggregation = metri_pb2.AVG
        cmp1 = cmp_query.comparisons.add()
        cmp1.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
        cmp1.shortcut = metri_pb2.AnalyticalComparison.PREVIOUS_PERIOD
        cmp_query.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
        cmp_query.time_frame.n_value = 30
        
        req = metri_pb2.QueryRequest(
            tenant_id="tenant-123",
            explain_plan=True,
            queries={"q_kpi_cmp_tf": cmp_query}
        )
        
        res = stub.Query(req)
        for r in res:
            d = MessageToDict(r, preserving_proto_field_name=True)
            print(json.dumps(d, indent=2))

if __name__ == "__main__":
    run()
