import sys
import os
import json
from google.protobuf.json_format import MessageToJson

sys.path.append(os.path.join(os.path.dirname(__file__), '../../src/metri/grpc/python'))
import grpc
import metri_pb2 as pb
import metri_pb2_grpc as pb_grpc

def test_vizmeta_kpi():
    channel = grpc.insecure_channel('127.0.0.1:9090')
    stub = pb_grpc.MetriServiceStub(channel)

    tenant_id = "golden-tenant-benchmark"

    req = pb.QueryRequest(
        tenant_id=tenant_id,
        queries={
            "kpi_test": pb.AnalyticsRequest(
                tenant_id=tenant_id,
                entity="asset",
                viz="kpi",
                metrics=[
                    pb.MetricDefinition(
                        entity="asset",
                        attribute="id",
                        aggregation=pb.COUNT,
                        name="total_assets"
                    )
                ],
                time_frame=pb.TimeFrameContext(
                    type=pb.TimeFrameContext.THIS_MONTH
                ),
                comparisons=[
                    pb.AnalyticalComparison(
                        type=pb.AnalyticalComparison.TIME_SHIFT_SHORTCUT,
                        shortcut=pb.AnalyticalComparison.PREVIOUS_PERIOD
                    )
                ]
            )
        }
    )

    print(f"=== Requesting KPI VizMeta ===")
    response_stream = stub.Query(req, timeout=60)
    
    for response in response_stream:
        print(MessageToJson(response))
        for k, v in response.batch_results.items():
            if not v.status.success:
                print(f"Batch Error for {k}: {v.status.error_code} - {v.status.error_message}")
        if not response.status.success:
            print(MessageToJson(response))
        else:
            print(f"Error: {response.status.error_code} - {response.status.error_message}")

if __name__ == "__main__":
    test_vizmeta_kpi()
