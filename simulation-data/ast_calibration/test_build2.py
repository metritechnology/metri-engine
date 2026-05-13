import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
import metri_pb2

analytics_req = metri_pb2.AnalyticsRequest()
analytics_req.viz = metri_pb2.KPI
print(analytics_req)
