import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
import metri_pb2

print(metri_pb2.KPI)
print(metri_pb2.TimeFrameContext.MONTH)
