import json
import logging
import metri_pb2
from viz_all_strategies import invoke_grpc_web

def generate_report():
    req = metri_pb2.QueryRequest()
    req.tenant_id = "golden-tenant"
    
    # 1. OLTP Table
    q_oltp_table = metri_pb2.AnalyticsRequest()
    q_oltp_table.tenant_id = "golden-tenant"
    q_oltp_table.entity = "inventory_movement"
    q_oltp_table.output_cast = metri_pb2.TABLE
    q_oltp_table.viz = "table"
    q_oltp_table.limit = 5
    req.queries["oltp_table"].CopyFrom(q_oltp_table)
    
    # 2. OLTP Line
    q_oltp_line = metri_pb2.AnalyticsRequest()
    q_oltp_line.tenant_id = "golden-tenant"
    q_oltp_line.entity = "inventory_movement"
    q_oltp_line.output_cast = metri_pb2.TIMESERIES
    q_oltp_line.viz = "line"
    m_oltp_line = q_oltp_line.metrics.add()
    m_oltp_line.aggregation = metri_pb2.SUM
    m_oltp_line.attribute = "quantity"
    d_oltp_line = q_oltp_line.dimensions.add()
    d_oltp_line.attribute = "timestamp"
    d_oltp_line.interval = "day"
    req.queries["oltp_line"].CopyFrom(q_oltp_line)

    # 3. OLTP Scatter
    q_oltp_scatter = metri_pb2.AnalyticsRequest()
    q_oltp_scatter.tenant_id = "golden-tenant"
    q_oltp_scatter.entity = "inventory_movement"
    q_oltp_scatter.output_cast = metri_pb2.BUBBLE
    q_oltp_scatter.viz = "scatter"
    m_oltp_scatter = q_oltp_scatter.metrics.add()
    m_oltp_scatter.aggregation = metri_pb2.AVG
    m_oltp_scatter.attribute = "quantity"
    d_oltp_scatter = q_oltp_scatter.dimensions.add()
    d_oltp_scatter.attribute = "type"
    req.queries["oltp_scatter"].CopyFrom(q_oltp_scatter)

    # 4. OLTP Pie
    q_oltp_pie = metri_pb2.AnalyticsRequest()
    q_oltp_pie.tenant_id = "golden-tenant"
    q_oltp_pie.entity = "inventory_movement"
    q_oltp_pie.output_cast = metri_pb2.PIE
    q_oltp_pie.viz = "pie"
    m_oltp_pie = q_oltp_pie.metrics.add()
    m_oltp_pie.aggregation = metri_pb2.COUNT
    m_oltp_pie.attribute = "quantity"
    d_oltp_pie = q_oltp_pie.dimensions.add()
    d_oltp_pie.attribute = "type"
    req.queries["oltp_pie"].CopyFrom(q_oltp_pie)

    # 5. OLTP Tree
    q_oltp_tree = metri_pb2.AnalyticsRequest()
    q_oltp_tree.tenant_id = "golden-tenant"
    q_oltp_tree.entity = "location"
    q_oltp_tree.output_cast = metri_pb2.TABLE
    q_oltp_tree.viz = "tree"
    q_oltp_tree.hierarchy.inject_has_children = True
    q_oltp_tree.hierarchy.parent_field = "parent_location_id"
    req.queries["oltp_tree"].CopyFrom(q_oltp_tree)

    # 1. OLAP Table
    q_olap_table = metri_pb2.AnalyticsRequest()
    q_olap_table.tenant_id = "golden-tenant"
    q_olap_table.entity = "meter_reading"
    q_olap_table.output_cast = metri_pb2.TABLE
    q_olap_table.viz = "table"
    q_olap_table.limit = 5
    req.queries["olap_table"].CopyFrom(q_olap_table)
    
    # 2. OLAP Line
    q_olap_line = metri_pb2.AnalyticsRequest()
    q_olap_line.tenant_id = "golden-tenant"
    q_olap_line.entity = "meter_reading"
    q_olap_line.output_cast = metri_pb2.TIMESERIES
    q_olap_line.viz = "line"
    m_olap_line = q_olap_line.metrics.add()
    m_olap_line.aggregation = metri_pb2.AVG
    m_olap_line.attribute = "reading_value"
    d_olap_line = q_olap_line.dimensions.add()
    d_olap_line.attribute = "timestamp"
    d_olap_line.interval = "day"
    req.queries["olap_line"].CopyFrom(q_olap_line)

    # 3. OLAP Scatter
    q_olap_scatter = metri_pb2.AnalyticsRequest()
    q_olap_scatter.tenant_id = "golden-tenant"
    q_olap_scatter.entity = "meter_reading"
    q_olap_scatter.output_cast = metri_pb2.BUBBLE
    q_olap_scatter.viz = "scatter"
    m_olap_scatter = q_olap_scatter.metrics.add()
    m_olap_scatter.aggregation = metri_pb2.AVG
    m_olap_scatter.attribute = "reading_value"
    d_olap_scatter = q_olap_scatter.dimensions.add()
    d_olap_scatter.attribute = "unit_of_measure"
    req.queries["olap_scatter"].CopyFrom(q_olap_scatter)

    # 4. OLAP Pie
    q_olap_pie = metri_pb2.AnalyticsRequest()
    q_olap_pie.tenant_id = "golden-tenant"
    q_olap_pie.entity = "meter_reading"
    q_olap_pie.output_cast = metri_pb2.PIE
    q_olap_pie.viz = "pie"
    m_olap_pie = q_olap_pie.metrics.add()
    m_olap_pie.aggregation = metri_pb2.COUNT
    m_olap_pie.attribute = "reading_value"
    d_olap_pie = q_olap_pie.dimensions.add()
    d_olap_pie.attribute = "unit_of_measure"
    req.queries["olap_pie"].CopyFrom(q_olap_pie)
    
    # 5. OLAP Tree
    q_olap_tree = metri_pb2.AnalyticsRequest()
    q_olap_tree.tenant_id = "golden-tenant"
    q_olap_tree.entity = "meter_reading"
    q_olap_tree.output_cast = metri_pb2.TABLE
    q_olap_tree.viz = "tree"
    q_olap_tree.hierarchy.inject_has_children = True
    q_olap_tree.hierarchy.parent_field = "asset_id"
    req.queries["olap_tree"].CopyFrom(q_olap_tree)

    frames = invoke_grpc_web("metri.MetriService/Query", req, token="datalog-golden-tenant")
    
    if frames:
        from google.protobuf.json_format import MessageToDict
        combined_batch_results = {}
        for f in frames:
            resp = metri_pb2.QueryResponse()
            resp.ParseFromString(f)
            if resp.status.success:
                chunk_dict = MessageToDict(resp, preserving_proto_field_name=True)
                if "batch_results" in chunk_dict:
                    combined_batch_results.update(chunk_dict["batch_results"])
                    
        with open("/Users/macuser/.gemini/antigravity/brain/686d10dd-3bf6-4597-a21c-e1372c00ad58/artifacts/viz_all_strategies_manual_verification.json", "w") as out:
            json.dump(combined_batch_results, out, indent=2)
            
if __name__ == "__main__":
    generate_report()
