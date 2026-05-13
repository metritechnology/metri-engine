import struct
import requests
import hashlib
import json
import metri_pb2

class GrpcWebClient:
    def __init__(self, endpoint_url: str):
        self.endpoint_url = endpoint_url
        if not self.endpoint_url.endswith("/"):
            self.endpoint_url += "/"
            
    def _invoke(self, rpc_method: str, req_message, resp_class):
        proto_bytes = req_message.SerializeToString()
        framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
        payload_hash = hashlib.sha256(framed_data).hexdigest()
        
        resp = requests.post(
            f"{self.endpoint_url}metri.MetriService/{rpc_method}", 
            data=framed_data, 
            headers={
                'Content-Type': 'application/grpc-web+proto',
                'x-amz-content-sha256': payload_hash,
                'x-metri-origin-token': 'simulate-zero-trust'
            },
            stream=True
        )
        
        if resp.status_code != 200:
            # For testing, we might want to throw an exception that can be caught
            raise Exception(f"HTTP Status {resp.status_code}: {resp.text}")

        response_bytes = resp.content
        if len(response_bytes) < 5:
            raise Exception("Response too short")
            
        offset = 0
        while offset < len(response_bytes):
            flag, length = struct.unpack('!BI', response_bytes[offset:offset+5])
            offset += 5
            
            if flag == 0x00: # Data frame
                chunk_bytes = response_bytes[offset:offset+length]
                proto_resp = resp_class()
                proto_resp.ParseFromString(chunk_bytes)
                return proto_resp
            
            offset += length
            
        raise Exception("No data frame found")

    def query(self, request: metri_pb2.QueryRequest) -> metri_pb2.QueryResponse:
        return self._invoke("Query", request, metri_pb2.QueryResponse)
        
    def transact(self, request: metri_pb2.TransactionRequest) -> metri_pb2.TransactionResponse:
        return self._invoke("Transact", request, metri_pb2.TransactionResponse)
        
    def bulk_ingest(self, request: metri_pb2.BulkRequest) -> metri_pb2.BulkResponse:
        return self._invoke("BulkIngest", request, metri_pb2.BulkResponse)
        
    def match_routing_rules_batch(self, request: metri_pb2.MatchRoutingRulesBatchRequest) -> metri_pb2.MatchRoutingRulesBatchResponse:
        return self._invoke("MatchRoutingRulesBatch", request, metri_pb2.MatchRoutingRulesBatchResponse)
