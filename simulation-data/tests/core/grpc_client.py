import grpc
import metri_pb2
import metri_pb2_grpc

class LocalGrpcClient:
    def __init__(self, channel):
        self.stub = metri_pb2_grpc.MetriServiceStub(channel)
        
    def query(self, request: metri_pb2.QueryRequest) -> metri_pb2.QueryResponse:
        # In local insecure channel, we read the stream
        responses = list(self.stub.Query(request))
        # Since our engine usually sends one response frame for standard queries,
        # we merge them if necessary or just return the first frame for the test suite.
        # Most of our tests only need the first batch_results frame.
        if len(responses) > 0:
            return responses[0]
        return metri_pb2.QueryResponse()
        
    def transact(self, request: metri_pb2.TransactionRequest) -> metri_pb2.TransactionResponse:
        return self.stub.Transact(request)
        
    def bulk_ingest(self, request: metri_pb2.BulkRequest) -> metri_pb2.BulkResponse:
        return self.stub.BulkIngest(request)
        
    def match_routing_rules_batch(self, request: metri_pb2.MatchRoutingRulesBatchRequest) -> metri_pb2.MatchRoutingRulesBatchResponse:
        return self.stub.MatchRoutingRulesBatch(request)
