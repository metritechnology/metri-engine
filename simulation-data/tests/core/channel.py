import grpc
from tests.core.grpc_client import LocalGrpcClient
from tests.core.grpc_web_client import GrpcWebClient

def get_client(env: str):
    if env == "local":
        channel = grpc.insecure_channel('localhost:9090')
        return LocalGrpcClient(channel)
    elif env == "production":
        return GrpcWebClient("https://engine.metri.one")
    else:
        raise ValueError(f"Unknown env: {env}")
