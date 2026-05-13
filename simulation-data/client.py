import json
import logging
import struct
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from urllib import request, error
import metri_pb2

# ============================================================================
# METRI ENGINE - PYTHON CLIENT SDK (FASE 2 / 3)
# Simula un cliente que respeta el patrón Zero-Trust (AWS_IAM) y 
# envía un payload crudo usando el protocolo oficial gRPC-Web
# ============================================================================

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Configuraciones base
REGION = "us-east-1"
PROFILE = "metri-dev"
FUNCTION_NAME = "metri-engine-MetriEngineFunction-Iph5UpmngRoy"
# NOTA: Volvemos a la URL directa de AWS para saltar el bug de CloudFront OAC
FUNCTION_URL = "https://engine.metri.one/"

def get_boto_session():
    try:
        return boto3.Session(profile_name=PROFILE, region_name=REGION)
    except Exception as e:
        logging.warning(f"No se encontró el perfil '{PROFILE}', usando credenciales por defecto.")
        return boto3.Session(region_name=REGION)

def make_grpc_equivalent_payload() -> metri_pb2.TransactionRequest:
    """
    Construye el TransactionRequest en código nativo Protobuf.
    """
    req = metri_pb2.TransactionRequest()
    req.tenant_id = "test-tenant-1"
    req.entity_type = "tenant"
    req.action = metri_pb2.CREATE
    # Simulando un payload 100% compliant con tenant.json
    req.payload.update({
        "name": "Acme Corp",
        "status": "ACTIVE",
        "tier": "ENTERPRISE",
        "storage_region": "us-east-1",
        "billing_admin_email": "admin@acme.com"
    })
    return req

def print_client_status(client_name: str, response: metri_pb2.TransactionResponse):
    """
    Estrategia para imprimir el estado de cada uno de los clientes.
    """
    logging.info(f"=== ESTATUS DEL CLIENTE: {client_name} ===")
    if response.status.success:
        logging.info("✅ ESTADO: FUNCIONANDO OK")
        logging.info(f"Respuesta Entity ID: {response.entity_id}")
    else:
        logging.error("❌ ESTADO: FALLO EN PLATAFORMA (Railway Error)")
        logging.error(f"Código de Error: {response.status.error_code}")
        logging.error(f"Mensaje de Error: {response.status.error_message}")
    logging.info("=========================================\n")

def invoke_via_function_url(req: metri_pb2.TransactionRequest):
    """
    METODO 2: Petición HTTP firmada con SigV4 a la Function URL.
    Enviamos bytes de Protobuf usando el protocolo de bloques gRPC-Web.
    """
    logging.info("--- Petición HTTP SigV4 Directa a Function URL (gRPC-Web) ---")
    import requests
    from requests_auth_aws_sigv4 import AWSSigV4
    
    session = get_boto_session()
    credentials = session.get_credentials()
    frozen_creds = credentials.get_frozen_credentials()

    # 1. Empaquetar en gRPC-Web Frame [0x00][Length 4-bytes][Datos]
    proto_bytes = req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    
    import requests
    from requests_auth_aws_sigv4 import AWSSigV4

    # 1. Empaquetar en gRPC-Web Frame [0x00][Length 4-bytes][Datos]
    proto_bytes = req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    
    import requests
    import hashlib

    # Calcular SHA256 del payload (Requisito de CloudFront OAC para POST en Lambda Function URLs)
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    
    try:
        resp = requests.post(
            f"{FUNCTION_URL}metri.MetriService/Transact", 
            data=framed_data, 
            headers={
                'Content-Type': 'application/grpc-web+proto',
                'x-amz-content-sha256': payload_hash
            }
        )
        
        if resp.status_code != 200:
            logging.error(f"❌ FAULT (Railway / Security): Status {resp.status_code}")
            logging.error(resp.text)
            return

        # Leemos la respuesta como bytes crudos (AWS ya decodificó el Base64)
        response_bytes = resp.content
        
        # 2. Desempaquetar gRPC-Web (Leemos el primer chunk de datos)
        if len(response_bytes) < 5:
            logging.error("Respuesta demasiado corta para ser gRPC-Web")
            return
            
        flag, length = struct.unpack('!BI', response_bytes[:5])
        if flag == 0x00:
            proto_response_bytes = response_bytes[5:5+length]
            
            # Deserializamos la respuesta de Protobuf
            proto_resp = metri_pb2.TransactionResponse()
            proto_resp.ParseFromString(proto_response_bytes)
            
            print_client_status("External Client HTTP SigV4 (gRPC-Web)", proto_resp)
            
            # Opcional: Leer Trailing Headers en el siguiente chunk
            if len(response_bytes) > 5 + length:
                trailer_start = 5 + length
                t_flag, t_length = struct.unpack('!BI', response_bytes[trailer_start:trailer_start+5])
                if t_flag == 0x80:
                    trailers = response_bytes[trailer_start+5:trailer_start+5+t_length]
                    logging.info(f"Trailing Headers recibidos: {trailers.decode().strip()}")
        else:
            logging.error(f"Flag gRPC-Web desconocido: {flag}")
            logging.error(f"Cuerpo de la respuesta: {response_bytes.decode('utf-8', errors='ignore')}")
            
    except Exception as e:
        logging.error(f"Error de red: {e}")

if __name__ == "__main__":
    payload = make_grpc_equivalent_payload()
    invoke_via_function_url(payload)
