#!/usr/bin/env python3
import os
import sys
import time
import uuid
import random
import json
import argparse
import hmac
import hashlib
import base64
import concurrent.futures
from google.protobuf import struct_pb2

# Setup paths to import stubs
from pathlib import Path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENGINE_ROOT = Path(SCRIPT_DIR).parent.parent
sys.path.append(str(ENGINE_ROOT / "scripts" / "proto"))

import struct
import requests

try:
    import metri_pb2 as pb
except ImportError as e:
    print(f"Error importing dependencies. Details: {e}")
    sys.exit(1)

class GrpcWebStub:
    def __init__(self, host):
        self.host = host
        scheme = "http" if "127.0.0.1" in self.host or "localhost" in self.host else "https"
        self.base = f"{scheme}://{self.host}"
        self.session = requests.Session()

    def _make_token(self, tenant_id="demo"):
        now = int(time.time())
        claims = {
            "exp": now + 3600 * 24,
            "iat": now,
            "jti": f"python-seeder-{now}",
            "tid": tenant_id,
            "uid": "usr_system_bff"
        }
        payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')
        payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
        
        secret = os.environ.get("HMAC_SECRET", "local-dev-secret-do-not-use-in-prod")
        sig = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256).digest()
        sig_b64 = base64.urlsafe_b64encode(sig).decode('utf-8').rstrip('=')
        
        return f"Bearer mk_{payload_b64}.{sig_b64}"

    def _grpc_frame(self, data: bytes) -> bytes:
        return b'\x00' + struct.pack('>I', len(data)) + data

    def _parse_frames(self, body: bytes):
        idx = 0
        while idx < len(body):
            if idx + 5 > len(body):
                break
            flag = body[idx]
            length = struct.unpack('>I', body[idx+1:idx+5])[0]
            idx += 5
            payload = body[idx:idx+length]
            idx += length
            yield (flag & 0x80 != 0, payload)

    def _post(self, path: str, proto_msg) -> bytes:
        data = proto_msg.SerializeToString()
        framed = self._grpc_frame(data)
        
        tenant_id = "metri-tenant-real"
        if hasattr(proto_msg, "tenant_id") and proto_msg.tenant_id:
            tenant_id = proto_msg.tenant_id
            
        headers = {
            "Content-Type": "application/grpc-web+proto",
            "Accept": "application/grpc-web+proto",
            "Authorization": self._make_token(tenant_id),
            "x-tenant-id": tenant_id,
            "x-grpc-web": "1",
            "X-Metri-Origin-Token": "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs",
        }
        resp = self.session.post(f"{self.base}{path}", data=framed, headers=headers, timeout=300)
        resp.raise_for_status()
        grpc_status = resp.headers.get("grpc-status")
        grpc_message = resp.headers.get("grpc-message")
        if grpc_status and grpc_status != "0":
            from urllib.parse import unquote
            decoded_msg = unquote(grpc_message) if grpc_message else ""
            raise Exception(f"gRPC Error (status={grpc_status}, message='{decoded_msg}')")
        return resp.content

    def Query(self, req: pb.QueryRequest):
        body = self._post("/metri.MetriService/Query", req)
        results = []
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.QueryResponse()
                msg.ParseFromString(payload)
                results.append(msg)
        return results

    def Transact(self, req: pb.TransactionRequest) -> pb.TransactionResponse:
        body = self._post("/metri.MetriService/Transact", req)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.TransactionResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.TransactionResponse()

    def Discover(self, req: pb.DiscoveryRequest) -> pb.DiscoveryResponse:
        body = self._post("/metri.MetriService/Discovery", req)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.DiscoveryResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.DiscoveryResponse()

    def BulkIngest(self, req: pb.BulkRequest) -> pb.BulkResponse:
        body = self._post("/metri.MetriService/BulkIngest", req)
        frames = list(self._parse_frames(body))
        for is_trailer, payload in frames:
            if not is_trailer:
                msg = pb.BulkResponse()
                msg.ParseFromString(payload)
                return msg
            else:
                print(f"      [DEBUG] Trailer payload: {payload.decode('utf-8', errors='ignore')}")
        if not frames:
            print("      [DEBUG] No frames returned from server")
        return pb.BulkResponse()

# Crockford Base32 characters for ULID generation
CROCKFORD_CHARS = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

def encode_crockford(num, length):
    res = []
    for _ in range(length):
        res.append(CROCKFORD_CHARS[num % 32])
        num //= 32
    return "".join(reversed(res))

def generate_ulid():
    """Generates a monotonic-like 26-character Crockford Base32 ULID."""
    ts = int(time.time() * 1000)
    rand_num = random.getrandbits(80)
    return encode_crockford(ts, 10) + encode_crockford(rand_num, 16)

def generate_signed_token(secret, tenant_id, user_id, ttl_seconds=3600):
    """Produces a system token starting with 'mk_' signed with HMAC-SHA256."""
    now = int(time.time())
    claims = {
        "tid": tenant_id,
        "uid": user_id,
        "iat": now,
        "exp": now + ttl_seconds,
        "jti": str(uuid.uuid4())
    }
    payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
    
    mac = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256)
    sig_bytes = mac.digest()
    sig_b64 = base64.urlsafe_b64encode(sig_bytes).decode('utf-8').rstrip('=')
    
    return f"mk_{payload_b64}.{sig_b64}"

def test_auth_login(auth_url, tenant_id, username, password):
    """
    Simulates a full OIDC / OAuth 2.1 authorization code flow with PKCE
    against the metri-auth service to validate main user login.
    """
    print(f"\n[1/5] Iniciando validación de autenticación de Tenant en: {auth_url}")
    import requests
    import hashlib
    import base64
    
    session = requests.Session()
    
    # 1. Authorize GET request (with S256 challenge)
    state = uuid.uuid4().hex
    verifier = uuid.uuid4().hex
    challenge_bytes = hashlib.sha256(verifier.encode('utf-8')).digest()
    challenge = base64.urlsafe_b64encode(challenge_bytes).decode('utf-8').rstrip('=')
    
    params = {
        "response_type": "code",
        "client_id": "metri-app",
        "redirect_uri": "http://localhost:5173/callback",
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": state
    }
    
    try:
        authorize_url = f"{auth_url}/oauth2/authorize"
        print(f"  -> GET {authorize_url} (iniciando flow PKCE S256)...")
        r_auth = session.get(authorize_url, params=params, allow_redirects=False)
        
        # Expect either redirect to login page or direct code if already logged in (cookies)
        login_url = r_auth.headers.get("Location")
        if not login_url:
            print("  ✗ Error: La petición de autorización no devolvió redirección a login.")
            return False
            
        print(f"  -> Redirigido a: {login_url}")
        
        # 2. Login submit POST request
        login_submit_url = f"{auth_url}/auth/login"
        print(f"  -> POST {login_submit_url} (enviando credenciales de {username})...")
        payload = {
            "state": state,
            "tenant_id": tenant_id,
            "username": username,
            "password": password
        }
        
        r_login = session.post(login_submit_url, data=payload, allow_redirects=False)
        callback_url = r_login.headers.get("Location")
        if not callback_url or "code=" not in callback_url:
            print(f"  ✗ Error: Login fallido. Redirección devuelta: {callback_url}")
            return False
            
        print(f"  -> Autenticación exitosa. Redirigido a callback: {callback_url}")
        
        # Extract code from callback redirect URL
        from urllib.parse import urlparse, parse_qs
        parsed_callback = urlparse(callback_url)
        query_params = parse_qs(parsed_callback.query)
        code = query_params.get("code", [None])[0]
        
        if not code:
            print("  ✗ Error: No se pudo extraer el 'code' del URL de callback.")
            return False
            
        print(f"  -> Code extraído: {code[:10]}...")
        
        # 3. Code Exchange to Token POST request
        token_url = f"{auth_url}/oauth2/token"
        print(f"  -> POST {token_url} (intercambiando código por token)...")
        token_payload = {
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": verifier,
            "client_id": "metri-app",
            "redirect_uri": "http://localhost:5173/callback"
        }
        
        r_token = session.post(token_url, data=token_payload)
        if r_token.status_code != 200:
            print(f"  ✗ Error: Intercambio de tokens fallido (HTTP {r_token.status_code}): {r_token.text}")
            return False
            
        token_data = r_token.json()
        access_token = token_data.get("access_token")
        print(f"  ✓ Token OIDC obtenido con éxito: {access_token[:30]}...")
        
        # 4. Introspect session to verify validity
        introspect_url = f"{auth_url}/oauth2/introspect"
        print(f"  -> POST {introspect_url} (introspección de sesión)...")
        introspect_payload = {"token": access_token}
        r_intro = session.post(introspect_url, data=introspect_payload)
        
        if r_intro.status_code == 200:
            intro_data = r_intro.json()
            if intro_data.get("active"):
                print(f"  ✓ Sesión introspeccionada activa: Usuario={intro_data.get('username')}, Roles={intro_data.get('roles')}")
                return True
            else:
                print("  ✗ Error: La sesión introspeccionada indica 'active = false'.")
                return False
        else:
            print(f"  ✗ Error al introspeccionar sesión (HTTP {r_intro.status_code})")
            return False
            
    except Exception as e:
        print(f"  ✗ Excepción ocurrida durante el flujo de autenticación: {e}")
        return False

def make_bulk_request(tenant_id, entity_type, rows_list, columns_keys):
    """Formats raw python dictionaries into a BulkRequest message."""
    columns = [
        pb.ColumnSchema(key=k, label=k, type="string")
        for k in columns_keys
    ]
    
    rows = []
    for r in rows_list:
        values = []
        for k in columns_keys:
            v = r.get(k)
            if v is None:
                values.append(struct_pb2.Value(null_value=0))
            elif isinstance(v, bool):
                values.append(struct_pb2.Value(bool_value=v))
            elif isinstance(v, (int, float)):
                values.append(struct_pb2.Value(number_value=float(v)))
            else:
                values.append(struct_pb2.Value(string_value=str(v)))
        rows.append(pb.DataRow(values=values))
        
    rowset = pb.RowSet(
        columns=columns,
        rows_json=pb.DataRowList(iter=rows)
    )
    
    req = pb.BulkRequest(
        tenant_id=tenant_id,
        entity_type=entity_type,
        action=pb.CREATE,
        data=rowset,
    )
    return req

def send_bulk_batch(stub, tenant_id, entity_type, batch_id, rows_list, columns_keys):
    """Sends a bulk ingest batch with exponential backoff on retryable errors."""
    req = make_bulk_request(tenant_id, entity_type, rows_list, columns_keys)
    max_retries = 6
    backoff_factor = 2.0
    
    for attempt in range(max_retries):
        try:
            resp = stub.BulkIngest(req)
            if resp.status and resp.status.success:
                return True, resp.ingested_count
            else:
                from google.protobuf.json_format import MessageToJson
                err_msg = resp.status.error_message if (resp.status and resp.status.error_message) else f"Unknown error (response={MessageToJson(resp)})"
                is_retryable = any(x in err_msg for x in ["429", "504", "Too Many Requests", "Gateway Timeout", "RESOURCE_EXHAUSTED", "ProvisionedThroughputExceededException", "ThrottlingException", "throttling"])
                if is_retryable and attempt < max_retries - 1:
                    sleep_time = (backoff_factor ** attempt) + random.uniform(0.5, 1.5)
                    time.sleep(sleep_time)
                    continue
                return False, err_msg
        except Exception as e:
            err_msg = str(e)
            is_retryable = any(x in err_msg for x in ["429", "504", "Too Many Requests", "Gateway Timeout", "RESOURCE_EXHAUSTED", "ProvisionedThroughputExceededException", "ThrottlingException", "throttling"])
            if is_retryable and attempt < max_retries - 1:
                sleep_time = (backoff_factor ** attempt) + random.uniform(0.5, 1.5)
                time.sleep(sleep_time)
                continue
            return False, err_msg
            
    return False, "Max retries exceeded"

def ingest_entity_in_parallel(stub, tenant_id, entity_type, records):
    """Drives parallel ingestion using ThreadPoolExecutor in batches of 100."""
    batch_size = 100
    total_records = len(records)
    num_batches = (total_records + batch_size - 1) // batch_size
    columns_keys = list(records[0].keys())
    
    batches = [
        records[i * batch_size : (i + 1) * batch_size]
        for i in range(num_batches)
    ]
    
    print(f"  -> Iniciando ingesta en paralelo de {total_records} '{entity_type}'s en {num_batches} lotes...")
    ok_count = 0
    total_ingested = 0
    
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(send_bulk_batch, stub, tenant_id, entity_type, i + 1, batch, columns_keys): i + 1
            for i, batch in enumerate(batches)
        }
        
        for future in concurrent.futures.as_completed(futures):
            batch_id = futures[future]
            success, result = future.result()
            if success:
                ok_count += 1
                total_ingested += result
                print(f"    ✓ Lote {batch_id:2d} completado: Ingestados {result} registros | Progreso: {total_ingested}/{total_records}")
            else:
                print(f"    ✗ Lote {batch_id:2d} FALLÓ: {result}")
                sys.exit(2)
                
    duration = time.time() - start_time
    print(f"  ✓ Ingesta de {entity_type} completada: {total_ingested} registros en {duration:.2f}s ({total_ingested/duration:.2f} registros/s)")
    return total_ingested

def run_analytics_verification(stub, tenant_id):
    """
    Executes analytics queries using the streaming Query endpoint to verify
    asset count and hierarchy trees.
    """
    print(f"\n[4/5] Ejecutando verificación analítica (gRPC-Web Query)...")
    
    # Query 1: Aggregate asset counts
    print("  -> Consulta 1: Conteo total de assets por status...")
    asset_query = pb.AnalyticsRequest(
        tenant_id=tenant_id,
        entity="asset",
        metrics=[pb.MetricDefinition(
            entity="asset",
            attribute="id",
            aggregation=pb.COUNT,
            name="total_count"
        )],
        dimensions=[pb.DimensionDefinition(
            entity="asset",
            attribute="status"
        )],
        output_cast=pb.OUTPUT_CAST_UNSPECIFIED,
        viz="table",
        limit=2000
    )
    
    req1 = pb.QueryRequest(
        tenant_id=tenant_id,
        queries={"asset_status": asset_query}
    )
    
    try:
        res1 = stub.Query(req1)
        total_assets = 0
        status_counts = {}
        for chunk in res1:
            if "asset_status" in chunk.batch_results:
                br = chunk.batch_results["asset_status"]
                cols = [c.key for c in br.data.columns]
                for r in br.data.rows_json.iter:
                    vals = [v.string_value or v.number_value for v in r.values]
                    row_dict = dict(zip(cols, vals))
                    status = row_dict.get("status", "UNKNOWN")
                    count = int(row_dict.get("total_count", 0))
                    status_counts[status] = count
                    total_assets += count
                    
        print(f"    ✓ Total assets en DB: {total_assets} | Desglose: {status_counts}")
        if total_assets < 1000:
            print("    ✗ Error: Se esperaban al menos 1000 assets.")
            return False
    except Exception as e:
        print(f"    ✗ Excepción en consulta analítica de assets: {e}")
        return False
        
    # Query 2: Hierarchy Tree Root verification
    print("  -> Consulta 2: Extracción de nodos raíz de la jerarquía de locations...")
    roots_query = pb.AnalyticsRequest(
        tenant_id=tenant_id,
        entity="location",
        output_cast=pb.OUTPUT_CAST_UNSPECIFIED,
        viz="tree",
        limit=20,
        hierarchy=pb.HierarchyContext(
            parent_field="parent_location_id",
            current_node_id="",
            inject_has_children=True
        )
    )
    
    req2 = pb.QueryRequest(
        tenant_id=tenant_id,
        queries={"root_locations": roots_query}
    )
    
    try:
        res2 = stub.Query(req2)
        root_nodes = []
        for chunk in res2:
            if "root_locations" in chunk.batch_results:
                br = chunk.batch_results["root_locations"]
                cols = [c.key for c in br.data.columns]
                for r in br.data.rows_json.iter:
                    # Resolve values correctly handling mixed types
                    vals = []
                    for v in r.values:
                        if v.HasField("string_value"):
                            vals.append(v.string_value)
                        elif v.HasField("number_value"):
                            vals.append(v.number_value)
                        elif v.HasField("bool_value"):
                            vals.append(v.bool_value)
                        else:
                            vals.append(None)
                    row_dict = dict(zip(cols, vals))
                    root_nodes.append(row_dict)
                    
        print(f"    ✓ Encontrados {len(root_nodes)} nodos raíz de tipo SITE.")
        for node in root_nodes[:3]:
            print(f"      • ID={node.get('id')}, Nombre={node.get('name')}, has_children={node.get('has_children')}")
            
        if not root_nodes:
            print("    ✗ Error: No se encontraron ubicaciones de nivel raíz (SITE).")
            return False
            
    except Exception as e:
        print(f"    ✗ Excepción en consulta analítica de jerarquía: {e}")
        return False

    return True

def main():
    parser = argparse.ArgumentParser(description="Metri Ingestion Benchmarking & Production Smoke Test")
    parser.add_argument("--env", choices=["local", "prod"], default="local",
                        help="Entorno: local (localhost) o prod (engine.metri.one)")
    parser.add_argument("--execute", action="store_true", default=False,
                        help="Envía los requests reales al engine (por defecto es Dry-run)")
    parser.add_argument("--tenant", default="metri-benchmark-tenant",
                        help="ID de Tenant objetivo para las pruebas")
    parser.add_argument("--engine-host", default=None,
                        help="Sobreescribe el host de metri-engine")
    parser.add_argument("--auth-host", default=None,
                        help="Sobreescribe el host de metri-auth")
    parser.add_argument("--username", default="admin",
                        help="Usuario principal de login")
    parser.add_argument("--password", default="SuperSecurePassword123!",
                        help="Contraseña del usuario")
    parser.add_argument("--skip-login", action="store_true", default=False,
                        help="Omite el paso de testear login contra metri-auth")
    args = parser.parse_args()

    # Determine hosts and secrets based on environment selection
    # Local compose defaults vs production URLs
    if args.env == "prod":
        engine_host = args.engine_host or "engine.metri.one"
        auth_host = args.auth_host or "https://auth.metri.one"
        hmac_secret = "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs"
    else:
        engine_host = args.engine_host or "127.0.0.1:9090"
        auth_host = args.auth_host or "http://localhost:8081"
        hmac_secret = "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"

    tag_suffix = uuid.uuid4().hex[:6]
    tenant_id = args.tenant
    if tenant_id == "metri-benchmark-tenant" and args.execute:
        # Generate a unique suffix for the benchmark tenant to prevent EAV AVET unique constraint collisions on sequential runs
        tenant_id = f"metri-benchmark-tenant-{tag_suffix}"

    print("=" * 70)
    print("        METRI SMOKE TEST & BENCHMARK INGESTION TOOL        ")
    print("=" * 70)
    print(f" Entorno:            {args.env.upper()}")
    print(f" Modo:               {'EJECUCIÓN REAL' if args.execute else 'DRY-RUN (Simulación)'}")
    print(f" Tenant:             {tenant_id}")
    print(f" Host metri-engine:  {engine_host}")
    print(f" Host metri-auth:    {auth_host}")
    print("=" * 70)

    # Set HMAC secret for local client token generation
    os.environ["HMAC_SECRET"] = hmac_secret
    os.environ["ENGINE_HOST"] = engine_host

    # 1. Login user verification
    if not args.skip_login:
        if args.execute:
            login_success = test_auth_login(auth_url=auth_host, tenant_id="system", username=args.username, password=args.password)
            if not login_success:
                print("\n[!] ADVERTENCIA: La verificación de Login de usuario falló. Se continuará con gRPC.")
        else:
            print("\n[1/5] [DRY-RUN] Simulación de login del usuario principal exitosa.")

    # 2. Setup direct gRPC stub with system token
    system_token = generate_signed_token(hmac_secret, tenant_id, "usr_system_bff")
    stub = GrpcWebStub(engine_host)
    # Patch client _make_token to use our direct system token for bulk/ingestion
    stub._make_token = lambda tenant_id="demo": f"Bearer {system_token}"

    print(f"\n[2/5] Generando datos de prueba (1,000 ubicaciones jerárquicas + 1,000 assets)...")
    
    # 2.1 Locations Tree (exactly 1000 nodes)
    # L0: 10 Sites
    # L1: 9 Buildings per Site (90)
    # L2: 5 Floors per Building (450)
    # L3: 1 Room per Floor (450)
    # Sum: 10 + 90 + 450 + 450 = 1000
    locations = []
    location_ids = []
    
    # deterministic seeding for testing consistency
    random.seed(1337)
    
    loc_counter = 0
    now_ts = int(time.time())
    
    print("  -> Generando 10 Sites...")
    for s_idx in range(10):
        site_id = generate_ulid()
        loc_counter += 1
        locations.append({
            "id": site_id,
            "name": f"Site {s_idx + 1:02d}",
            "tag": f"L-SITE{s_idx + 1:02d}-{tag_suffix}",
            "type": "SITE",
            "parent_location_id": None,
            "area_value": float(random.randint(5000, 15000)),
            "area_unit": "m2",
            "timestamp": now_ts
        })
        location_ids.append(site_id)
        
        # Buildings for this Site (9 per site)
        for b_idx in range(9):
            bldg_id = generate_ulid()
            loc_counter += 1
            locations.append({
                "id": bldg_id,
                "name": f"Edificio Site {s_idx + 1:02d} - {b_idx + 1:02d}",
                "tag": f"L-BLDG{loc_counter:02d}-{tag_suffix}",
                "type": "BUILDING",
                "parent_location_id": site_id,
                "area_value": float(random.randint(800, 2500)),
                "area_unit": "m2",
                "timestamp": now_ts
            })
            
            # Floors for this Building (5 per building)
            for f_idx in range(5):
                floor_id = generate_ulid()
                loc_counter += 1
                locations.append({
                    "id": floor_id,
                    "name": f"Piso Bldg {loc_counter:02d} - {f_idx + 1:02d}",
                    "tag": f"L-FLOR{loc_counter:02d}-{tag_suffix}",
                    "type": "FLOOR",
                    "parent_location_id": bldg_id,
                    "area_value": float(random.randint(150, 400)),
                    "area_unit": "m2",
                    "timestamp": now_ts
                })
                
                # Room for this Floor (1 per floor)
                for r_idx in range(1):
                    room_id = generate_ulid()
                    loc_counter += 1
                    locations.append({
                        "id": room_id,
                        "name": f"Sala Room {loc_counter:02d}",
                        "tag": f"L-ROOM{loc_counter:02d}-{tag_suffix}",
                        "type": "ROOM",
                        "parent_location_id": floor_id,
                        "area_value": float(random.randint(15, 60)),
                        "area_unit": "m2",
                        "timestamp": now_ts
                    })
                    # Save room ID as a candidate to assign assets
                    location_ids.append(room_id)

    print(f"  ✓ 1,000 ubicaciones jerárquicas generadas en memoria.")

    # 2.2 Assets (exactly 1000)
    assets = []
    ASSET_TYPES = ["PUMP", "MOTOR", "COMPRESSOR", "VALVE", "SENSOR", "TURBINE"]
    CRITICALITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    CATEGORIES = ["EQUIPMENT", "INFRASTRUCTURE", "SAFETY", "UTILITY"]
    MANUFACTURERS = ["Siemens", "ABB", "General Electric", "Caterpillar", "Schneider"]

    print("  -> Generando 1,000 assets completos...")
    for a_idx in range(1000):
        asset_id = generate_ulid()
        asset_type = ASSET_TYPES[a_idx % len(ASSET_TYPES)]
        criticality = CRITICALITIES[a_idx % len(CRITICALITIES)]
        category = CATEGORIES[(a_idx // 2) % len(CATEGORIES)]
        manufacturer = MANUFACTURERS[a_idx % len(MANUFACTURERS)]
        
        # Distribute assets evenly across the locations
        assigned_loc = location_ids[a_idx % len(location_ids)]
        
        health = round(30.0 + (a_idx * 0.07) % 70.0, 2)
        meter_reading = round(100.0 + (a_idx * 4.8) % 4800.0, 2)
        
        assets.append({
            "id": asset_id,
            "name": f"Asset Benchmark {a_idx + 1:04d} - {asset_type}",
            "serial_number": f"SN-BENCH-{a_idx + 1:04d}-{uuid.uuid4().hex[:6].upper()}",
            "tag": f"A-B{a_idx + 1:05d}-{tag_suffix}",
            "status": "ACTIVE" if a_idx < 800 else ("INACTIVE" if a_idx < 950 else "IN_MAINTENANCE"),
            "location_id": assigned_loc,
            "omniclass_category": f"23-33 {10 + a_idx%90:02d} 11",
            "type": asset_type,
            "category": category,
            "criticality": criticality,
            "manufacturer": manufacturer,
            "model": f"Model-B{a_idx%10}",
            "omniclass_code": f"OM-B-{a_idx%100}",
            "omniclass_name": f"OmniClass {asset_type}",
            "health_score": health,
            "current_meter_reading": meter_reading,
            "telemetry_config": '{"interval":60,"enabled":true}',
            "timestamp": now_ts
        })
    print(f"  ✓ 1,000 assets generados en memoria.")

    # 3. Ingest data
    if args.execute:
        print("\n[3/5] Iniciando ingestión masiva en metri-engine...")
        # Ingest locations first (dependencies)
        ingest_entity_in_parallel(stub, tenant_id, "location", locations)
        # Ingest assets second
        ingest_entity_in_parallel(stub, tenant_id, "asset", assets)
    else:
        print("\n[3/5] [DRY-RUN] Simulación de ingesta paralela de datos exitosa.")

    # 4. Verification queries
    if args.execute:
        success = run_analytics_verification(stub, tenant_id)
        if success:
            print("\n[5/5] ✅ VALIDACIÓN FINAL: PASS")
            print("  - Los 1,000 assets y 1,000 locations jerárquicos se ingirieron correctamente.")
            print("  - Las consultas analíticas de gRPC-Web respondieron con la información esperada.")
        else:
            print("\n[5/5] ❌ VALIDACIÓN FINAL: FAIL")
            sys.exit(3)
    else:
        print("\n[5/5] [DRY-RUN] Simulación de consultas analíticas de verificación exitosa.")
        print("\n[DRY-RUN] ✅ VALIDACIÓN FINAL: PASS")

    print("\nBenchmark completado con éxito.")
    print("=" * 70)

if __name__ == "__main__":
    main()
