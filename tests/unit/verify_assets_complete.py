#!/usr/bin/env python3
import os
import sys
import argparse
import struct
import requests
import json
import pathlib

# Add proto directory to path
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
ENGINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.append(str(ENGINE_ROOT / "scripts" / "proto"))

try:
    import metri_pb2 as pb
except ImportError as e:
    print(f"Error importing stubs: {e}")
    sys.exit(1)

class GrpcWebStub:
    def __init__(self, host):
        self.host = host
        scheme = "http" if "127.0.0.1" in self.host or "localhost" in self.host else "https"
        self.base = f"{scheme}://{self.host}"
        self.session = requests.Session()

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
        tenant_id = getattr(proto_msg, "tenant_id", "demo") or "demo"
        headers = {
            "Content-Type": "application/grpc-web+proto",
            "Accept": "application/grpc-web+proto",
            "x-tenant-id": tenant_id,
            "x-grpc-web": "1",
            "X-Metri-Origin-Token": "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs",
        }
        resp = self.session.post(f"{self.base}{path}", data=framed, headers=headers, timeout=30)
        resp.raise_for_status()
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


def main():
    parser = argparse.ArgumentParser(description="Verificador de integridad de datos de Activos")
    parser.add_argument("--tenant", default="demo", help="Tenant ID a verificar")
    parser.add_argument("--host", default="127.0.0.1:9091", help="Host/puerto del proxy Envoy gRPC")
    args = parser.parse_args()

    stub = GrpcWebStub(args.host)
    tenant_id = args.tenant

    req = pb.QueryRequest(
        tenant_id=tenant_id,
        queries={
            "all_assets": pb.AnalyticsRequest(
                tenant_id=tenant_id,
                entity="asset",
                viz="table",
                limit=1000
            )
        }
    )

    print(f"Consultando assets para tenant '{tenant_id}' en {args.host}...")
    assets = []
    columns = []

    try:
        for chunk in stub.Query(req):
            for key, res in chunk.batch_results.items():
                if res.data and res.data.rows_json and res.data.rows_json.iter:
                    columns = [col.key for col in res.data.columns]
                    for r in res.data.rows_json.iter:
                        row_dict = {}
                        for col, val in zip(res.data.columns, r.values):
                            # Extraer valor según tipo de protobuf Value
                            val_type = val.WhichOneof("kind")
                            if val_type == "string_value":
                                row_dict[col.key] = val.string_value
                            elif val_type == "number_value":
                                row_dict[col.key] = val.number_value
                            elif val_type == "bool_value":
                                row_dict[col.key] = val.bool_value
                            elif val_type == "null_value":
                                row_dict[col.key] = None
                            else:
                                row_dict[col.key] = None
                        assets.append(row_dict)
    except Exception as e:
        print(f"Error al realizar la consulta gRPC: {e}", file=sys.stderr)
        sys.exit(1)

    if not assets:
        print(f"Error: No se encontraron assets para el tenant '{tenant_id}'. ¿Se ejecutó el seeder?", file=sys.stderr)
        sys.exit(1)

    print(f"Se encontraron {len(assets)} assets. Columnas en la respuesta: {columns}")

    # Verificar presencia de las columnas requeridas
    required_cols = ["health_score", "criticality"]
    missing_cols = [c for c in required_cols if c not in columns]
    if missing_cols:
        print(f"Error: Faltan las siguientes columnas en la respuesta: {missing_cols}", file=sys.stderr)
        sys.exit(1)

    errors = []
    for idx, asset in enumerate(assets):
        name = asset.get("name", f"Asset #{idx}")
        asset_id = asset.get("id", f"unknown-id-{idx}")
        
        # Validar health_score
        hs = asset.get("health_score")
        if hs is None:
            errors.append(f"Asset '{name}' ({asset_id}) no tiene health_score (es nulo o no existe)")
        else:
            try:
                hs_val = float(hs)
                if not (0 <= hs_val <= 100):
                    errors.append(f"Asset '{name}' ({asset_id}) tiene un health_score fuera de rango [0, 100]: {hs_val}")
            except ValueError:
                errors.append(f"Asset '{name}' ({asset_id}) tiene un health_score inválido (no es numérico): {hs}")

        # Validar criticality
        crit = asset.get("criticality")
        if crit is None:
            errors.append(f"Asset '{name}' ({asset_id}) no tiene criticality")
        elif crit not in ["A", "B", "C"]:
            errors.append(f"Asset '{name}' ({asset_id}) tiene criticality fuera de los valores esperados [A, B, C]: {crit}")

    if errors:
        print("\n--- ERRORES ENCONTRADOS ---", file=sys.stderr)
        for err in errors:
            print(f"✗ {err}", file=sys.stderr)
        sys.exit(1)

    print("\n✓ ¡Todos los assets tienen health_score y criticality correctos!")
    sys.exit(0)

if __name__ == "__main__":
    main()
