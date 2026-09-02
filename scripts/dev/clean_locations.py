#!/usr/bin/env python3
import sys
import os
import argparse
from pathlib import Path

# Add script directory to path to enable local imports
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR))

try:
    from seed_base import SeederBase, DEFAULT_HMAC_SECRET, GRPC_AVAILABLE
except ImportError:
    print("✗ Error: No se pudo importar seed_base.py. Asegúrate de ejecutar el script en su directorio.")
    sys.exit(1)

class LocationCleaner(SeederBase):
    def clean_all_locations(self):
        print(f"\n--- Limpiando tabla existente de location para el tenant: {self.tenant_id} ---")
        self.clear_entity("location")

def main():
    parser = argparse.ArgumentParser(description="Metri Location Table Cleaner")
    parser.add_argument("--host", default="localhost", help="gRPC host")
    parser.add_argument("--port", type=int, default=9090, help="gRPC port")
    parser.add_argument("--tenant", default="demo", help="Tenant ID")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC sign secret")
    parser.add_argument("--execute", action="store_true", help="Send actual requests instead of dry-run")
    parser.add_argument("--force-non-local", action="store_true", help="Bypass local host safety check")

    args = parser.parse_args()

    # ── Security Check: Environment ──
    env = os.environ.get("ENVIRONMENT", "").lower()
    if env in ("production", "prod", "staging"):
        print(f"✗ Security Error: Clean script cannot be run in a production or staging environment (ENVIRONMENT={env}).")
        sys.exit(1)

    # ── Security Check: Local Host ──
    is_local_host = args.host in ("localhost", "127.0.0.1", "0.0.0.0", "host.docker.internal")
    if not is_local_host and not args.force_non_local:
        print(f"✗ Security Error: Clean script cannot be run against remote hosts unless --force-non-local is specified.")
        sys.exit(1)

    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available.")
        sys.exit(1)

    dry_run = not args.execute
    cleaner = LocationCleaner(
        tenant_id=args.tenant,
        host=args.host,
        port=args.port,
        hmac_secret=args.secret,
        dry_run=dry_run
    )

    cleaner.clean_all_locations()

if __name__ == "__main__":
    main()
