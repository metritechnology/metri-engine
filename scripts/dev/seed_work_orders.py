#!/usr/bin/env python3
"""Seed de work orders para verificar el calendario en local."""
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR))

from seed_base import SeederBase, DEFAULT_HMAC_SECRET  # noqa: E402


class WorkOrderSeeder(SeederBase):
    def seed(self, count: int):
        now = time.time()
        today_start = int(now) // 86400 * 86400
        titles = [
            ("Inspección de bomba hidráulica", "OPEN", "WO-90001"),
            ("Cambio de rodamientos motor A", "IN_PROGRESS", "WO-90002"),
            ("Calibración de sensor de presión", "OPEN", "WO-90003"),
            ("Revisión de tablero eléctrico", "REVIEW", "WO-90004"),
            ("Engrase de transportador", "OPEN", "WO-90005"),
            ("Sustitución de filtro de aire", "CLOSED", "WO-90006"),
        ]
        created = 0
        for idx in range(min(count, len(titles))):
            title, status, number = titles[idx]
            # due dates repartidas: hoy ± días, a horas locales concretas (09:00, 14:00, 11:00…)
            day_offset = idx - 1
            hour = (9 + idx * 3) % 24
            due_ms = int((today_start + day_offset * 86400 + hour * 3600) * 1000)
            end_ms = due_ms + 2 * 3600 * 1000
            payload = {
                "work_order_number": number,
                "title": title,
                "description": f"Orden sembrada para pruebas del calendario ({title})",
                "category": "PREVENTIVE",
                "priority": "MEDIUM",
                "status": status,
                "due_date": due_ms,
                "scheduled_end": end_ms,
            }
            entity_id = self.transact("work_order", payload)
            if entity_id:
                created += 1
                print(f"  ✓ work_order {number} due={due_ms} id={entity_id}")
        print(f"--- {created}/{min(count, len(titles))} work orders creadas para tenant {self.tenant_id}")


def main():
    seeder = WorkOrderSeeder(
        tenant_id="system",
        host="localhost",
        port=9090,
        hmac_secret="c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2",
        dry_run=False,
    )
    seeder.seed(6)


if __name__ == "__main__":
    main()
