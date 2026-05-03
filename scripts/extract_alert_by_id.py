"""
Localiza um alerta pelo AlertId e salva o registro completo em JSON.

Uso:
  python extract_alert_by_id.py <alert_id>
  python extract_alert_by_id.py orca-68727
"""

import json
import sys
import os

SCRIPT_DIR = os.path.dirname(__file__)
JSON_PATH  = os.path.join(SCRIPT_DIR, "json", "report-Scheduled-alert-report-2026-04-08.json")

alert_id = sys.argv[1] if len(sys.argv) > 1 else "orca-68727"

with open(JSON_PATH, encoding="utf-8") as f:
    data = json.load(f)

if isinstance(data, dict):
    for v in data.values():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            data = v
            break

found = next(
    (rec for rec in data if rec.get("AlertId") == alert_id or rec.get("alertid") == alert_id),
    None,
)

if found is None:
    print(f"AlertId '{alert_id}' não encontrado.")
    sys.exit(1)

out_path = os.path.join(SCRIPT_DIR, "json", f"alert_{alert_id.replace('/', '_')}.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(found, f, indent=2, ensure_ascii=False)

print(f"Salvo em: {out_path}")
