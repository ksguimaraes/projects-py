"""
Uso:
  python find_riskfindings_key.py            → varre todo o JSON e salva
                                               riskfindings_keys_summary.json
  python find_riskfindings_key.py <chave>    → inspeciona uma chave específica
"""

import json
import sys
import os
from collections import Counter, defaultdict

SCRIPT_DIR = os.path.dirname(__file__)
JSON_PATH = os.path.join(SCRIPT_DIR, "json", "report-Scheduled-alert-report-2026-04-08.json")
OUT_PATH   = os.path.join(SCRIPT_DIR, "json", "riskfindings_keys_summary.json")


def load_data() -> list:
    with open(JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return data


def infer_value_kind(value) -> str:
    if isinstance(value, list):
        if not value:
            return "array_empty"
        if isinstance(value[0], dict):
            return "array_of_objects"
        return "array_of_scalars"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def scan_all_keys(data: list) -> dict:
    """Varre todos os registros e classifica cada chave do RiskFindings."""
    key_kinds:   dict[str, Counter] = defaultdict(Counter)
    key_counts:  Counter = Counter()
    key_example: dict[str, dict] = {}

    for rec in data:
        rf = rec.get("RiskFindings") or rec.get("riskfindings")
        if not isinstance(rf, dict):
            continue
        for k, v in rf.items():
            kind = infer_value_kind(v)
            key_kinds[k][kind] += 1
            if kind == "array_of_objects":
                key_counts[k] += 1
                if k not in key_example:
                    key_example[k] = {
                        "alert_id": rec.get("AlertId") or rec.get("alertid"),
                        "sample_element": v[0] if v else None,
                    }

    result = []
    for key, count in sorted(key_counts.items(), key=lambda x: -x[1]):
        result.append({
            "key": key,
            "alert_count": count,
            "value_kinds": dict(key_kinds[key]),
            "sample_alert_id": key_example[key]["alert_id"],
            "sample_element": key_example[key]["sample_element"],
        })

    scalar_keys = [
        {"key": k, "value_kinds": dict(v)}
        for k, v in key_kinds.items()
        if k not in key_counts
    ]

    return {
        "total_records": len(data),
        "object_array_keys_count": len(result),
        "object_array_keys": result,
        "other_keys_count": len(scalar_keys),
        "other_keys": sorted(scalar_keys, key=lambda x: x["key"]),
    }


def inspect_key(data: list, key: str) -> None:
    print(f"Procurando RiskFindings['{key}'] em {JSON_PATH}...\n")
    for i, rec in enumerate(data):
        rf = rec.get("RiskFindings") or rec.get("riskfindings") or {}
        value = rf.get(key)
        if value is not None:
            alert_id = rec.get("AlertId") or rec.get("alertid")
            print(f"Registro #{i}  AlertId={alert_id}")
            print(f"Tipo: {infer_value_kind(value)}")
            print(json.dumps(value if not isinstance(value, list) else value[:3], indent=2))
            return
    print(f"Nenhum registro com RiskFindings['{key}'] encontrado.")


# ── entry point ───────────────────────────────────────────────────────────────

data = load_data()

if len(sys.argv) > 1:
    inspect_key(data, sys.argv[1])
else:
    print(f"Varrendo {len(data):,} registros...")
    summary = scan_all_keys(data)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"Salvo em: {OUT_PATH}")
    print(f"  {summary['object_array_keys_count']} chaves com array de objetos")
    print(f"  {summary['other_keys_count']} outras chaves (escalares/vazias)")
    for entry in summary["object_array_keys"]:
        print(f"  {entry['key']:<40} {entry['alert_count']} alertas")
