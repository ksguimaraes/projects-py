"""
Analisa um JSON de alertas e identifica possíveis subtabelas relacionais.

Saída:
  - Resumo dos campos raiz (tipo, % preenchido, cardinalidade estimada)
  - Subtabelas sugeridas com campos e tipos inferidos
  - Avisos sobre campos com schemas variáveis (ex.: RiskFindings)
"""

import json
import sys
import os
from collections import defaultdict, Counter
from typing import Any


# ─── helpers ────────────────────────────────────────────────────────────────

def python_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def infer_column_type(types: Counter) -> str:
    non_null = {t: c for t, c in types.items() if t != "null"}
    if not non_null:
        return "null"
    dominant = max(non_null, key=non_null.get)
    return dominant


def analyze_fields(records: list[dict], prefix: str = "") -> dict:
    """Retorna estatísticas por campo para uma lista de dicts."""
    field_types: dict[str, Counter] = defaultdict(Counter)
    field_counts: Counter = Counter()
    total = len(records)

    for rec in records:
        for k, v in rec.items():
            col = f"{prefix}{k}" if prefix else k
            field_types[col][python_type(v)] += 1
            if v is not None:
                field_counts[col] += 1

    result = {}
    for col, types in field_types.items():
        filled = field_counts.get(col, 0)
        result[col] = {
            "inferred_type": infer_column_type(types),
            "type_dist": dict(types),
            "filled_pct": round(filled / total * 100, 1),
            "total": total,
        }
    return result


def unique_keys_across(records: list[dict]) -> set[str]:
    keys: set[str] = set()
    for r in records:
        keys.update(r.keys())
    return keys


def detect_list_element_type(samples: list) -> str:
    for item in samples:
        if isinstance(item, dict):
            return "object"
        if isinstance(item, list):
            return "nested_array"
    return "scalar"


def _resolve_case_conflicts(cols: list[TableColumn]) -> list[TableColumn]:
    """Renomeia colunas que conflitem em case: o que inicia com maiúscula recebe '_' no final."""
    lower_counts: Counter = Counter(name.lower() for name, _, _ in cols)
    result = []
    for name, typ, pct in cols:
        if lower_counts[name.lower()] > 1 and name != name.lower():
            result.append((name + "_", typ, pct))
        else:
            result.append((name, typ, pct))
    return result


def _find_id_field(records: list[dict]) -> str | None:
    """Retorna o nome exato do campo 'id' (qualquer case) no primeiro registro que o tiver."""
    for rec in records[:200]:
        for key in rec:
            if key.lower() == "id":
                return key
    return None


def array_elem_type(rows: list[dict], col: str) -> str:
    """Verifica o tipo dos elementos de um campo array em uma lista de registros."""
    for row in rows:
        val = row.get(col)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    return "object"
    return "scalar"


# ─── análise principal ───────────────────────────────────────────────────────

# Coluna nomeada de uma tabela sugerida
TableColumn = tuple[str, str, float]   # (nome, tipo, filled_pct)
TableSchema = dict[str, list[TableColumn]]  # table_name → colunas


def _print_columns(cols: list[TableColumn], indent: str = "       ") -> None:
    print(f"{indent}{'Coluna':<35} {'Tipo':<12} {'Preench.':>8}")
    print(f"{indent}{'-'*35} {'-'*12} {'-'*8}")
    for name, typ, pct in cols:
        flag = "  ← sparse" if pct < 30 else ""
        print(f"{indent}{name:<35} {typ:<12} {pct:>7}%{flag}")


def _build_summary_lines(schemas: TableSchema, object_prefixes: list[str] | None = None) -> list[str]:
    prefixes = [p.lower() + "_" for p in (object_prefixes or [])]

    def _col_lines(table: str, cols: list[TableColumn]) -> list[str]:
        out = []
        seen_groups: set[str] = set()
        for name, typ, pct in cols:
            # detecta se a coluna pertence a um grupo de objeto inline
            group = next((p for p in prefixes if name.lower().startswith(p)), None)
            if group and table == "alerts":
                if group in seen_groups:
                    continue
                seen_groups.add(group)
                # agrega todas as sub-colunas deste grupo
                sub = [(n, t, p) for n, t, p in cols if n.lower().startswith(group)]
                avg_pct = sum(p for _, _, p in sub) / len(sub) if sub else 0
                group_name = group.rstrip("_")
                out.append(f"  │   {group_name:<33} object({len(sub)} campos)  avg {avg_pct:.0f}%")
            else:
                marker = " [FK]" if name == "alert_id" else ""
                sparse = "  ← sparse" if pct < 30 else ""
                out.append(f"  │   {name:<33} {typ:<12}{marker}{sparse}")
        return out

    lines = []
    lines.append(f"\n{'='*70}")
    lines.append("  RESUMO — ESQUEMA RELACIONAL SUGERIDO")
    lines.append(f"{'='*70}")
    for table, cols in schemas.items():
        lines.append(f"\n  ┌─ {table}")
        lines.extend(_col_lines(table, cols))
        lines.append(f"  └{'─'*55}")

    # totalizador
    table_names = list(schemas.keys())
    lines.append(f"\n{'='*70}")
    lines.append(f"  TOTAL: {len(table_names)} tabela(s)")
    lines.append(f"{'─'*70}")
    for i, name in enumerate(table_names, 1):
        n_cols = len(schemas[name])
        lines.append(f"  {i:>2}. {name:<40} ({n_cols} colunas)")
    lines.append(f"{'='*70}")
    lines.append("")
    return lines


def _print_summary(schemas: TableSchema, object_prefixes: list[str] | None = None) -> None:
    for line in _build_summary_lines(schemas, object_prefixes):
        print(line)


def _write_summary(schemas: TableSchema, out_path: str, object_prefixes: list[str] | None = None) -> None:
    lines = _build_summary_lines(schemas, object_prefixes)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\n  Resumo salvo em: {out_path}")


def analyze(data: list[dict]) -> dict:
    total = len(data)
    subtable_schemas: dict[str, list[TableColumn]] = {}  # só subtabelas, alerts é montada ao fim
    main_cols: list[TableColumn] = []

    print(f"\n{'='*70}")
    print(f"  RELATÓRIO DE SUBTABELAS POTENCIAIS")
    print(f"{'='*70}")
    print(f"  Total de registros (raiz): {total:,}\n")

    # ── 1. classificar campos raiz ───────────────────────────────────────────
    root_stats = analyze_fields(data)

    flat_fields: list[str] = []
    scalar_array_fields: list[str] = []   # arrays de scalars → coluna JSON em alerts
    object_array_fields: list[str] = []   # arrays de objetos → subtabela 1:N
    object_fields: list[str] = []         # objetos aninhados → flatten em alerts

    for col, info in root_stats.items():
        t = info["inferred_type"]
        if t == "array":
            if array_elem_type(data, col) == "object":
                object_array_fields.append(col)
            else:
                scalar_array_fields.append(col)
        elif t == "object":
            object_fields.append(col)
        else:
            flat_fields.append(col)

    # ── 2. campos escalares → tabela principal ───────────────────────────────
    for col in flat_fields:
        info = root_stats[col]
        main_cols.append((col, info["inferred_type"], info["filled_pct"]))

    # arrays de scalars → coluna JSON em alerts (sem subtabela)
    for col in scalar_array_fields:
        info = root_stats[col]
        samples = [row[col] for row in data if isinstance(row.get(col), list)]
        all_items = [i for lst in samples for i in lst]
        distinct = set(str(i) for i in all_items[:2000])
        main_cols.append((col, "array<string>", info["filled_pct"]))
        print(f"  [array<string> em alerts] {col:<28} {len(distinct)} valores distintos  ex: {list(distinct)[:3]}")

    # ── 3. objetos aninhados → flatten em alerts + subtabelas internas ───────
    print(f"\n{'─'*70}")
    print("  [OBJETOS ANINHADOS → campos inline em alerts]")
    print(f"{'─'*70}")

    for col in object_fields:
        obj_records = [row[col] for row in data if isinstance(row.get(col), dict)]
        if not obj_records:
            continue
        prefix = col.lower() + "_"
        sub_stats = analyze_fields(obj_records[:5000])
        unique_cols_count = len(unique_keys_across(obj_records[:5000]))

        print(f"\n  {col}  ({len(obj_records):,} registros, {unique_cols_count} chaves distintas)")
        if unique_cols_count > 8:
            print(f"    ⚠  Schema variável — campos sparse esperados.")

        for sub_col, si in sub_stats.items():
            t = si["inferred_type"]
            filled = si["filled_pct"]
            full_name = f"{prefix}{sub_col}"

            if t == "array":
                if array_elem_type(obj_records[:5000], sub_col) == "object":
                    # array de objetos dentro do objeto → vira subtabela
                    all_items = [
                        item
                        for rec in obj_records
                        if isinstance(rec.get(sub_col), list)
                        for item in rec[sub_col]
                        if isinstance(item, dict)
                    ]
                    table_name = f"alert_{col}_{sub_col}".lower()
                    print(f"    → SUBTABELA (array de objetos): {table_name}")
                    if all_items:
                        inner_stats = analyze_fields(all_items[:5000])

                        # FK primária
                        fk_cols: list[TableColumn] = [("alert_id", "string", 100.0)]

                        # FK do pai: inclui o id do objeto pai se existir
                        parent_id_key = _find_id_field(obj_records)
                        if parent_id_key:
                            parent_id_pct = sub_stats.get(parent_id_key, {}).get("filled_pct", 0.0)
                            fk_cols.append((f"{col.lower()}_id", "string", parent_id_pct))

                        sub_cols: list[TableColumn] = fk_cols + [
                            (sc, si2["inferred_type"], si2["filled_pct"])
                            for sc, si2 in inner_stats.items()
                        ]
                        sub_cols = _resolve_case_conflicts(sub_cols)
                        _print_columns(sub_cols)
                        subtable_schemas[table_name] = sub_cols
                else:
                    print(f"    [array<string> em alerts] {full_name}")
                    main_cols.append((full_name, "array<string>", filled))
            elif t == "object":
                main_cols.append((full_name, "object(json)", filled))
                print(f"    [object(json) em alerts]  {full_name}")
            else:
                flag = "  ← sparse" if filled < 30 else ""
                print(f"    [alerts.{full_name:<30}] {t:<12} {filled:>6.1f}%{flag}")
                main_cols.append((full_name, t, filled))

    # ── 4. arrays de objetos na raiz → subtabelas 1:N ───────────────────────
    print(f"\n{'─'*70}")
    print("  [ARRAYS DE OBJETOS → SUBTABELAS 1:N]")
    print(f"{'─'*70}")

    for col in object_array_fields:
        samples = [row[col] for row in data if isinstance(row.get(col), list)]
        all_items = [item for lst in samples for item in lst if isinstance(item, dict)]
        lengths = [len(row.get(col, [])) for row in data]
        avg_len = sum(lengths) / total if total else 0
        max_len = max(lengths) if lengths else 0
        table_name = f"alert_{col.lower()}"

        print(f"\n  Coluna: {col}")
        print(f"    Média de itens/reg : {avg_len:.1f}  |  Máx: {max_len}")
        print(f"    → SUBTABELA: {table_name}")

        if all_items:
            sub_stats = analyze_fields(all_items[:5000])
            unique_cols = unique_keys_across(all_items[:5000])
            if len(unique_cols) > len(sub_stats) * 0.8 and len(unique_cols) > 5:
                print(f"    ⚠  Schema variável ({len(unique_cols)} chaves distintas) — considere coluna JSON.")
            sub_cols = [("alert_id", "string", 100.0)] + [
                (sc, si["inferred_type"], si["filled_pct"])
                for sc, si in sub_stats.items()
            ]
            sub_cols = _resolve_case_conflicts(sub_cols)
            _print_columns(sub_cols)
            subtable_schemas[table_name] = sub_cols

    # ── 5. imprimir tabela principal completa ────────────────────────────────
    print(f"\n{'─'*70}")
    print("  [TABELA PRINCIPAL]  alerts  (campos finais)")
    print(f"{'─'*70}")
    _print_columns(main_cols, indent="  ")

    # ── 6. resumo com colunas ────────────────────────────────────────────────
    schemas: TableSchema = {"alerts": main_cols, **subtable_schemas}
    _print_summary(schemas, object_prefixes=object_fields)
    return schemas, object_fields


# ─── entry point ─────────────────────────────────────────────────────────────

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__),
        "json",
        "report-Scheduled-alert-report-2026-04-08.json",
    )

    print(f"Lendo: {path}")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        # tenta encontrar a lista dentro do dict
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                data = v
                break

    if not isinstance(data, list):
        print("ERRO: JSON não é uma lista de objetos na raiz.")
        sys.exit(1)

    schemas, object_fields = analyze(data)

    stem = os.path.splitext(os.path.basename(path))[0]
    out_path = os.path.join(os.path.dirname(path), f"{stem}_subtables_summary.txt")
    _write_summary(schemas, out_path, object_prefixes=object_fields)


if __name__ == "__main__":
    main()
