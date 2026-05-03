"""
Analisa um JSON de alertas e identifica possíveis subtabelas relacionais.

Saída:
  - Resumo dos campos raiz (tipo, % preenchido, cardinalidade estimada)
  - Subtabelas sugeridas com campos e tipos inferidos
  - Avisos sobre campos com schemas variáveis (ex.: RiskFindings)

Estratégia para objetos com schema variável (ex.: RiskFindings):
  - Descoberta de chaves: scan COMPLETO (todos os registros)
  - Inferência de tipos:  amostra de até SAMPLE_SIZE itens por subtabela
"""

import json
import sys
import os
from collections import defaultdict, Counter
from typing import Any

SAMPLE_SIZE = 5_000   # máx de itens usados para inferência de tipos por subtabela


# ─── helpers ────────────────────────────────────────────────────────────────

def python_type(value: Any) -> str:
    if value is None:      return "null"
    if isinstance(value, bool):   return "boolean"
    if isinstance(value, int):    return "integer"
    if isinstance(value, float):  return "float"
    if isinstance(value, str):    return "string"
    if isinstance(value, list):   return "array"
    if isinstance(value, dict):   return "object"
    return type(value).__name__


def infer_column_type(types: Counter) -> str:
    non_null = {t: c for t, c in types.items() if t != "null"}
    if not non_null:
        return "null"
    return max(non_null, key=non_null.get)


def analyze_fields(records: list[dict]) -> dict:
    """Retorna estatísticas por campo para uma lista de dicts."""
    field_types: dict[str, Counter] = defaultdict(Counter)
    field_counts: Counter = Counter()
    total = len(records)

    for rec in records:
        for k, v in rec.items():
            field_types[k][python_type(v)] += 1
            if v is not None:
                field_counts[k] += 1

    return {
        col: {
            "inferred_type": infer_column_type(types),
            "type_dist": dict(types),
            "filled_pct": round(field_counts.get(col, 0) / total * 100, 1),
        }
        for col, types in field_types.items()
    }


def _resolve_case_conflicts(cols: list) -> list:
    """Campo com mesmo nome em case diferente: o que inicia com maiúscula recebe '_'."""
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


def _discover_object_array_keys(obj_records: list[dict]) -> dict[str, list[dict]]:
    """
    Scan COMPLETO de todos os registros para encontrar quais sub-chaves
    contêm arrays de objetos.

    Retorna: {chave_canônica → lista de até SAMPLE_SIZE itens}

    Deduplicação de case: se 'Ec2Instances' e 'EC2Instances' existem, são
    mescladas sob a chave com mais ocorrências (a mais comum vence).
    """
    # Mapa: lowercase → chave canônica (a mais frequente)
    canonical: dict[str, str] = {}       # lowercase → nome canônico
    key_freq:  Counter = Counter()        # nome exato → nº de registros
    discarded: set[str] = set()
    samples: dict[str, list[dict]] = {}  # keyed pelo nome canônico

    for rec in obj_records:
        for k, v in rec.items():
            lower = k.lower()
            if lower in discarded:
                continue
            if isinstance(v, list) and v and isinstance(v[0], dict):
                key_freq[k] += 1
                # Elege como canônico o nome com maior frequência
                if lower not in canonical or key_freq[k] > key_freq[canonical[lower]]:
                    old_canon = canonical.get(lower)
                    canonical[lower] = k
                    # Migra amostras do nome anterior para o novo canônico
                    if old_canon and old_canon != k:
                        samples[k] = samples.pop(old_canon, [])
                canon = canonical[lower]
                if canon not in samples:
                    samples[canon] = []
                if len(samples[canon]) < SAMPLE_SIZE:
                    samples[canon].extend(item for item in v if isinstance(item, dict))
            elif isinstance(v, list) and v and lower not in canonical:
                discarded.add(lower)

    return samples


# ─── tipos de colunas ────────────────────────────────────────────────────────

TableColumn = tuple[str, str, float]       # (nome, tipo, filled_pct)
TableSchema  = dict[str, list[TableColumn]]  # table_name → colunas


# ─── formatação ──────────────────────────────────────────────────────────────

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
            group = next((p for p in prefixes if name.lower().startswith(p)), None)
            if group and table == "alerts":
                if group in seen_groups:
                    continue
                seen_groups.add(group)
                sub = [(n, t, p) for n, t, p in cols if n.lower().startswith(group)]
                avg_pct = sum(p for _, _, p in sub) / len(sub) if sub else 0
                out.append(f"  │   {group.rstrip('_'):<33} object({len(sub)} campos)  avg {avg_pct:.0f}%")
            else:
                marker = " [FK]" if name == "alert_id" else ""
                sparse = "  ← sparse" if pct < 30 else ""
                out.append(f"  │   {name:<33} {typ:<12}{marker}{sparse}")
        return out

    lines = [
        f"\n{'='*70}",
        "  RESUMO — ESQUEMA RELACIONAL SUGERIDO",
        f"{'='*70}",
    ]
    for table, cols in schemas.items():
        lines.append(f"\n  ┌─ {table}")
        lines.extend(_col_lines(table, cols))
        lines.append(f"  └{'─'*55}")

    table_names = list(schemas.keys())
    lines += [
        f"\n{'='*70}",
        f"  TOTAL: {len(table_names)} tabela(s)",
        f"{'─'*70}",
    ]
    for i, name in enumerate(table_names, 1):
        lines.append(f"  {i:>2}. {name:<40} ({len(schemas[name])} colunas)")
    lines += [f"{'='*70}", ""]
    return lines


def _print_summary(schemas: TableSchema, object_prefixes: list[str] | None = None) -> None:
    for line in _build_summary_lines(schemas, object_prefixes):
        print(line)


def _write_summary(schemas: TableSchema, out_path: str, object_prefixes: list[str] | None = None) -> None:
    lines = _build_summary_lines(schemas, object_prefixes)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\n  Resumo salvo em: {out_path}")


# ─── análise principal ───────────────────────────────────────────────────────

def analyze(data: list[dict]) -> tuple[TableSchema, list[str]]:
    total = len(data)
    subtable_schemas: TableSchema = {}
    main_cols: list[TableColumn] = []

    print(f"\n{'='*70}")
    print(f"  RELATÓRIO DE SUBTABELAS POTENCIAIS")
    print(f"{'='*70}")
    print(f"  Total de registros (raiz): {total:,}\n")

    # ── 1. classificar campos raiz ────────────────────────────────────────────
    root_stats = analyze_fields(data)

    flat_fields:         list[str] = []
    scalar_array_fields: list[str] = []
    object_array_fields: list[str] = []
    object_fields:       list[str] = []

    for col, info in root_stats.items():
        t = info["inferred_type"]
        if t == "array":
            # verifica o tipo do primeiro elemento não-nulo
            is_obj = any(
                isinstance(item, dict)
                for row in data
                for item in (row.get(col) or [])
                if item is not None
            )
            (object_array_fields if is_obj else scalar_array_fields).append(col)
        elif t == "object":
            object_fields.append(col)
        else:
            flat_fields.append(col)

    # ── 2. campos escalares → tabela principal ────────────────────────────────
    for col in flat_fields:
        info = root_stats[col]
        main_cols.append((col, info["inferred_type"], info["filled_pct"]))

    for col in scalar_array_fields:
        info = root_stats[col]
        all_items = [i for row in data for i in (row.get(col) or [])]
        distinct = len(set(str(i) for i in all_items[:2_000]))
        main_cols.append((col, "array<string>", info["filled_pct"]))
        print(f"  [array<string> em alerts] {col:<28} {distinct} valores distintos")

    # ── 3. objetos aninhados → scan completo + flatten/subtabelas ────────────
    print(f"\n{'─'*70}")
    print("  [OBJETOS ANINHADOS → campos inline em alerts]")
    print(f"{'─'*70}")

    for col in object_fields:
        obj_records = [row[col] for row in data if isinstance(row.get(col), dict)]
        if not obj_records:
            continue

        prefix = col.lower() + "_"

        # Descoberta completa: scan de TODOS os registros
        obj_array_samples = _discover_object_array_keys(obj_records)

        # Estatísticas de campos escalares: amostra suficiente para tipo
        sub_stats = analyze_fields(obj_records[:SAMPLE_SIZE])

        unique_cols_count = sum(1 for _ in {k for rec in obj_records for k in rec})
        print(f"\n  {col}  ({len(obj_records):,} registros, {unique_cols_count} chaves distintas)")
        if unique_cols_count > 8:
            print(f"    ⚠  Schema variável — campos sparse esperados.")

        parent_id_key = _find_id_field(obj_records)

        for sub_col, si in sub_stats.items():
            t = si["inferred_type"]
            filled = si["filled_pct"]
            full_name = f"{prefix}{sub_col}"

            if t == "array":
                if sub_col in obj_array_samples:
                    # já tratado abaixo no loop de obj_array_samples
                    pass
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

        # Subtabelas descobertas via scan completo
        for sub_col, items in sorted(obj_array_samples.items()):
            table_name = f"alert_{col}_{sub_col}".lower()
            # Contagem real de alertas que têm esta chave
            alert_count = sum(
                1 for rec in obj_records if isinstance(rec.get(sub_col), list) and rec[sub_col]
            )
            print(f"    → SUBTABELA: {table_name}  ({alert_count} alertas)")

            if not items:
                print(f"       (sem itens para inferência de schema)")
                continue

            inner_stats = analyze_fields(items[:SAMPLE_SIZE])

            fk_cols: list[TableColumn] = [("alert_id", "string", 100.0)]
            if parent_id_key:
                parent_id_pct = sub_stats.get(parent_id_key, {}).get("filled_pct", 0.0)
                fk_cols.append((f"{col.lower()}_id", "string", parent_id_pct))

            sub_cols = _resolve_case_conflicts(fk_cols + [
                (sc, si2["inferred_type"], si2["filled_pct"])
                for sc, si2 in inner_stats.items()
            ])
            _print_columns(sub_cols)
            subtable_schemas[table_name] = sub_cols

    # ── 4. arrays de objetos na raiz → subtabelas 1:N ─────────────────────────
    print(f"\n{'─'*70}")
    print("  [ARRAYS DE OBJETOS → SUBTABELAS 1:N]")
    print(f"{'─'*70}")

    for col in object_array_fields:
        all_items = [
            item
            for row in data
            for item in (row.get(col) or [])
            if isinstance(item, dict)
        ]
        lengths = [len(row.get(col) or []) for row in data]
        avg_len = sum(lengths) / total if total else 0
        table_name = f"alert_{col.lower()}"

        print(f"\n  Coluna: {col}")
        print(f"    Média de itens/reg : {avg_len:.1f}  |  Máx: {max(lengths) if lengths else 0}")
        print(f"    → SUBTABELA: {table_name}")

        if all_items:
            sub_stats = analyze_fields(all_items[:SAMPLE_SIZE])
            unique_cols = {k for item in all_items[:SAMPLE_SIZE] for k in item}
            if len(unique_cols) > len(sub_stats) * 0.8 and len(unique_cols) > 5:
                print(f"    ⚠  Schema variável ({len(unique_cols)} chaves distintas)")
            sub_cols = _resolve_case_conflicts(
                [("alert_id", "string", 100.0)] + [
                    (sc, si["inferred_type"], si["filled_pct"])
                    for sc, si in sub_stats.items()
                ]
            )
            _print_columns(sub_cols)
            subtable_schemas[table_name] = sub_cols

    # ── 5. tabela principal ───────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  [TABELA PRINCIPAL]  alerts")
    print(f"{'─'*70}")
    _print_columns(main_cols, indent="  ")

    # ── 6. resumo ─────────────────────────────────────────────────────────────
    schemas: TableSchema = {"alerts": main_cols, **subtable_schemas}
    _print_summary(schemas, object_prefixes=object_fields)
    return schemas, object_fields


# ─── entry point ─────────────────────────────────────────────────────────────

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "json", "report-Scheduled-alert-report-2026-04-08.json",
    )

    print(f"Lendo: {path}")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
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
