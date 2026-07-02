#!/usr/bin/env python3
"""Aggregate EAR by use-case category using the LLM classification in clf_output.json."""
import csv
import json
import textwrap
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).parent
CSV = HERE / "use-cases.csv"
CLF = HERE / "clf_output.json"
REPORT = HERE / "ear-by-category.txt"

# Opportunity Id 006UY00000M0hgDYAR is reused for two unrelated rows; the id-keyed
# classification only resolves the first (SoFi). Disambiguate the second by opp name.
OVERRIDES = {("006UY00000M0hgDYAR", "Bitdefender - Alert Processing"): "Other"}

INDENT = 49  # detail column: 2 + $ + 15-wide amount + 2 + 28-wide account + 1
DETAIL_WIDTH = 90


def detail_text(r: dict) -> str:
    parts = [r["Opp Name"].strip()]
    if r["Rationale"].strip():
        parts.append(r["Rationale"].strip())
    if r["Evidence"].strip():
        parts.append("Evidence: " + r["Evidence"].strip())
    if r["Pass 2 Notes"].strip():
        parts.append("Pass 2: " + r["Pass 2 Notes"].strip())
    return " — ".join(parts)


def main() -> None:
    categories = json.loads(CLF.read_text())
    rows = list(csv.DictReader(CSV.open()))

    missing = [r["Opportunity Id"] for r in rows if r["Opportunity Id"] not in categories]
    if missing:
        raise SystemExit(f"{len(missing)} rows lack a classification, e.g. {missing[:3]}")

    ear_by_cat: dict[str, float] = defaultdict(float)
    workloads_by_cat: dict[str, list[tuple[str, str, float]]] = defaultdict(list)
    for r in rows:
        cat = OVERRIDES.get((r["Opportunity Id"], r["Opp Name"]), categories[r["Opportunity Id"]])
        ear = float(r["EAR"] or 0)
        ear_by_cat[cat] += ear
        workloads_by_cat[cat].append((r["Account"], detail_text(r), ear))

    out: list[str] = []
    grand_total = sum(ear_by_cat.values())
    for cat in sorted(ear_by_cat, key=ear_by_cat.get, reverse=True):
        total = ear_by_cat[cat]
        n = len(workloads_by_cat[cat])
        out.append("=" * 92)
        out.append(cat)
        out.append(f"  total EAR: ${total:,.0f}   ({total/grand_total*100:.1f}% of all EAR, {n} workloads)")
        out.append("  " + "-" * 88)
        top = sorted(workloads_by_cat[cat], key=lambda x: x[2], reverse=True)[:10]
        for acct, detail, ear in top:
            lines = textwrap.wrap(detail, width=DETAIL_WIDTH) or [""]
            out.append(f"  ${ear:>15,.0f}  {acct[:28]:<28} {lines[0]}")
            out.extend(" " * INDENT + line for line in lines[1:])
        out.append("")

    out.append("=" * 92)
    out.append(f"GRAND TOTAL EAR: ${grand_total:,.0f} across {len(rows)} workloads, {len(ear_by_cat)} categories")

    report = "\n".join(out)
    print(report)
    REPORT.write_text(report + "\n")
    print(f"\nwrote {REPORT}")


if __name__ == "__main__":
    main()
