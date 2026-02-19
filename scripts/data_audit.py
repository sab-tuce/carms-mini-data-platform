from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd


RAW = Path("data/raw")
STAGED = Path("data/staged")
DOCS = Path("docs")
STAGED.mkdir(parents=True, exist_ok=True)
DOCS.mkdir(parents=True, exist_ok=True)


def find_x_section_csv() -> Path:
    # 1) If a CSV already exists in raw, use it
    csvs = list(RAW.glob("*.csv"))
    for p in csvs:
        if "section" in p.name.lower() or "x_section" in p.name.lower():
            return p

    # 2) Otherwise try to extract from zip
    zips = list(RAW.glob("*.zip"))
    for z in zips:
        if "x_section" in z.name.lower() or "section" in z.name.lower():
            with zipfile.ZipFile(z, "r") as zf:
                members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
                if not members:
                    raise FileNotFoundError(f"No CSV found inside {z}")
                # take the first csv
                member = members[0]
                out_path = STAGED / Path(member).name
                zf.extract(member, STAGED)
                extracted = STAGED / member
                # move/rename to flat path if needed
                if extracted != out_path:
                    out_path.write_bytes(extracted.read_bytes())
                return out_path

    raise FileNotFoundError(
        "Could not find x_section CSV. Put 1503_program_descriptions_x_section.zip (or its CSV) into data/raw/."
    )


def pct(n, d):
    return round((n / d) * 100, 2) if d else 0.0


def main():
    need = ["1503_discipline.xlsx", "1503_program_master.xlsx"]
    missing = [f for f in need if not (RAW / f).exists()]
    if missing:
        print("❌ Missing files in data/raw/:")
        for m in missing:
            print("  -", m)
        return

    discipline_path = RAW / "1503_discipline.xlsx"
    master_path = RAW / "1503_program_master.xlsx"
    x_section_path = find_x_section_csv()

    d = pd.read_excel(discipline_path)
    m = pd.read_excel(master_path)
    x = pd.read_csv(x_section_path)

    print("\n=== SHAPES ===")
    print("discipline:", d.shape)
    print("program_master:", m.shape)
    print("x_section:", x.shape)

    print("\n=== COLUMNS (first 12) ===")
    print("discipline:", list(d.columns)[:12])
    print("program_master:", list(m.columns)[:12])
    print("x_section:", list(x.columns)[:12])

    # Clean hint
    if "Unnamed: 0" in m.columns:
        print("\n⚠️ program_master has 'Unnamed: 0' column (old Excel index). We'll drop it later.")

    # Key checks (best-effort based on available columns)
    # discipline_id
    if "discipline_id" in d.columns:
        du = d["discipline_id"].nunique(dropna=True)
        print("\n=== KEY CHECKS ===")
        print(f"discipline_id unique: {du} / {len(d)} (dupes: {len(d)-du})")
    else:
        print("\n⚠️ discipline.xlsx has no 'discipline_id' column? Please confirm column names.")

    # program_stream_id
    if "program_stream_id" in m.columns:
        mu = m["program_stream_id"].nunique(dropna=True)
        print(f"program_stream_id unique in master: {mu} / {len(m)} (dupes: {len(m)-mu})")
    else:
        print("\n⚠️ program_master has no 'program_stream_id' column? Please confirm column names.")

    # Join consistency between master and x_section
    join_key = None
    for cand in ["program_stream_id", "program_description_id"]:
        if cand in x.columns:
            join_key = cand
            break

    if join_key and join_key in m.columns:
        master_ids = set(m[join_key].dropna().astype(str))
        x_ids = set(x[join_key].dropna().astype(str))
        missing_in_x = len(master_ids - x_ids)
        extra_in_x = len(x_ids - master_ids)
        print(f"\nJoin key detected: {join_key}")
        print(f"IDs in master not in x_section: {missing_in_x}")
        print(f"IDs in x_section not in master: {extra_in_x}")
    else:
        print("\n⚠️ Couldn't do join-check (no shared key found). We'll inspect columns together.")

    # Null profile for x_section text columns
    id_like = {c for c in x.columns if c.lower().endswith("_id")} | {
        "program_name", "match_iteration_name", "program_site", "school_name", "discipline_name"
    }
    text_cols = [c for c in x.columns if c not in id_like and x[c].dtype == "object"]

    null_stats = []
    for c in text_cols:
        n_null = int(x[c].isna().sum())
        null_stats.append((c, n_null, pct(n_null, len(x))))

    null_stats.sort(key=lambda t: t[1], reverse=True)

    print("\n=== NULLS (Top 12 text cols) ===")
    for c, n, p in null_stats[:12]:
        print(f"{c:30} nulls={n:4d} ({p:5.2f}%)")

    # Save summary artifacts
    summary = {
        "rows": {
            "discipline": len(d),
            "program_master": len(m),
            "x_section": len(x),
        },
        "columns": {
            "discipline": list(d.columns),
            "program_master": list(m.columns),
            "x_section": list(x.columns),
        },
        "has_unnamed_0": "Unnamed: 0" in m.columns,
        "x_section_nulls_top12": null_stats[:12],
        "join_key_detected": join_key,
    }

    (STAGED / "data_profile.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    # Data dictionary (simple but valuable)
    md = []
    md.append("# Data Dictionary (Iteration 1503)\n")
    md.append("## Tables / Files\n")
    md.append(f"- **discipline**: {d.shape[0]} rows × {d.shape[1]} cols\n")
    md.append(f"- **program_master**: {m.shape[0]} rows × {m.shape[1]} cols\n")
    md.append(f"- **x_section**: {x.shape[0]} rows × {x.shape[1]} cols\n")

    md.append("\n## Columns\n")
    md.append("### discipline\n")
    for c in d.columns:
        md.append(f"- `{c}`\n")
    md.append("\n### program_master\n")
    for c in m.columns:
        md.append(f"- `{c}`\n")
    md.append("\n### x_section\n")
    for c in x.columns:
        md.append(f"- `{c}`\n")

    md.append("\n## Notes\n")
    if "Unnamed: 0" in m.columns:
        md.append("- `Unnamed: 0` is an old Excel index column → should be dropped.\n")
    md.append("- `data_profile.json` contains a machine-readable summary.\n")

    (DOCS / "data_dictionary.md").write_text("".join(md), encoding="utf-8")

    print("\n✅ Wrote:")
    print(" - data/staged/data_profile.json")
    print(" - docs/data_dictionary.md")


if __name__ == "__main__":
    main()
