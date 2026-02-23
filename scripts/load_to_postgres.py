from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


RAW = Path("data/raw")

import re

def _clean_text_series(s: pd.Series) -> pd.Series:
    # normalize weird spaces/BOM, then strip
    return (
        s.astype(str)
         .str.replace("\u00a0", " ", regex=False)  # NBSP
         .str.replace("\ufeff", "", regex=False)   # BOM
         .str.strip()
    )

def extract_program_stream_id(x: pd.DataFrame) -> tuple[pd.Series, str]:
    """
    Try to extract program_stream_id from document_id first (preferred),
    fallback to source URL if needed.
    Returns: (series_of_int_or_NA, method_name)
    """
    # 1) document_id (e.g. "1503-27447", "1503 - 27447", etc.)
    if "document_id" in x.columns:
        doc = _clean_text_series(x["document_id"])
        # robust patterns: last digits or last part after '-'
        m1 = doc.str.extract(r"(\d+)\s*$")[0]
        m2 = doc.str.extract(r".*-\s*(\d+)\s*$")[0]
        out = pd.to_numeric(m2.fillna(m1), errors="coerce").astype("Int64")
        if out.notna().sum() >= max(1, int(0.95 * len(x))):
            return out, "document_id"

    # 2) source URL (e.g. ".../1503/27447?programLanguage=en")
    if "source" in x.columns:
        src = _clean_text_series(x["source"])
        sid = src.str.extract(r"/\d+/(\d+)\b")[0]  # works for /1503/<id>
        out = pd.to_numeric(sid, errors="coerce").astype("Int64")
        if out.notna().sum() >= max(1, int(0.95 * len(x))):
            return out, "source_url"

    raise ValueError("Could not extract program_stream_id (no usable document_id or source).")

def df_rows(df: pd.DataFrame):
    # convert pandas dtypes / <NA> to plain python types + None
    df2 = df.astype(object).where(pd.notna(df), None)
    return list(df2.itertuples(index=False, name=None))


def get_conn():
    load_dotenv()  # reads .env
    user = os.getenv("POSTGRES_USER", "carms")
    password = os.getenv("POSTGRES_PASSWORD", "carms")
    db = os.getenv("POSTGRES_DB", "carms")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    host = os.getenv("POSTGRES_HOST", "localhost")  # host machine (WSL) uses localhost

    return psycopg2.connect(
        host=host, port=port, dbname=db, user=user, password=password
    )


def main():
    # ---- read files
    discipline_path = RAW / "1503_discipline.xlsx"
    master_path = RAW / "1503_program_master.xlsx"
    x_path = RAW / "1503_program_descriptions_x_section.csv"

    if not discipline_path.exists() or not master_path.exists() or not x_path.exists():
        raise FileNotFoundError("Put required files in data/raw (discipline xlsx, master xlsx, x_section csv).")

    d = pd.read_excel(discipline_path)
    m = pd.read_excel(master_path)
    x = pd.read_csv(x_path, low_memory=False)

    # ---- clean
    if "Unnamed: 0" in m.columns:
        m = m.drop(columns=["Unnamed: 0"])
    if "Unnamed: 0" in x.columns:
        x = x.drop(columns=["Unnamed: 0"])

    # Ensure ids are numeric where expected
    for col in ["discipline_id", "school_id", "program_stream_id"]:
        if col in m.columns:
            m[col] = pd.to_numeric(m[col], errors="coerce").astype("Int64")

    for col in ["match_iteration_id", "program_description_id", "n_program_description_sections"]:
        if col in x.columns:
            x[col] = pd.to_numeric(x[col], errors="coerce").astype("Int64")

    # ---- robust join master + x_section via extracted program_stream_id
    expected = len(m)

    x = x.copy()
    x["program_stream_id_extracted"], method = extract_program_stream_id(x)

    bad = int(x["program_stream_id_extracted"].isna().sum())
    if bad:
        print(f"❌ {method}: failed to extract program_stream_id in {bad} rows out of {len(x)}")
        # show a few examples
        cols = [c for c in ["document_id", "source"] if c in x.columns]
        print(x.loc[x["program_stream_id_extracted"].isna(), cols].head(10).to_string(index=False))
        raise SystemExit(1)

    # uniqueness check (important for stable join)
    uniq = int(x["program_stream_id_extracted"].nunique(dropna=True))
    if uniq != expected:
        print(f"❌ Extracted ids are not unique/complete: unique={uniq}, expected={expected}")
        dupes = x[x.duplicated("program_stream_id_extracted", keep=False)][["program_stream_id_extracted"]].head(20)
        print("Sample duplicates:\n", dupes.to_string(index=False))
        raise SystemExit(1)

    # merge on ID (clean, stable)
    merged = m.merge(
        x,
        left_on="program_stream_id",
        right_on="program_stream_id_extracted",
        how="inner",
        suffixes=("_m", "_x"),
    )

    if len(merged) != expected:
        print("❌ join did not return expected rows.")
        print("expected:", expected, "merged:", len(merged))
        # show missing ids if any
        m_ids = set(pd.to_numeric(m["program_stream_id"], errors="coerce").dropna().astype(int).tolist())
        x_ids = set(x["program_stream_id_extracted"].dropna().astype(int).tolist())
        print("missing_in_x:", len(m_ids - x_ids), "extra_in_x:", len(x_ids - m_ids))
        raise SystemExit(1)

    print(f"✅ Joined {len(merged)}/{expected} using method: {method}")

    # ---- derive schools table
    schools = m[["school_id", "school_name"]].dropna().drop_duplicates()
    disciplines = d[["discipline_id", "discipline"]].dropna().drop_duplicates()

    # ---- program_streams rows
    program_streams = m[[
        "program_stream_id",
        "discipline_id",
        "school_id",
        "discipline_name",
        "school_name",
        "program_stream_name",
        "program_site",
        "program_stream",
        "program_name",
        "program_url",
    ]].copy()
    program_streams["match_iteration_id"] = 1503

    # ---- program_descriptions rows (from merged/x_section)
    program_desc = merged[[
        "program_description_id",
        "program_stream_id",
        "source",
        "document_id",
        "match_iteration_id",
        "match_iteration_name",
        "program_name_x",
        "n_program_description_sections",
    ]].copy()

    program_desc = program_desc.rename(columns={
        "source": "source_url",
        "program_name_x": "program_name",
    })

    # ---- sections (normalized)
    meta_cols = {
        "document_id",
        "source",
        "n_program_description_sections",
        "program_name",
        "match_iteration_name",
        "match_iteration_id",
        "program_description_id",
        "program_stream_id_extracted",
    }
    section_cols = [c for c in x.columns if c not in meta_cols]

    section_rows = []
    for _, row in x.iterrows():
        pdid = row.get("program_description_id")
        if pd.isna(pdid):
            continue
        for c in section_cols:
            val = row.get(c)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            text = str(val).strip()
            if not text:
                continue
            section_rows.append((int(pdid), c, text))

    print("Prepared rows:")
    print(" - disciplines:", len(disciplines))
    print(" - schools:", len(schools))
    print(" - program_streams:", len(program_streams))
    print(" - program_descriptions:", len(program_desc))
    print(" - sections:", len(section_rows))

    # ---- load into postgres
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                TRUNCATE program_description_sections,
                         program_descriptions,
                         program_streams,
                         schools,
                         disciplines
                RESTART IDENTITY CASCADE;
            """)

            execute_values(
                cur,
                """
                INSERT INTO disciplines (discipline_id, discipline)
                VALUES %s
                ON CONFLICT (discipline_id) DO UPDATE SET discipline = EXCLUDED.discipline;
                """,
                df_rows(disciplines)
            )

            execute_values(
                cur,
                """
                INSERT INTO schools (school_id, school_name)
                VALUES %s
                ON CONFLICT (school_id) DO UPDATE SET school_name = EXCLUDED.school_name;
                """,
                df_rows(schools)
            )

            execute_values(
                cur,
                """
                INSERT INTO program_streams (
                    program_stream_id, discipline_id, school_id,
                    discipline_name, school_name,
                    program_stream_name, program_site, program_stream, program_name,
                    program_url, match_iteration_id
                )
                VALUES %s
                ON CONFLICT (program_stream_id) DO UPDATE SET
                    discipline_id = EXCLUDED.discipline_id,
                    school_id = EXCLUDED.school_id,
                    discipline_name = EXCLUDED.discipline_name,
                    school_name = EXCLUDED.school_name,
                    program_stream_name = EXCLUDED.program_stream_name,
                    program_site = EXCLUDED.program_site,
                    program_stream = EXCLUDED.program_stream,
                    program_name = EXCLUDED.program_name,
                    program_url = EXCLUDED.program_url,
                    match_iteration_id = EXCLUDED.match_iteration_id;
                """,
                df_rows(program_streams)
            )

            execute_values(
                cur,
                """
                INSERT INTO program_descriptions (
                    program_description_id, program_stream_id, source_url,
                    document_id, match_iteration_id, match_iteration_name,
                    program_name, n_program_description_sections
                )
                VALUES %s
                ON CONFLICT (program_description_id) DO UPDATE SET
                    program_stream_id = EXCLUDED.program_stream_id,
                    source_url = EXCLUDED.source_url,
                    document_id = EXCLUDED.document_id,
                    match_iteration_id = EXCLUDED.match_iteration_id,
                    match_iteration_name = EXCLUDED.match_iteration_name,
                    program_name = EXCLUDED.program_name,
                    n_program_description_sections = EXCLUDED.n_program_description_sections;
                """,
                df_rows(program_desc)
            )

            execute_values(
                cur,
                """
                INSERT INTO program_description_sections (program_description_id, section_name, section_text)
                VALUES %s;
                """,
                section_rows,
                page_size=5000
            )

        conn.commit()
        print("\n✅ Load completed successfully.")
    except Exception:
        conn.rollback()
        print("\n❌ Load failed. Rolled back.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
