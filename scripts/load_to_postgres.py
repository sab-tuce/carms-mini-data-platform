from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


RAW = Path("data/raw")

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

    # ---- join master + x_section via URL
    # master.program_url == x_section.source
    merged = m.merge(
        x,
        left_on="program_url",
        right_on="source",
        how="inner",
        suffixes=("_m", "_x")
    )

    if len(merged) != 815:
        print("❌ join did not return 815 rows.")
        print("merged rows:", len(merged))
        raise SystemExit(1)

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
