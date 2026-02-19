from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from dagster import job, op, get_dagster_logger


RAW = Path("data/raw")


def get_conn():
    load_dotenv()
    user = os.getenv("POSTGRES_USER", "carms")
    password = os.getenv("POSTGRES_PASSWORD", "carms")
    db = os.getenv("POSTGRES_DB", "carms")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    host = os.getenv("POSTGRES_HOST", "localhost")
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)


@op
def etl_load_to_postgres():
    log = get_dagster_logger()

    d = pd.read_excel(RAW / "1503_discipline.xlsx")
    m = pd.read_excel(RAW / "1503_program_master.xlsx")
    x = pd.read_csv(RAW / "1503_program_descriptions_x_section.csv", low_memory=False)

    if "Unnamed: 0" in m.columns:
        m = m.drop(columns=["Unnamed: 0"])
    if "Unnamed: 0" in x.columns:
        x = x.drop(columns=["Unnamed: 0"])

    # join via URL
    merged = m.merge(x, left_on="program_url", right_on="source", how="inner")
    if len(merged) != 815:
        raise Exception(f"Join failed: expected 815, got {len(merged)}")

    schools = m[["school_id", "school_name"]].dropna().drop_duplicates()
    disciplines = d[["discipline_id", "discipline"]].dropna().drop_duplicates()

    program_streams = m[[
        "program_stream_id","discipline_id","school_id",
        "discipline_name","school_name",
        "program_stream_name","program_site","program_stream","program_name",
        "program_url",
    ]].copy()
    program_streams["match_iteration_id"] = 1503

    program_desc = merged[[
        "program_description_id","program_stream_id","source",
        "document_id","match_iteration_id","match_iteration_name",
        "program_name_y","n_program_description_sections",
    ]].copy().rename(columns={"source": "source_url", "program_name_y": "program_name"})

    meta_cols = {
        "document_id","source","n_program_description_sections",
        "program_name","match_iteration_name","match_iteration_id","program_description_id",
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
            if text:
                section_rows.append((int(pdid), c, text))

    log.info(f"Prepared: disciplines={len(disciplines)} schools={len(schools)} "
             f"program_streams={len(program_streams)} program_descriptions={len(program_desc)} "
             f"sections={len(section_rows)}")

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

            execute_values(cur,
                "INSERT INTO disciplines (discipline_id, discipline) VALUES %s "
                "ON CONFLICT (discipline_id) DO UPDATE SET discipline=EXCLUDED.discipline;",
                list(disciplines.itertuples(index=False, name=None))
            )

            execute_values(cur,
                "INSERT INTO schools (school_id, school_name) VALUES %s "
                "ON CONFLICT (school_id) DO UPDATE SET school_name=EXCLUDED.school_name;",
                list(schools.itertuples(index=False, name=None))
            )

            execute_values(cur,
                """
                INSERT INTO program_streams (
                    program_stream_id, discipline_id, school_id,
                    discipline_name, school_name,
                    program_stream_name, program_site, program_stream, program_name,
                    program_url, match_iteration_id
                ) VALUES %s
                ON CONFLICT (program_stream_id) DO UPDATE SET
                    discipline_id=EXCLUDED.discipline_id,
                    school_id=EXCLUDED.school_id,
                    discipline_name=EXCLUDED.discipline_name,
                    school_name=EXCLUDED.school_name,
                    program_stream_name=EXCLUDED.program_stream_name,
                    program_site=EXCLUDED.program_site,
                    program_stream=EXCLUDED.program_stream,
                    program_name=EXCLUDED.program_name,
                    program_url=EXCLUDED.program_url,
                    match_iteration_id=EXCLUDED.match_iteration_id;
                """,
                list(program_streams.itertuples(index=False, name=None))
            )

            execute_values(cur,
                """
                INSERT INTO program_descriptions (
                    program_description_id, program_stream_id, source_url,
                    document_id, match_iteration_id, match_iteration_name,
                    program_name, n_program_description_sections
                ) VALUES %s
                ON CONFLICT (program_description_id) DO UPDATE SET
                    program_stream_id=EXCLUDED.program_stream_id,
                    source_url=EXCLUDED.source_url,
                    document_id=EXCLUDED.document_id,
                    match_iteration_id=EXCLUDED.match_iteration_id,
                    match_iteration_name=EXCLUDED.match_iteration_name,
                    program_name=EXCLUDED.program_name,
                    n_program_description_sections=EXCLUDED.n_program_description_sections;
                """,
                list(program_desc.itertuples(index=False, name=None))
            )

            execute_values(cur,
                "INSERT INTO program_description_sections (program_description_id, section_name, section_text) VALUES %s;",
                section_rows,
                page_size=5000
            )

        conn.commit()
        log.info("✅ ETL finished and committed.")
    except Exception:
        conn.rollback()
        log.error("❌ ETL failed, rolled back.")
        raise
    finally:
        conn.close()


@job
def etl_job():
    etl_load_to_postgres()
