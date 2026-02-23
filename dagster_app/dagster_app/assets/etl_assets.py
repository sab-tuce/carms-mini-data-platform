from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from dagster import (
    asset,
    MaterializeResult,
    MetadataValue,
    define_asset_job,
    AssetSelection,
    get_dagster_logger,
)

RAW = Path("data/raw")


# ---------- helpers ----------
def _clean_text_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.replace("\u00a0", " ", regex=False)  # NBSP
        .str.replace("\ufeff", "", regex=False)   # BOM
        .str.strip()
    )


def extract_program_stream_id(x: pd.DataFrame) -> tuple[pd.Series, str]:
    """
    Prefer extracting program_stream_id from document_id (e.g. "1503-27447"),
    fallback to source URL if needed.
    Returns: (series_of_Int64, method_name)
    """
    # 1) document_id
    if "document_id" in x.columns:
        doc = _clean_text_series(x["document_id"])
        m1 = doc.str.extract(r"(\d+)\s*$")[0]
        m2 = doc.str.extract(r".*-\s*(\d+)\s*$")[0]
        out = pd.to_numeric(m2.fillna(m1), errors="coerce").astype("Int64")
        if out.notna().sum() >= max(1, int(0.95 * len(x))):
            return out, "document_id"

    # 2) source URL fallback
    if "source" in x.columns:
        src = _clean_text_series(x["source"])
        sid = src.str.extract(r"/\d+/(\d+)\b")[0]  # /1503/<id>
        out = pd.to_numeric(sid, errors="coerce").astype("Int64")
        if out.notna().sum() >= max(1, int(0.95 * len(x))):
            return out, "source_url"

    raise ValueError("Could not extract program_stream_id from document_id or source.")


def get_conn():
    # works both locally (reads .env) and in docker (env vars are injected)
    load_dotenv()
    user = os.getenv("POSTGRES_USER", "carms")
    password = os.getenv("POSTGRES_PASSWORD", "carms")
    db = os.getenv("POSTGRES_DB", "carms")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    host = os.getenv("POSTGRES_HOST", "localhost")
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)


# ---------- assets ----------
@asset(group_name="etl", compute_kind="pandas")
def disciplines_df() -> pd.DataFrame:
    d = pd.read_excel(RAW / "1503_discipline.xlsx")
    return d


@asset(group_name="etl", compute_kind="pandas")
def program_master_df() -> pd.DataFrame:
    m = pd.read_excel(RAW / "1503_program_master.xlsx")
    if "Unnamed: 0" in m.columns:
        m = m.drop(columns=["Unnamed: 0"])
    return m


@asset(group_name="etl", compute_kind="pandas")
def x_section_df() -> pd.DataFrame:
    x = pd.read_csv(RAW / "1503_program_descriptions_x_section.csv", low_memory=False)
    if "Unnamed: 0" in x.columns:
        x = x.drop(columns=["Unnamed: 0"])
    return x


@asset(group_name="etl", compute_kind="pandas")
def x_section_with_id(x_section_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds program_stream_id_extracted based on document_id (preferred).
    """
    x = x_section_df.copy()
    x["program_stream_id_extracted"], method = extract_program_stream_id(x)

    bad = int(x["program_stream_id_extracted"].isna().sum())
    if bad:
        cols = [c for c in ["document_id", "source"] if c in x.columns]
        sample = x.loc[x["program_stream_id_extracted"].isna(), cols].head(10)
        raise Exception(f"{method}: failed to extract program_stream_id for {bad}/{len(x)} rows.\n{sample}")

    return x


@asset(group_name="etl", compute_kind="pandas")
def joined_master_xsection(program_master_df: pd.DataFrame, x_section_with_id: pd.DataFrame) -> pd.DataFrame:
    """
    Joins master and x_section via program_stream_id (stable identifier).
    """
    m = program_master_df
    x = x_section_with_id

    expected = len(m)
    uniq = int(x["program_stream_id_extracted"].nunique(dropna=True))
    if uniq != expected:
        raise Exception(f"Extracted ids not unique/complete: unique={uniq}, expected={expected}")

    merged = m.merge(
        x,
        left_on="program_stream_id",
        right_on="program_stream_id_extracted",
        how="inner",
        suffixes=("_m", "_x"),
    )

    if len(merged) != expected:
        raise Exception(f"Join failed: expected {expected}, got {len(merged)}")

    return merged


@asset(group_name="etl", compute_kind="python")
def prepared_payload(
    disciplines_df: pd.DataFrame,
    program_master_df: pd.DataFrame,
    x_section_with_id: pd.DataFrame,
    joined_master_xsection: pd.DataFrame,
) -> dict:
    """
    Prepare all objects needed for loading into Postgres.
    Returns a dict to keep downstream code simple.
    """
    d = disciplines_df
    m = program_master_df
    x = x_section_with_id
    merged = joined_master_xsection

    schools = m[["school_id", "school_name"]].dropna().drop_duplicates()
    disciplines = d[["discipline_id", "discipline"]].dropna().drop_duplicates()

    program_streams = m[
        [
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
        ]
    ].copy()
    program_streams["match_iteration_id"] = 1503

    # program_name can be suffixed after merge; choose robustly
    if "program_name_x" in merged.columns:
        program_name_col = "program_name_x"
    elif "program_name_m" in merged.columns:
        program_name_col = "program_name_m"
    else:
        program_name_col = "program_name"

    program_desc = merged[
        [
            "program_description_id",
            "program_stream_id",
            "source",
            "document_id",
            "match_iteration_id",
            "match_iteration_name",
            program_name_col,
            "n_program_description_sections",
        ]
    ].copy()

    program_desc = program_desc.rename(columns={"source": "source_url", program_name_col: "program_name"})

    # Sections (normalized) - IMPORTANT: exclude helper column
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
            if text:
                section_rows.append((int(pdid), c, text))

    return {
        "disciplines": disciplines,
        "schools": schools,
        "program_streams": program_streams,
        "program_desc": program_desc,
        "section_rows": section_rows,
    }


@asset(group_name="etl", compute_kind="postgres")
def load_postgres(prepared_payload: dict) -> MaterializeResult:
    """
    Loads all tables into Postgres (rerunnable: truncates then reloads).
    """
    log = get_dagster_logger()

    disciplines = prepared_payload["disciplines"]
    schools = prepared_payload["schools"]
    program_streams = prepared_payload["program_streams"]
    program_desc = prepared_payload["program_desc"]
    section_rows = prepared_payload["section_rows"]

    log.info(
        f"Prepared: disciplines={len(disciplines)} schools={len(schools)} "
        f"program_streams={len(program_streams)} program_descriptions={len(program_desc)} "
        f"sections={len(section_rows)}"
    )

    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE program_description_sections,
                         program_descriptions,
                         program_streams,
                         schools,
                         disciplines
                RESTART IDENTITY CASCADE;
                """
            )

            execute_values(
                cur,
                "INSERT INTO disciplines (discipline_id, discipline) VALUES %s "
                "ON CONFLICT (discipline_id) DO UPDATE SET discipline=EXCLUDED.discipline;",
                list(disciplines.itertuples(index=False, name=None)),
            )

            execute_values(
                cur,
                "INSERT INTO schools (school_id, school_name) VALUES %s "
                "ON CONFLICT (school_id) DO UPDATE SET school_name=EXCLUDED.school_name;",
                list(schools.itertuples(index=False, name=None)),
            )

            execute_values(
                cur,
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
                list(program_streams.itertuples(index=False, name=None)),
            )

            execute_values(
                cur,
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
                list(program_desc.itertuples(index=False, name=None)),
            )

            execute_values(
                cur,
                "INSERT INTO program_description_sections (program_description_id, section_name, section_text) VALUES %s;",
                section_rows,
                page_size=5000,
            )

        conn.commit()
        log.info("✅ load_postgres committed successfully.")
    except Exception:
        conn.rollback()
        log.error("❌ load_postgres failed, rolled back.")
        raise
    finally:
        conn.close()

    return MaterializeResult(
        metadata={
            "disciplines": MetadataValue.int(len(disciplines)),
            "schools": MetadataValue.int(len(schools)),
            "program_streams": MetadataValue.int(len(program_streams)),
            "program_descriptions": MetadataValue.int(len(program_desc)),
            "sections": MetadataValue.int(len(section_rows)),
        }
    )


# A job that materializes the whole ETL asset graph:
etl_assets_job = define_asset_job(
    name="etl_assets_job",
    selection=AssetSelection.groups("etl"),
)
