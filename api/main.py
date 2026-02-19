from fastapi import FastAPI, Query, HTTPException
from api.db import get_conn

app = FastAPI(title="CaRMS Mini Data Platform", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/disciplines")
def list_disciplines():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT discipline_id, discipline FROM disciplines ORDER BY discipline;")
            rows = cur.fetchall()
        return [{"discipline_id": r[0], "discipline": r[1]} for r in rows]
    finally:
        conn.close()


@app.get("/programs")
def list_programs(
    discipline_id: int | None = None,
    school_id: int | None = None,
    q: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    where = []
    params = []

    if discipline_id is not None:
        where.append("ps.discipline_id = %s")
        params.append(discipline_id)

    if school_id is not None:
        where.append("ps.school_id = %s")
        params.append(school_id)

    if q:
        where.append("(ps.program_name ILIKE %s OR ps.program_stream_name ILIKE %s OR ps.school_name ILIKE %s)")
        like = f"%{q}%"
        params.extend([like, like, like])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
    SELECT
      ps.program_stream_id, ps.program_name, ps.program_stream_name,
      ps.discipline_id, ps.discipline_name,
      ps.school_id, ps.school_name,
      ps.program_site, ps.program_url
    FROM program_streams ps
    {where_sql}
    ORDER BY ps.program_name
    LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [
            {
                "program_stream_id": r[0],
                "program_name": r[1],
                "program_stream_name": r[2],
                "discipline_id": r[3],
                "discipline_name": r[4],
                "school_id": r[5],
                "school_name": r[6],
                "program_site": r[7],
                "program_url": r[8],
            }
            for r in rows
        ]
    finally:
        conn.close()


@app.get("/programs/{program_stream_id}")
def program_detail(program_stream_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # master
            cur.execute(
                """
                SELECT program_stream_id, program_name, program_stream_name, program_site,
                       discipline_id, discipline_name, school_id, school_name, program_url, match_iteration_id
                FROM program_streams
                WHERE program_stream_id = %s;
                """,
                (program_stream_id,),
            )
            ps = cur.fetchone()
            if not ps:
                raise HTTPException(status_code=404, detail="program_stream_id not found")

            # description header
            cur.execute(
                """
                SELECT program_description_id, source_url, match_iteration_name, n_program_description_sections
                FROM program_descriptions
                WHERE program_stream_id = %s;
                """,
                (program_stream_id,),
            )
            pd = cur.fetchone()

            # sections
            sections = []
            if pd:
                program_description_id = pd[0]
                cur.execute(
                    """
                    SELECT section_name, section_text
                    FROM program_description_sections
                    WHERE program_description_id = %s
                    ORDER BY section_name;
                    """,
                    (program_description_id,),
                )
                rows = cur.fetchall()
                sections = [{"section_name": r[0], "section_text": r[1]} for r in rows]

        return {
            "program": {
                "program_stream_id": ps[0],
                "program_name": ps[1],
                "program_stream_name": ps[2],
                "program_site": ps[3],
                "discipline_id": ps[4],
                "discipline_name": ps[5],
                "school_id": ps[6],
                "school_name": ps[7],
                "program_url": ps[8],
                "match_iteration_id": ps[9],
            },
            "description": None
            if not pd
            else {
                "program_description_id": pd[0],
                "source_url": pd[1],
                "match_iteration_name": pd[2],
                "n_program_description_sections": pd[3],
            },
            "sections": sections,
        }
    finally:
        conn.close()


@app.get("/search")
def search(
    query: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  ps.program_stream_id,
                  ps.program_name,
                  ps.school_name,
                  ps.discipline_name,
                  s.section_name,
                  ts_rank_cd(to_tsvector('english', COALESCE(s.section_text, '')), q) AS rank,
                  ts_headline('english', s.section_text, q) AS snippet
                FROM program_description_sections s
                JOIN program_descriptions d ON d.program_description_id = s.program_description_id
                JOIN program_streams ps ON ps.program_stream_id = d.program_stream_id,
                     websearch_to_tsquery('english', %s) q
                WHERE to_tsvector('english', COALESCE(s.section_text, '')) @@ q
                ORDER BY rank DESC
                LIMIT %s OFFSET %s;
                """,
                (query, limit, offset),
            )
            rows = cur.fetchall()

        return [
            {
                "program_stream_id": r[0],
                "program_name": r[1],
                "school_name": r[2],
                "discipline_name": r[3],
                "section_name": r[4],
                "rank": float(r[5]) if r[5] is not None else 0.0,
                "snippet": r[6],
            }
            for r in rows
        ]
    finally:
        conn.close()
