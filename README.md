# CaRMS Mini Data Platform

A small, containerized **ETL + API** platform built with **PostgreSQL + Dagster + FastAPI**.  
It ingests CaRMS program description data (match iteration **1503**) and exposes query/search endpoints.

## Why this exists
This project demonstrates that I can:
- Stand up a reproducible data platform using Docker Compose
- Model structured + text-heavy data in PostgreSQL
- Orchestrate ETL with Dagster (logs, reruns, failure visibility)
- Serve internal-style APIs with FastAPI (Swagger UI)
- Implement basic PostgreSQL full-text search over extracted sections

## Architecture
Services (Docker Compose):
- **postgres** (DB) → `localhost:5432`
- **dagster_webserver** (UI) → `http://localhost:3000`
- **dagster_daemon** (runs scheduled/queued work)
- **api** (FastAPI) → `http://localhost:8000`

High level flow:
1) Place raw files in `data/raw/`
2) Start the stack (`docker compose up -d --build`)
3) Run Dagster job `etl_job` to load data into Postgres
4) Query/search via FastAPI endpoints

## Data inputs (not committed to git)
Put these files in `data/raw/`:
- `1503_discipline.xlsx`
- `1503_program_master.xlsx`
- `1503_program_descriptions_x_section.csv`

> Note: `data/raw/*` is ignored by git.

## Quickstart

### 1) Configure environment
```bash
cp .env.example .env
```

### 2) Start the platform
```bash
docker compose up -d --build
docker ps
```

Open:
- Dagster UI: http://localhost:3000
- API Docs (Swagger): http://localhost:8000/docs

### 3) Run ETL
In Dagster UI:
**Jobs → etl_job → Run**

Expected row counts after a successful run:
- `disciplines`: 37
- `schools`: ~398
- `program_streams`: 815
- `program_descriptions`: 815
- `program_description_sections`: ~9,000+

## Demo (2 minutes)
1) `docker compose up -d --build`
2) Open Dagster UI → run `etl_job`
3) Open API docs: http://localhost:8000/docs
4) Try:
   - `GET /programs?limit=3`
   - `GET /search?query=interview&limit=3`

## API (MVP)

### Health
```bash
curl -s http://localhost:8000/health
```

### Disciplines
```bash
curl -s http://localhost:8000/disciplines | head
```

### Programs (list + filters)
```bash
curl -s "http://localhost:8000/programs?limit=3"
curl -s "http://localhost:8000/programs?discipline_id=1&limit=5"
curl -s "http://localhost:8000/programs?q=Toronto&limit=5"
```

### Program detail (includes normalized sections)
```bash
curl -s "http://localhost:8000/programs/27447" | head
```

### Full-text search (PostgreSQL FTS)
```bash
curl -s "http://localhost:8000/search?query=interview&limit=3"

## Screenshots

### Dagster (ETL as assets)
![Dagster assets graph](docs/images/dagster-assets-graph.png)
![Dagster run success](docs/images/dagster-run-success.png)

### FastAPI (Swagger)
![Swagger endpoints](docs/images/swagger-endpoints.png)
![Search parameters](docs/images/swagger-search-params.png)
![Search response](docs/images/swagger-search-response.png)
```