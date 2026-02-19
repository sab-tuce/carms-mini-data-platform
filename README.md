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

## Demo (2 minutes)
1) `docker compose up -d --build`
2) Open Dagster: http://localhost:3000 → run `etl_job`
3) Open API docs: http://localhost:8000/docs
4) Try:
   - `/programs?limit=3`
   - `/search?query=interview&limit=3`

