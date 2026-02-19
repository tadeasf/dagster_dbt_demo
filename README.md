# Dagster + dbt ETL Pipeline Demo

An end-to-end ETL pipeline using **Dagster** (orchestration), **dbt** (SQL transformations), open-source REST APIs (data sources), and **PostgreSQL** (warehouse). Everything runs via `docker compose`.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     docker-compose                          │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐  │
│  │ postgres │  │user_code │  │webserver │  │  daemon   │  │
│  │ :5432    │  │ :4000    │  │ :3000    │  │           │  │
│  │          │  │ (gRPC)   │  │ (UI)     │  │(schedules)│  │
│  │ DBs:     │  │          │  │          │  │           │  │
│  │ -metadata│  │ dagster  │  │          │  │           │  │
│  │ -warehouse│ │ + dbt    │  │          │  │           │  │
│  └──────────┘  └──────────┘  └──────────┘  └───────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Data Sources

| Source | API | Records |
|--------|-----|---------|
| Users | [JSONPlaceholder](https://jsonplaceholder.typicode.com) `/users` | 10 |
| Posts | JSONPlaceholder `/posts` | 100 |
| Comments | JSONPlaceholder `/comments` | 500 |
| Countries | [REST Countries](https://restcountries.com) `/v3.1/all` | 250 |

## Asset DAG

```
Python ingestion:         dbt staging:           dbt marts:
  raw_users        →    stg_users        ─┬─→  mart_user_activity
  raw_posts        →    stg_posts        ─┼─→  mart_post_engagement
  raw_comments     →    stg_comments     ─┼─→  mart_regional_activity
  raw_countries    →    stg_countries    ─┘
```

**11 assets** total: 4 raw (Python) → 4 staging (dbt views) → 3 marts (dbt tables)

## Quick Start

```bash
# 1. Start all services
docker compose up -d

# 2. Open Dagster UI
open http://localhost:3000

# 3. Click "Materialize All" in the UI to run the full pipeline
```

## Project Structure

```
dagster_dbt_demo/
├── docker-compose.yml
├── .env / .env.example
├── pyproject.toml
├── docker/
│   ├── Dockerfile.dagster          # webserver + daemon image
│   ├── Dockerfile.user_code        # user code image (dagster + dbt)
│   ├── dagster.yaml                # Dagster instance config (PG storage)
│   ├── workspace.yaml              # points to user_code gRPC server
│   └── init-db.sh                  # creates warehouse DB + schemas
├── dagster_dbt_demo/               # Python package
│   ├── definitions.py              # top-level Definitions
│   ├── assets/
│   │   ├── ingestion.py            # 4 raw assets (API → PG)
│   │   └── dbt_assets.py           # @dbt_assets decorator
│   ├── resources/
│   │   ├── database.py             # WarehouseResource (SQLAlchemy)
│   │   └── dbt.py                  # DbtProject config
│   ├── jobs/
│   │   └── etl_job.py              # define_asset_job for full pipeline
│   ├── schedules/
│   │   └── daily_schedule.py       # daily 6 AM UTC cron
│   └── dbt_project/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── packages.yml            # dbt_utils
│       ├── macros/
│       │   └── generate_schema_name.sql
│       └── models/
│           ├── sources.yml
│           ├── staging/            # 4 view models
│           └── marts/              # 3 table models
└── tests/
    └── test_assets.py
```

## PostgreSQL Schemas

| Schema | Purpose | Materialization |
|--------|---------|-----------------|
| `raw` | Raw API data loaded by Python assets | `pandas.to_sql` |
| `staging` | Cleaned/renamed columns | dbt views |
| `marts` | Joined/aggregated analytics | dbt tables |

Connect directly:

```bash
docker compose exec postgres psql -U dagster -d warehouse
```

## Useful Commands

```bash
# Rebuild after code changes
docker compose build user_code && docker compose up -d

# View logs
docker compose logs -f user_code

# Query mart data
docker compose exec postgres psql -U dagster -d warehouse \
  -c "SELECT username, total_posts, avg_comments_per_post FROM marts.mart_user_activity;"

# Tear down (remove volumes too)
docker compose down -v
```
