
# SPEAKFLAKE

**Conversational analytics built 100% inside Snowflake.**

SPEAKFLAKE turns natural language into SQL, insights, and what-if simulations — all without leaving Snowflake. It combines Cortex Analyst (text-to-SQL), Cortex COMPLETE (LLM narratives), Snowpark Python (stored procedures), and Streamlit in Snowflake (frontend) into a single conversational analytics platform.

---

## Architecture

┌──────────────────────────────────────────────────────────┐ │ Streamlit in Snowflake │ │ (SPEAKFLAKE_DB.APP.SPEAKFLAKE) │ └────────────────────────┬─────────────────────────────────┘ │ ┌──────────▼──────────┐ │ ORCHESTRATOR │ │ Intent Classifier │ │ (Cortex COMPLETE) │ └──┬───────┬───────┬──┘ │ │ │ ┌────────▼┐ ┌───▼───┐ ┌▼──────────┐ │SQL_QUERY│ │NARRAT.│ │SIMULATION │ │ Cortex │ │Cortex │ │Zero-copy │ │ Analyst │ │COMPLT.│ │Clone + LLM│ └────┬────┘ └───┬───┘ └─────┬─────┘ │ │ │ ┌───────▼───────────▼─────────────▼───────┐ │ SPEAKFLAKE_DB │ │ CORE │ MEMORY │ SIMULATION │ SEMANTIC │ └─────────────────────────────────────────┘



## Schemas

| Schema | Purpose |
|--------|---------|
| `CORE` | Sales data — facts, reps, regional targets |
| `MEMORY` | Conversation persistence — sessions, history, results, summaries, insight docs |
| `SIMULATION` | What-if engine — sim log + transient clones |
| `SEMANTIC` | Cortex Analyst semantic model (YAML on stage) |
| `APP` | Stored procedures, Streamlit stage, error log, benchmarks |

## Key Components

| Component | Type | Description |
|-----------|------|-------------|
| `SPEAKFLAKE_ORCHESTRATOR` | Stored Procedure | Main entry — classifies intent, routes to SQL/narrative/simulation |
| `WRITE_YAML_TO_STAGE` | Stored Procedure | Deploys semantic model YAML to `@YAML_STAGE` |
| `GENERATE_DEMO_DATA` | Stored Procedure | Seeds `CORE` tables with sample sales data |
| `RUN_SIMULATION` | Stored Procedure | Zero-copy clone engine for what-if scenarios |
| `GENERATE_INSIGHT_DOCUMENT` | Stored Procedure | Board-ready report generator |
| `SPEAKFLAKE` | Streamlit App | Frontend UI at `SPEAKFLAKE_DB.APP` |
| `semantic_model.yaml` | Semantic Model | Cortex Analyst context on `@YAML_STAGE` |

## Tech Stack

- **Query Engine**: Cortex Analyst
- **LLM Layer**: Cortex COMPLETE (llama3.1-70b / llama3.1-8b)
- **Compute**: Snowpark Python 3.11
- **Frontend**: Streamlit in Snowflake
- **Warehouse**: SPEAKFLAKE_WH (X-Small)

## Quick Start

Run the SQL files in order:

1. `sql/01_setup_database.sql` — Database, schemas, warehouse
2. `sql/02_tables.sql` — All tables and dynamic tables
3. `sql/03_file_formats.sql` — File formats for stage reads
4. `sql/04_stages.sql` — Internal stages
5. `sql/05_semantic_model.sql` — Deploy semantic YAML
6. `sql/06_stored_procedures.sql` — All stored procedures
7. `sql/07_seed_data.sql` — Populate demo data
8. `sql/08_streamlit_app.sql` — Deploy Streamlit app

See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for detailed instructions.

## Repository Structure

## License

MIT
