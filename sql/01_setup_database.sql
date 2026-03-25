-- ============================================================
-- SPEAKFLAKE: Database, Schemas, and Warehouse Setup
-- ============================================================

CREATE DATABASE IF NOT EXISTS SPEAKFLAKE_DB;

CREATE SCHEMA IF NOT EXISTS SPEAKFLAKE_DB.CORE
  COMMENT = 'Main app tables — sales facts, reps, targets';

CREATE SCHEMA IF NOT EXISTS SPEAKFLAKE_DB.MEMORY
  COMMENT = 'Conversation and session persistence tables';

CREATE SCHEMA IF NOT EXISTS SPEAKFLAKE_DB.SIMULATION
  COMMENT = 'Transient clone tracking and what-if results';

CREATE SCHEMA IF NOT EXISTS SPEAKFLAKE_DB.SEMANTIC
  COMMENT = 'Semantic model support tables';

CREATE SCHEMA IF NOT EXISTS SPEAKFLAKE_DB.APP
  COMMENT = 'Streamlit app objects';

CREATE WAREHOUSE IF NOT EXISTS SPEAKFLAKE_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'SPEAKFLAKE compute warehouse — X-SMALL for dev/demo';
