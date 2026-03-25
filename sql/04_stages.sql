-- ============================================================
-- SPEAKFLAKE: Internal Stages
-- ============================================================

CREATE OR REPLACE STAGE SPEAKFLAKE_DB.APP.STREAMLIT_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Streamlit app source code for SPEAKFLAKE';

CREATE OR REPLACE STAGE SPEAKFLAKE_DB.SEMANTIC.YAML_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Stores semantic model YAML files for Cortex Analyst';
