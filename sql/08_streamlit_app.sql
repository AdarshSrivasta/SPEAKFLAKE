-- ============================================================
-- SPEAKFLAKE: Deploy Streamlit App
-- ============================================================

-- Step 1: Upload environment.yml
CALL SPEAKFLAKE_DB.APP.DEPLOY_ENV_YML();

-- Step 2: Create the Streamlit app object
-- NOTE: Before running this, upload streamlit_app.py to @SPEAKFLAKE_DB.APP.STREAMLIT_STAGE
-- You can use ASSEMBLE_AND_DEPLOY_APP() or PUT the file manually.

CREATE OR REPLACE STREAMLIT SPEAKFLAKE_DB.APP.SPEAKFLAKE
  ROOT_LOCATION = '@SPEAKFLAKE_DB.APP.STREAMLIT_STAGE'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = 'SPEAKFLAKE_WH';
