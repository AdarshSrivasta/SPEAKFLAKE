-- ============================================================
-- SPEAKFLAKE: Semantic Model Deployment
-- ============================================================
-- This procedure creates and uploads the semantic_model.yaml
-- to @SPEAKFLAKE_DB.SEMANTIC.YAML_STAGE
-- ============================================================

CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.SEMANTIC.WRITE_YAML_TO_STAGE()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import os, tempfile

def run(session):
    yaml_content = r'''name: speakflake_semantic_model
description: >
  Business context for SPEAKFLAKE conversational analytics.
  Covers sales transactions, regional targets, and rep directory
  across North America, APAC, EMEA, and LATAM.

custom_instructions: >
  When a user asks about "target vs actual" or "attainment", use the REGION_TARGETS table.
  When filtering by fiscal year, use the FISCAL_YEAR column with integer values (e.g., 2024 not 'FY2024').
  "Last year" means FISCAL_YEAR = 2023. "This year" or "current year" means FISCAL_YEAR = 2024.
  Always include attainment percentage (actual/target * 100) in target vs actual queries.
  When asked "which region missed target", filter WHERE ACTUAL_VALUE < TARGET_VALUE.
  Revenue questions without target context should use SALES_FACT, not REGION_TARGETS.
  Default to FY2024 when no fiscal year is specified for target/attainment questions.

tables:
  - name: SALES_FACT
    base_table:
      database: SPEAKFLAKE_DB
      schema: CORE
      table: SALES_FACT
    description: >
      Primary sales transaction table. Each row is one deal with region,
      product, rep, value, status, and close date. Spans FY2024-FY2025.
    primary_key:
      columns:
        - SALE_ID

    filters:
      - name: won_deals
        synonyms:
          - "closed deals"
          - "closed won"
          - "active deals"
        description: "Only deals with status Closed Won"
        expr: "DEAL_STATUS = 'Closed Won'"

      - name: lost_deals
        synonyms:
          - "closed lost"
          - "lost opportunities"
        description: "Only deals with status Closed Lost"
        expr: "DEAL_STATUS = 'Closed Lost'"

      - name: pipeline_deals
        synonyms:
          - "open deals"
          - "in pipeline"
          - "pending"
        description: "Deals still in pipeline (not closed)"
        expr: "DEAL_STATUS = 'Pipeline'"

    dimensions:
      - name: region
        synonyms:
          - "territory"
          - "geo"
          - "market"
        description: "Sales region. Values: North America, APAC, EMEA, LATAM"
        expr: REGION
        data_type: TEXT
        is_enum: true
        sample_values:
          - "North America"
          - "APAC"
          - "EMEA"
          - "LATAM"

      - name: country
        synonyms:
          - "nation"
          - "market"
        description: "Country where the deal was closed"
        expr: COUNTRY
        data_type: TEXT

      - name: sales_rep
        synonyms:
          - "rep"
          - "AE"
          - "account executive"
          - "seller"
        description: "Name of the sales representative who owns the deal"
        expr: SALES_REP
        data_type: TEXT

      - name: product
        synonyms:
          - "product name"
          - "SKU"
          - "offering"
        description: "Product sold. Values: DataVault Pro, InsightEngine, CloudSync"
        expr: PRODUCT
        data_type: TEXT
        is_enum: true
        sample_values:
          - "DataVault Pro"
          - "InsightEngine"
          - "CloudSync"

      - name: category
        synonyms:
          - "product category"
          - "product type"
          - "line of business"
        description: "Product category. Values: Platform, Analytics, Integration"
        expr: CATEGORY
        data_type: TEXT
        is_enum: true
        sample_values:
          - "Platform"
          - "Analytics"
          - "Integration"

      - name: deal_status
        synonyms:
          - "status"
          - "stage"
          - "deal stage"
          - "opportunity status"
        description: "Current deal status. Values: Closed Won, Closed Lost, Pipeline"
        expr: DEAL_STATUS
        data_type: TEXT
        is_enum: true
        sample_values:
          - "Closed Won"
          - "Closed Lost"
          - "Pipeline"

      - name: quarter
        synonyms:
          - "Q"
          - "fiscal quarter"
        description: "Fiscal quarter. Values: Q1, Q2, Q3, Q4"
        expr: QUARTER
        data_type: TEXT
        is_enum: true
        sample_values:
          - "Q1"
          - "Q2"
          - "Q3"
          - "Q4"

      - name: fiscal_year
        synonyms:
          - "FY"
          - "year"
          - "fiscal year"
        description: "Fiscal year as integer (e.g. 2024)"
        expr: FISCAL_YEAR
        data_type: NUMBER
        sample_values:
          - "2023"
          - "2024"

    time_dimensions:
      - name: close_date
        synonyms:
          - "closed on"
          - "when"
          - "deal date"
        description: "The date the deal was closed or is expected to close"
        expr: CLOSE_DATE
        data_type: DATE

    measures:
      - name: total_revenue
        synonyms:
          - "revenue"
          - "sales"
          - "bookings"
          - "ARR"
          - "total deal value"
        description: "Sum of all deal values. Primary revenue metric."
        expr: SUM(DEAL_VALUE)
        data_type: NUMBER
        default_aggregation: sum

      - name: deal_count
        synonyms:
          - "number of deals"
          - "count"
          - "volume"
          - "deal volume"
        description: "Count of deals"
        expr: COUNT(SALE_ID)
        data_type: NUMBER
        default_aggregation: count

      - name: average_deal_size
        synonyms:
          - "ADS"
          - "avg deal"
          - "deal size"
          - "average deal value"
        description: "Average value per deal"
        expr: AVG(DEAL_VALUE)
        data_type: NUMBER
        default_aggregation: avg

  - name: REGION_TARGETS
    base_table:
      database: SPEAKFLAKE_DB
      schema: CORE
      table: REGION_TARGETS
    description: >
      Quarterly revenue targets and actuals by region. Use this table for
      target vs actual, attainment, and quota analysis. One row per region per quarter.

    dimensions:
      - name: region
        synonyms:
          - "territory"
          - "geo"
        description: "Sales region"
        expr: REGION
        data_type: TEXT
        is_enum: true
        sample_values:
          - "North America"
          - "APAC"
          - "EMEA"
          - "LATAM"

      - name: quarter
        synonyms:
          - "Q"
          - "fiscal quarter"
        description: "Fiscal quarter"
        expr: QUARTER
        data_type: TEXT
        is_enum: true
        sample_values:
          - "Q1"
          - "Q2"
          - "Q3"
          - "Q4"

      - name: fiscal_year
        synonyms:
          - "FY"
          - "year"
        description: "Fiscal year as integer"
        expr: FISCAL_YEAR
        data_type: NUMBER
        sample_values:
          - "2023"
          - "2024"

    measures:
      - name: target_value
        synonyms:
          - "target"
          - "quota"
          - "goal"
          - "plan"
        description: "Revenue target for the region/quarter"
        expr: SUM(TARGET_VALUE)
        data_type: NUMBER
        default_aggregation: sum

      - name: actual_value
        synonyms:
          - "actual"
          - "actuals"
          - "achieved"
          - "actual revenue"
        description: "Actual revenue achieved for the region/quarter"
        expr: SUM(ACTUAL_VALUE)
        data_type: NUMBER
        default_aggregation: sum

  - name: SALES_REPS
    base_table:
      database: SPEAKFLAKE_DB
      schema: CORE
      table: SALES_REPS
    description: >
      Sales representative roster. Contains rep name, region, team,
      hire date, and active status. Used to join with SALES_FACT on rep name.
    primary_key:
      columns:
        - REP_ID

    dimensions:
      - name: rep_name
        synonyms:
          - "name"
          - "sales rep"
          - "AE"
          - "account executive"
        description: "Full name of the sales representative"
        expr: REP_NAME
        data_type: TEXT
        unique: true

      - name: region
        synonyms:
          - "territory"
          - "geo"
        description: "Region the rep is assigned to"
        expr: REGION
        data_type: TEXT
        is_enum: true
        sample_values:
          - "North America"
          - "APAC"
          - "EMEA"
          - "LATAM"

      - name: team
        synonyms:
          - "segment"
          - "team type"
          - "sales team"
        description: "Team assignment. Values: Enterprise, Mid-Market"
        expr: TEAM
        data_type: TEXT
        is_enum: true
        sample_values:
          - "Enterprise"
          - "Mid-Market"

      - name: is_active
        synonyms:
          - "active"
          - "current"
          - "employed"
        description: "Whether the rep is currently active"
        expr: IS_ACTIVE
        data_type: BOOLEAN

    time_dimensions:
      - name: hire_date
        synonyms:
          - "start date"
          - "joined"
          - "tenure start"
        description: "Date the rep was hired"
        expr: HIRE_DATE
        data_type: DATE
'''

    tmp_dir = tempfile.mkdtemp()
    local_path = os.path.join(tmp_dir, "semantic_model.yaml")
    with open(local_path, "w") as f:
        f.write(yaml_content)

    session.file.put(
        local_path,
        "@SPEAKFLAKE_DB.SEMANTIC.YAML_STAGE",
        auto_compress=False,
        overwrite=True,
    )
    os.remove(local_path)
    return "OK: semantic_model.yaml uploaded to @SPEAKFLAKE_DB.SEMANTIC.YAML_STAGE"
$$;

-- Deploy it:
CALL SPEAKFLAKE_DB.SEMANTIC.WRITE_YAML_TO_STAGE();
