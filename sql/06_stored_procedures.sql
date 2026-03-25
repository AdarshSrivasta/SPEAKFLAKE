-- ============================================================
-- SPEAKFLAKE: All Stored Procedures
-- ============================================================

-- ----- CREATE_SESSION -----
CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.CREATE_SESSION(USER_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import uuid

def run(session, USER_NAME: str) -> str:
    sid = str(uuid.uuid4())
    esc_sid = sid.replace("'", "''")
    esc_user = USER_NAME.replace("'", "''")
    session.sql(f"""
        INSERT INTO SPEAKFLAKE_DB.MEMORY.SESSIONS
        (SESSION_ID, USER_NAME, LAST_ACTIVE, STATUS, TITLE)
        VALUES ('{esc_sid}', '{esc_user}', CURRENT_TIMESTAMP(), 'ACTIVE', NULL)
    """).collect()
    return sid
$$;

-- ----- CLOSE_SESSION -----
CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.CLOSE_SESSION(SESSION_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import json

def run(session, SESSION_ID: str) -> str:
    sid = SESSION_ID.replace("'", "''")

    turn_count_row = session.sql(f"""
        SELECT COUNT(*) AS CNT
        FROM SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY
        WHERE SESSION_ID = '{sid}'
    """).collect()
    turn_count = int(turn_count_row[0]["CNT"]) if turn_count_row else 0

    if turn_count >= 3:
        try:
            session.sql(f"""
                CALL SPEAKFLAKE_DB.APP.GENERATE_INSIGHT_DOCUMENT('{sid}', NULL)
            """).collect()
        except Exception:
            pass

    session.sql(f"""
        UPDATE SPEAKFLAKE_DB.MEMORY.SESSIONS
        SET STATUS = 'CLOSED', LAST_ACTIVE = CURRENT_TIMESTAMP()
        WHERE SESSION_ID = '{sid}'
    """).collect()

    report_note = " (insight document auto-generated)" if turn_count >= 3 else ""
    return f"SESSION CLOSED: {SESSION_ID}{report_note}"
$$;

-- ----- GET_SESSION_HISTORY -----
CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.GET_SESSION_HISTORY(SESSION_ID VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import json

def run(session, SESSION_ID: str) -> dict:
    sid = SESSION_ID.replace("'", "''")

    turns_rows = session.sql(f"""
        SELECT TURN_NUMBER, ROLE, MESSAGE, INTENT_TYPE, CONFIDENCE,
               TO_VARCHAR(CREATED_AT, 'YYYY-MM-DD HH24:MI:SS') AS CREATED_AT
        FROM SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY
        WHERE SESSION_ID = '{sid}'
        ORDER BY TURN_NUMBER ASC
    """).collect()

    turns = [row.as_dict() for row in turns_rows]

    results_rows = session.sql(f"""
        SELECT RESULT_ID, TURN_NUMBER, QUESTION, SQL_GENERATED, RESULT_SUMMARY,
               TO_VARCHAR(CREATED_AT, 'YYYY-MM-DD HH24:MI:SS') AS CREATED_AT
        FROM SPEAKFLAKE_DB.MEMORY.SESSION_RESULTS
        WHERE SESSION_ID = '{sid}'
        ORDER BY TURN_NUMBER ASC
    """).collect()

    results = [row.as_dict() for row in results_rows]

    return {"session_id": SESSION_ID, "turns": turns, "results": results}
$$;

-- ----- LIST_USER_SESSIONS -----
CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.LIST_USER_SESSIONS(USER_NAME VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
def run(session, USER_NAME: str) -> list:
    esc_user = USER_NAME.replace("'", "''")
    rows = session.sql(f"""
        SELECT
            s.SESSION_ID,
            s.USER_NAME,
            s.STATUS,
            s.TITLE,
            TO_VARCHAR(s.CREATED_AT, 'YYYY-MM-DD HH24:MI:SS') AS CREATED_AT,
            TO_VARCHAR(s.LAST_ACTIVE, 'YYYY-MM-DD HH24:MI:SS') AS LAST_ACTIVE,
            COALESCE(c.TURN_COUNT, 0) AS TURN_COUNT,
            sm.FIRST_QUESTION
        FROM SPEAKFLAKE_DB.MEMORY.SESSIONS s
        LEFT JOIN (
            SELECT SESSION_ID, COUNT(*) AS TURN_COUNT
            FROM SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY
            GROUP BY SESSION_ID
        ) c ON s.SESSION_ID = c.SESSION_ID
        LEFT JOIN SPEAKFLAKE_DB.MEMORY.SESSION_SUMMARY sm
            ON s.SESSION_ID = sm.SESSION_ID
        WHERE s.USER_NAME = '{esc_user}'
        ORDER BY s.LAST_ACTIVE DESC
    """).collect()
    return [row.as_dict() for row in rows]
$$;

-- ----- GENERATE_INSIGHT_DOCUMENT -----
CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.GENERATE_INSIGHT_DOCUMENT(SESSION_ID VARCHAR, REPORT_TITLE VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import json

def _escape(s):
    if s is None:
        return ""
    return str(s).replace("'", "''")

def _complete(session, model, system_prompt, user_prompt):
    sys_esc = _escape(system_prompt)
    usr_esc = _escape(user_prompt)
    row = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            [
                {{'role':'system','content':'{sys_esc}'}},
                {{'role':'user','content':'{usr_esc}'}}
            ],
            {{}}
        ) AS R
    """).collect()[0]["R"]
    parsed = json.loads(row)
    return parsed["choices"][0]["messages"]

def _complete_simple(session, model, prompt):
    esc = _escape(prompt)
    return session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{esc}') AS R").collect()[0]["R"]

def main(session, SESSION_ID: str, REPORT_TITLE: str) -> dict:
    try:
        sid = _escape(SESSION_ID)

        meta_rows = session.sql(f"""
            SELECT USER_NAME,
                   TO_VARCHAR(CREATED_AT, 'YYYY-MM-DD HH24:MI') AS CREATED,
                   TO_VARCHAR(LAST_ACTIVE, 'YYYY-MM-DD HH24:MI') AS LAST_ACTIVE,
                   STATUS
            FROM SPEAKFLAKE_DB.MEMORY.SESSIONS
            WHERE SESSION_ID = '{sid}'
        """).collect()
        if not meta_rows:
            return {"type": "error", "message": "Session not found"}

        meta = meta_rows[0]
        user_name = meta["USER_NAME"]
        created = meta["CREATED"]
        last_active = meta["LAST_ACTIVE"]

        history_rows = session.sql(f"""
            SELECT ROLE, MESSAGE, INTENT_TYPE
            FROM SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY
            WHERE SESSION_ID = '{sid}'
            ORDER BY TURN_NUMBER ASC
        """).collect()

        user_questions = [r["MESSAGE"] for r in history_rows if r["ROLE"] == "USER"]
        narrative_insights = [r["MESSAGE"] for r in history_rows if r["ROLE"] == "ASSISTANT" and r["INTENT_TYPE"] == "NARRATIVE"]

        result_rows = session.sql(f"""
            SELECT QUESTION, SQL_GENERATED, RESULT_SUMMARY
            FROM SPEAKFLAKE_DB.MEMORY.SESSION_RESULTS
            WHERE SESSION_ID = '{sid}'
            ORDER BY TURN_NUMBER ASC
        """).collect()

        sim_rows = session.sql(f"""
            SELECT SCENARIO_DESC, BASELINE_JSON, SIM_RESULT_JSON
            FROM SPEAKFLAKE_DB.SIMULATION.SIM_LOG
            WHERE SESSION_ID = '{sid}' AND STATUS IN ('COMPLETE','CLEANED')
            ORDER BY CREATED_AT
        """).collect()

        duration_mins = 0
        try:
            ts1 = session.sql(f"SELECT DATEDIFF('minute', '{created}', '{last_active}') AS D").collect()
            duration_mins = int(ts1[0]["D"])
        except:
            pass

        ctx_lines = [
            f"SESSION METADATA: User={user_name}, Created={created}, Duration={duration_mins}min",
            "",
            "QUESTIONS ASKED:"
        ]
        for i, q in enumerate(user_questions, 1):
            ctx_lines.append(f"  {i}. {q}")
        ctx_lines.append("")

        ctx_lines.append("SQL RESULTS:")
        for r in result_rows:
            ctx_lines.append(f"  Q: {r['QUESTION']}")
            ctx_lines.append(f"  Summary: {r['RESULT_SUMMARY']}")
            if r["SQL_GENERATED"]:
                ctx_lines.append(f"  SQL: {r['SQL_GENERATED'][:200]}")
            ctx_lines.append("")

        ctx_lines.append("SIMULATIONS:")
        if sim_rows:
            for s in sim_rows:
                scenario = s["SCENARIO_DESC"]
                try:
                    baseline_json = json.loads(str(s["BASELINE_JSON"]))
                    sim_json = json.loads(str(s["SIM_RESULT_JSON"]))
                    delta_pct = sim_json.get("delta_pct", "N/A")
                    baseline_val = baseline_json.get("value", "N/A")
                    sim_val = sim_json.get("value", "N/A")
                    ctx_lines.append(
                        f"  Scenario: {scenario}  ->  "
                        f"Baseline: {baseline_val:,.0f}, Simulated: {sim_val:,.0f}, Delta: {delta_pct}%"
                    )
                except:
                    ctx_lines.append(f"  Scenario: {scenario}")
        else:
            ctx_lines.append("  (no simulations run)")
        ctx_lines.append("")

        ctx_lines.append("NARRATIVE INSIGHTS:")
        if narrative_insights:
            for ni in narrative_insights:
                ctx_lines.append(f"  {ni[:500]}")
        else:
            ctx_lines.append("  (no narrative insights generated)")

        full_context = "\n".join(ctx_lines)

        doc_system = (
            "You are a senior business analyst writing a board-ready executive report. "
            "Structure your response as a professional document with these exact sections:\n\n"
            "**Executive Summary** - 3-4 sentences overview\n"
            "**Key Findings** - Numbered list of 3-5 data-backed findings\n"
            "**Simulations & Scenarios** - What-if analysis results (skip if none)\n"
            "**Recommendations** - 2-3 actionable next steps\n"
            "**Questions Answered** - Bullet list of questions covered\n\n"
            "Use specific numbers. Be concise and executive-friendly."
        )

        document_content = _complete(session, "llama3.1-70b", doc_system, full_context)

        if REPORT_TITLE and REPORT_TITLE.strip():
            title = REPORT_TITLE.strip()
        else:
            title = _complete_simple(
                session, "llama3.1-8b",
                f"Generate a 6-10 word professional report title from: {user_questions[0] if user_questions else 'analytics session'}. Return only the title."
            ).strip().strip('"').strip("'").rstrip(".")[:150]

        esc_title = _escape(title)
        esc_content = _escape(document_content)
        esc_user = _escape(user_name)

        session.sql(f"""
            INSERT INTO SPEAKFLAKE_DB.MEMORY.INSIGHT_DOCUMENTS
            (SESSION_ID, TITLE, CONTENT, USER_NAME)
            VALUES ('{sid}', '{esc_title}', '{esc_content}', '{esc_user}')
        """).collect()

        doc_id_row = session.sql(f"""
            SELECT MAX(DOC_ID) AS DID FROM SPEAKFLAKE_DB.MEMORY.INSIGHT_DOCUMENTS
            WHERE SESSION_ID = '{sid}'
        """).collect()
        doc_id = int(doc_id_row[0]["DID"]) if doc_id_row else 0

        if title and not REPORT_TITLE:
            session.sql(f"""
                UPDATE SPEAKFLAKE_DB.MEMORY.SESSIONS
                SET TITLE = '{esc_title}'
                WHERE SESSION_ID = '{sid}' AND (TITLE IS NULL OR TITLE = '')
            """).collect()

        return {
            "type": "insight_document",
            "doc_id": doc_id,
            "title": title,
            "content": document_content,
            "session_id": SESSION_ID,
            "stats": {
                "questions": len(user_questions),
                "simulations": len(sim_rows),
                "narrative_insights": len(narrative_insights),
                "duration_mins": duration_mins
            },
            "sections": [
                "executive_summary",
                "key_findings",
                "simulations",
                "recommendations",
                "questions_answered"
            ]
        }

    except Exception as e:
        return {
            "type": "error",
            "message": f"GENERATE_INSIGHT_DOCUMENT error: {str(e)}"
        }
$$;

-- ----- PARSE_SIMULATION -----
CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.PARSE_SIMULATION(SESSION_ID VARCHAR, USER_MESSAGE VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import json

def _escape(s):
    if s is None:
        return ""
    return str(s).replace("'", "''")

def run(session, SESSION_ID: str, USER_MESSAGE: str) -> dict:
    try:
        sid = _escape(SESSION_ID)

        result_rows = session.sql(f"""
            SELECT TURN_NUMBER, QUESTION, RESULT_SUMMARY
            FROM SPEAKFLAKE_DB.MEMORY.SESSION_RESULTS
            WHERE SESSION_ID = '{sid}'
            ORDER BY TURN_NUMBER DESC LIMIT 5
        """).collect()

        prior = "Session context:\n"
        for r in reversed(result_rows):
            prior += f"  Turn {r['TURN_NUMBER']}: {r['QUESTION']} -> {r['RESULT_SUMMARY']}\n"

        target_rows = session.sql("""
            SELECT REGION, QUARTER, FISCAL_YEAR, TARGET_VALUE, ACTUAL_VALUE
            FROM SPEAKFLAKE_DB.CORE.REGION_TARGETS
            ORDER BY FISCAL_YEAR, QUARTER, REGION
        """).collect()
        target_context = "Target vs Actual data:\n"
        for t in target_rows:
            gap = float(t["ACTUAL_VALUE"] or 0) - float(t["TARGET_VALUE"] or 0)
            target_context += f"  {t['REGION']} {t['QUARTER']} FY{t['FISCAL_YEAR']}: Target={t['TARGET_VALUE']}, Actual={t['ACTUAL_VALUE']}, Gap={gap:+,.0f}\n"

        prompt = (
            f"{prior}\n{target_context}\n"
            f"User simulation request: {USER_MESSAGE}\n\n"
            "Extract simulation parameters as JSON with these exact keys:\n"
            "- source_table: always 'SALES_FACT'\n"
            "- modification_type: 'SCALE_VALUE' or 'SET_VALUE'\n"
            "- target_column: column to modify (usually 'DEAL_VALUE')\n"
            "- condition: SQL WHERE clause (use exact region names like 'North America', 'APAC', 'EMEA', 'LATAM')\n"
            "- scale_factor: multiplier for SCALE_VALUE (e.g. 1.1 for 10% increase)\n"
            "- set_value: value for SET_VALUE\n"
            "- baseline_metric: SQL aggregation for baseline (e.g. \"SUM(DEAL_VALUE)\")\n"
            "- description: 1-line scenario description\n\n"
            "Return ONLY valid JSON, no explanation."
        )

        esc = _escape(prompt)
        raw = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{esc}') AS R").collect()[0]["R"]

        cleaned = raw.strip()
        if "```" in cleaned:
            parts = cleaned.split("```")
            for p in parts:
                p = p.strip()
                if p.startswith("json"):
                    p = p[4:].strip()
                if p.startswith("{"):
                    cleaned = p
                    break

        js = cleaned.find("{")
        je = cleaned.rfind("}") + 1
        if js >= 0 and je > js:
            result = json.loads(cleaned[js:je])
            for key in ["source_table", "modification_type", "target_column", "condition",
                         "baseline_metric", "description"]:
                if key not in result:
                    if key == "source_table":
                        result[key] = "SALES_FACT"
                    elif key == "modification_type":
                        result[key] = "SCALE_VALUE"
                    elif key == "target_column":
                        result[key] = "DEAL_VALUE"
                    elif key == "condition":
                        result[key] = "1=1"
                    elif key == "baseline_metric":
                        result[key] = "SUM(DEAL_VALUE)"
                    elif key == "description":
                        result[key] = USER_MESSAGE[:200]
                    else:
                        break
            cond = result["condition"]
            if "NA" in cond and "North America" not in cond:
                result["condition"] = cond.replace("'NA'", "'North America'").replace("= NA", "= 'North America'")
            sf = result.get("scale_factor")
            if sf is not None:
                try:
                    sf = float(sf)
                    if sf == 0:
                        sf = 1.0
                    result["scale_factor"] = sf
                except (ValueError, TypeError):
                    result["scale_factor"] = 1.0
            return result
        else:
            return {"error": "Could not parse simulation parameters from LLM response."}

    except Exception as e:
        return {"error": f"PARSE_SIMULATION error: {str(e)}"}
$$;

-- ----- RUN_SIMULATION -----
CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.RUN_SIMULATION(SESSION_ID VARCHAR, SIM_PARAMS VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import json
import time

def _escape(s):
    if s is None:
        return ""
    return str(s).replace("'", "''")

def _complete_simple(session, model, prompt):
    esc = _escape(prompt)
    return session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{esc}') AS R").collect()[0]["R"]

def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def run(session, SESSION_ID: str, SIM_PARAMS: dict) -> dict:
    clone_name = None
    sim_id = None
    try:
        source_table = SIM_PARAMS.get("source_table", "SALES_FACT")
        mod_type = SIM_PARAMS.get("modification_type", "SCALE_VALUE")
        target_column = SIM_PARAMS.get("target_column", "DEAL_VALUE")
        condition = SIM_PARAMS.get("condition", "1=1")
        scale_factor = _safe_float(SIM_PARAMS.get("scale_factor", 1.0), 1.0)
        set_value = SIM_PARAMS.get("set_value", "")
        baseline_metric = SIM_PARAMS.get("baseline_metric", "SUM(DEAL_VALUE)")
        description = SIM_PARAMS.get("description", "What-if simulation")

        sid_short = SESSION_ID.replace("-", "")[:8].upper()
        ts = str(int(time.time()))
        clone_name = f"SIM_{sid_short}_{ts}"

        session.sql(f"""
            CREATE TRANSIENT TABLE SPEAKFLAKE_DB.SIMULATION.{clone_name}
            CLONE SPEAKFLAKE_DB.CORE.{source_table}
        """).collect()

        baseline_row = session.sql(f"""
            SELECT {baseline_metric} AS METRIC
            FROM SPEAKFLAKE_DB.SIMULATION.{clone_name}
            WHERE DEAL_STATUS = 'Closed Won'
        """).collect()
        baseline_value = _safe_float(baseline_row[0]["METRIC"]) if baseline_row else 0

        if mod_type == "SCALE_VALUE":
            session.sql(f"""
                UPDATE SPEAKFLAKE_DB.SIMULATION.{clone_name}
                SET {target_column} = {target_column} * {scale_factor}
                WHERE {condition} AND DEAL_STATUS = 'Closed Won'
            """).collect()
        elif mod_type == "SET_VALUE":
            session.sql(f"""
                UPDATE SPEAKFLAKE_DB.SIMULATION.{clone_name}
                SET {target_column} = {set_value}
                WHERE {condition} AND DEAL_STATUS = 'Closed Won'
            """).collect()

        rows_affected_row = session.sql(f"SELECT COUNT(*) AS C FROM SPEAKFLAKE_DB.SIMULATION.{clone_name} WHERE {condition} AND DEAL_STATUS = 'Closed Won'").collect()
        rows_affected = int(rows_affected_row[0]["C"]) if rows_affected_row else 0

        sim_row = session.sql(f"""
            SELECT {baseline_metric} AS METRIC
            FROM SPEAKFLAKE_DB.SIMULATION.{clone_name}
            WHERE DEAL_STATUS = 'Closed Won'
        """).collect()
        sim_value = _safe_float(sim_row[0]["METRIC"]) if sim_row else 0

        delta = sim_value - baseline_value
        delta_pct = round((delta / baseline_value * 100), 2) if baseline_value else 0

        narrative = _complete_simple(
            session, "llama3.1-8b",
            f"Simulation result: scenario='{description}', baseline=${baseline_value:,.0f}, "
            f"simulated=${sim_value:,.0f}, delta={delta_pct:+.1f}%. "
            f"Rows affected: {rows_affected}. "
            "Write a 2-3 sentence executive summary of this what-if scenario. Be specific with numbers."
        )

        baseline_json = json.dumps({"metric": baseline_metric, "value": baseline_value})
        sim_result_json = json.dumps({
            "metric": baseline_metric, "value": sim_value,
            "delta": delta, "delta_pct": delta_pct, "rows_affected": rows_affected
        })

        session.sql(f"""
            INSERT INTO SPEAKFLAKE_DB.SIMULATION.SIM_LOG
            (SESSION_ID, CLONE_NAME, SOURCE_TABLE, SCENARIO_DESC,
             BASELINE_JSON, SIM_RESULT_JSON, STATUS, COMPLETED_AT)
            SELECT
                '{_escape(SESSION_ID)}',
                '{clone_name}',
                '{_escape(source_table)}',
                '{_escape(description)}',
                PARSE_JSON('{_escape(baseline_json)}'),
                PARSE_JSON('{_escape(sim_result_json)}'),
                'COMPLETE',
                CURRENT_TIMESTAMP()
        """).collect()

        sim_id_row = session.sql(f"""
            SELECT MAX(SIM_ID) AS SIM_ID FROM SPEAKFLAKE_DB.SIMULATION.SIM_LOG
            WHERE CLONE_NAME = '{clone_name}'
        """).collect()
        sim_id = int(sim_id_row[0]["SIM_ID"]) if sim_id_row else None

        return {
            "type": "simulation",
            "sim_id": sim_id,
            "clone_name": clone_name,
            "baseline": baseline_value,
            "sim_result": sim_value,
            "delta": delta,
            "delta_pct": delta_pct,
            "rows_affected": rows_affected,
            "narrative": narrative,
            "description": description
        }

    except Exception as e:
        if clone_name:
            try:
                session.sql(f"DROP TABLE IF EXISTS SPEAKFLAKE_DB.SIMULATION.{clone_name}").collect()
            except:
                pass
        return {"type": "error", "message": f"RUN_SIMULATION error: {str(e)}"}
$$;

-- ----- SPEAKFLAKE_ORCHESTRATOR -----
CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.SPEAKFLAKE_ORCHESTRATOR(SESSION_ID VARCHAR, USER_MESSAGE VARCHAR, SEMANTIC_STAGE_PATH VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import json
import _snowflake

def _escape(s):
    if s is None:
        return ""
    return str(s).replace("'", "''")

def _complete(session, model, system_prompt, user_prompt):
    sys_esc = _escape(system_prompt)
    usr_esc = _escape(user_prompt)
    row = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            [
                {{'role':'system','content':'{sys_esc}'}},
                {{'role':'user','content':'{usr_esc}'}}
            ],
            {{}}
        ) AS R
    """).collect()[0]["R"]
    parsed = json.loads(row)
    return parsed["choices"][0]["messages"]

def _complete_simple(session, model, prompt):
    esc = _escape(prompt)
    return session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{esc}') AS R").collect()[0]["R"]

def _call_analyst(semantic_path, user_message, history_rows=None):
    messages = []
    if history_rows:
        for row in history_rows:
            role = row["ROLE"]
            msg = row["MESSAGE"]
            if role == "USER":
                messages.append({"role": "user", "content": [{"type": "text", "text": msg}]})
            elif role == "ASSISTANT":
                messages.append({"role": "analyst", "content": [{"type": "text", "text": msg}]})
    messages.append({"role": "user", "content": [{"type": "text", "text": user_message}]})
    resp = _snowflake.send_snow_api_request(
        "POST", "/api/v2/cortex/analyst/message",
        {}, {}, {"messages": messages, "semantic_model_file": semantic_path}, {},
        30000
    )
    content = json.loads(resp["content"])
    return content

def main(session, SESSION_ID: str, USER_MESSAGE: str, SEMANTIC_STAGE_PATH: str) -> dict:
    try:
        sid = _escape(SESSION_ID)

        session.sql(f"""
            UPDATE SPEAKFLAKE_DB.MEMORY.SESSIONS
            SET LAST_ACTIVE = CURRENT_TIMESTAMP()
            WHERE SESSION_ID = '{sid}'
        """).collect()

        turn_rows = session.sql(f"""
            SELECT COALESCE(MAX(TURN_NUMBER), 0) AS MAX_TURN
            FROM SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY
            WHERE SESSION_ID = '{sid}'
        """).collect()
        next_turn = int(turn_rows[0]["MAX_TURN"]) + 1

        prior_rows = session.sql(f"""
            SELECT TURN_NUMBER, QUESTION, RESULT_SUMMARY
            FROM SPEAKFLAKE_DB.MEMORY.SESSION_RESULTS
            WHERE SESSION_ID = '{sid}'
            ORDER BY TURN_NUMBER DESC LIMIT 3
        """).collect()
        prior_context = ""
        for r in reversed(prior_rows):
            prior_context += f"Turn {r['TURN_NUMBER']}: {r['QUESTION']} -> {r['RESULT_SUMMARY']}\n"

        classify_prompt = (
            f"Prior conversation context:\n{prior_context}\n\n"
            f"New user message: {USER_MESSAGE}\n\n"
            "Classify the intent. Return JSON: {\"intent_type\": \"SQL_QUERY\" | \"NARRATIVE\" | \"SIMULATION\" | \"CLARIFY_NEEDED\", \"confidence\": 0.0-1.0}\n"
            "SQL_QUERY = needs data from database. NARRATIVE = wants explanation/analysis of prior results. "
            "SIMULATION = what-if/hypothetical scenario. CLARIFY_NEEDED = too vague.\n"
            "Return ONLY JSON."
        )

        esc_classify = _escape(classify_prompt)
        raw_intent = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', '{esc_classify}') AS R").collect()[0]["R"]

        intent_type = "SQL_QUERY"
        confidence = 0.8
        try:
            cleaned = raw_intent.strip()
            js = cleaned.find("{")
            je = cleaned.rfind("}") + 1
            if js >= 0 and je > js:
                ij = json.loads(cleaned[js:je])
                intent_type = ij.get("intent_type", "SQL_QUERY")
                confidence = float(ij.get("confidence", 0.8))
        except (json.JSONDecodeError, ValueError, TypeError):
            pass

        if confidence < 0.6 or intent_type == "CLARIFY_NEEDED":
            clarify_response = _complete_simple(
                session, "llama3.1-8b",
                f"The user said: '{USER_MESSAGE}'. This is ambiguous. "
                "Ask ONE short clarifying question. Max 15 words."
            )
            session.sql(f"""
                INSERT INTO SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY
                (SESSION_ID, TURN_NUMBER, ROLE, MESSAGE, INTENT_TYPE, CONFIDENCE)
                VALUES ('{sid}', {next_turn}, 'USER', '{_escape(USER_MESSAGE)}', 'CLARIFY_NEEDED', {confidence})
            """).collect()
            session.sql(f"""
                INSERT INTO SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY
                (SESSION_ID, TURN_NUMBER, ROLE, MESSAGE, INTENT_TYPE, CONFIDENCE)
                VALUES ('{sid}', {next_turn + 1}, 'ASSISTANT', '{_escape(clarify_response)}', 'CLARIFY_NEEDED', {confidence})
            """).collect()
            return {
                "session_id": SESSION_ID, "turn_number": next_turn,
                "type": "clarify", "intent_type": "CLARIFY_NEEDED",
                "confidence": confidence, "message": clarify_response.strip()
            }

        response_payload = None

        if intent_type == "NARRATIVE":
            narr_context = prior_context if prior_context else "No prior context available."
            narr_response = _complete(
                session, "llama3.1-70b",
                "You are a senior business analyst. Provide clear, data-backed narrative insights. "
                "Use specific numbers from the context. Be concise (3-5 sentences).",
                f"Context:\n{narr_context}\n\nUser request: {USER_MESSAGE}"
            )
            response_payload = {
                "session_id": SESSION_ID, "turn_number": next_turn,
                "type": "narrative", "intent_type": "NARRATIVE",
                "confidence": confidence, "message": narr_response.strip()
            }

        elif intent_type == "SQL_QUERY":
            history_rows = session.sql(f"""
                SELECT ROLE, MESSAGE
                FROM SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY
                WHERE SESSION_ID = '{sid}'
                ORDER BY TURN_NUMBER ASC
            """).collect()

            analyst_response = _call_analyst(SEMANTIC_STAGE_PATH, USER_MESSAGE, history_rows)

            analyst_text = ""
            analyst_sql = ""
            analyst_interpretation = ""
            for item in analyst_response.get("message", {}).get("content", []):
                if item.get("type") == "text":
                    analyst_text += item.get("text", "")
                    analyst_interpretation = item.get("text", "")
                elif item.get("type") == "sql":
                    analyst_sql = item.get("statement", "")

            result_data = []
            result_summary = ""
            if analyst_sql:
                try:
                    data_rows = session.sql(analyst_sql).collect()
                    result_data = [row.as_dict() for row in data_rows[:50]]
                    preview = json.dumps(result_data[:10], default=str)
                    result_summary = _complete_simple(
                        session, "llama3.1-8b",
                        f"Summarize this data as a concise business insight in 2-4 sentences. "
                        f"Do NOT list raw data. Do NOT repeat the question. Do NOT say 'Here is' or 'Based on'. "
                        f"Use em-dashes to connect related facts. Keep it to 2-4 sentences. Be specific with numbers. "
                        f"Question: {USER_MESSAGE}. Data: {preview}"
                    )
                except Exception as sql_err:
                    result_summary = f"SQL execution error: {str(sql_err)}"

                result_json_str = json.dumps(result_data, default=str)
                session.sql(f"""
                    INSERT INTO SPEAKFLAKE_DB.MEMORY.SESSION_RESULTS
                    (SESSION_ID, TURN_NUMBER, QUESTION, SQL_GENERATED, RESULT_SUMMARY, RESULT_JSON)
                    SELECT '{sid}', {next_turn}, '{_escape(USER_MESSAGE)}',
                           '{_escape(analyst_sql)}', '{_escape(result_summary)}',
                           PARSE_JSON('{_escape(result_json_str)}')
                """).collect()

                response_payload = {
                    "session_id": SESSION_ID, "turn_number": next_turn,
                    "type": "sql", "intent_type": "SQL_QUERY",
                    "confidence": confidence,
                    "sql": analyst_sql,
                    "analyst_interpretation": analyst_interpretation.strip(),
                    "result_summary": result_summary.strip(),
                    "result_data": result_data,
                    "message": result_summary.strip()
                }
            else:
                response_payload = {
                    "session_id": SESSION_ID, "turn_number": next_turn,
                    "type": "narrative", "intent_type": "SQL_QUERY",
                    "confidence": confidence,
                    "message": analyst_text.strip() if analyst_text else "I couldn't generate SQL for that question. Could you rephrase?"
                }

        elif intent_type == "SIMULATION":
            sim_params = session.sql(f"""
                CALL SPEAKFLAKE_DB.APP.PARSE_SIMULATION('{sid}', '{_escape(USER_MESSAGE)}')
            """).collect()[0][0]
            if isinstance(sim_params, str):
                sim_params = json.loads(sim_params)

            if "error" in sim_params:
                response_payload = {
                    "session_id": SESSION_ID, "turn_number": next_turn,
                    "type": "error", "intent_type": "SIMULATION",
                    "confidence": confidence, "message": sim_params["error"]
                }
            else:
                sim_result = session.sql(f"""
                    CALL SPEAKFLAKE_DB.APP.RUN_SIMULATION('{sid}', PARSE_JSON('{_escape(json.dumps(sim_params))}'))
                """).collect()[0][0]
                if isinstance(sim_result, str):
                    sim_result = json.loads(sim_result)

                if sim_result.get("type") == "error":
                    response_payload = {
                        "session_id": SESSION_ID, "turn_number": next_turn,
                        "type": "error", "intent_type": "SIMULATION",
                        "confidence": confidence, "message": sim_result.get("message", "Simulation failed")
                    }
                else:
                    response_payload = {
                        "session_id": SESSION_ID, "turn_number": next_turn,
                        "type": "simulation", "intent_type": "SIMULATION",
                        "confidence": confidence,
                        "baseline": sim_result.get("baseline"),
                        "sim_result": sim_result.get("sim_result"),
                        "delta": sim_result.get("delta"),
                        "delta_pct": sim_result.get("delta_pct"),
                        "rows_affected": sim_result.get("rows_affected"),
                        "narrative": sim_result.get("narrative"),
                        "message": sim_result.get("narrative", "")
                    }

        else:
            response_payload = {
                "session_id": SESSION_ID, "turn_number": next_turn,
                "type": "unknown", "intent_type": intent_type,
                "confidence": confidence,
                "message": f"Unrecognized intent '{intent_type}'. Please rephrase."
            }

        session.sql(f"""
            INSERT INTO SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY
            (SESSION_ID, TURN_NUMBER, ROLE, MESSAGE, INTENT_TYPE, CONFIDENCE)
            VALUES ('{sid}', {next_turn}, 'USER', '{_escape(USER_MESSAGE)}', '{intent_type}', {confidence})
        """).collect()

        assistant_msg = response_payload.get("message", "") if response_payload else ""
        session.sql(f"""
            INSERT INTO SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY
            (SESSION_ID, TURN_NUMBER, ROLE, MESSAGE, INTENT_TYPE, CONFIDENCE)
            VALUES ('{sid}', {next_turn + 1}, 'ASSISTANT', '{_escape(assistant_msg)}', '{intent_type}', {confidence})
        """).collect()

        return response_payload

    except Exception as e:
        return {"type": "error", "message": f"ORCHESTRATOR error: {str(e)}"}
$$;

-- ----- GENERATE_DEMO_DATA -----
CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.GENERATE_DEMO_DATA()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import random
import json

def run(session):
    random.seed(42)
    products = [
        ('DataVault Pro','Platform'),
        ('InsightEngine','Analytics'),
        ('CloudSync','Integration')
    ]
    countries = {
        'North America': ['United States','Canada','United States','United States','Mexico'],
        'APAC': ['Japan','Australia','Singapore','South Korea','India'],
        'EMEA': ['United Kingdom','Germany','France','Netherlands','Sweden'],
        'LATAM': ['Brazil','Mexico','Colombia','Chile','Argentina']
    }
    reps = {
        'North America': ['Sarah Chen','Marcus Johnson','Emily Rodriguez','David Park','Jessica Williams','Ryan Cooper'],
        'APAC': ['Akiko Tanaka','Wei Zhang','Priya Sharma','Kenji Nakamura','Min-Jun Lee'],
        'EMEA': ['James Okafor','Lena Mueller','Raj Patel','Sofia Rossi','Thomas Weber'],
        'LATAM': ['Carlos Mendez','Ana Gutierrez','Diego Fuentes']
    }
    won_targets = {
        'North America': {2023: [650,700,720,780], 2024: [700,750,780,830]},
        'APAC':          {2023: [380,410,420,460], 2024: [420,450,470,510]},
        'EMEA':          {2023: [500,540,560,600], 2024: [550,590,610,660]},
        'LATAM':         {2023: [200,220,230,250], 2024: [230,250,260,280]}
    }
    quarter_months = {'Q1': [1,2,3], 'Q2': [4,5,6], 'Q3': [7,8,9], 'Q4': [10,11,12]}
    close_dates = {
        'Q1': ['2024-01-15','2024-02-10','2024-03-20','2024-01-28','2024-03-05'],
        'Q2': ['2024-04-12','2024-05-18','2024-06-25','2024-04-30','2024-06-10'],
        'Q3': ['2024-07-08','2024-08-14','2024-09-22','2024-07-25','2024-09-05'],
        'Q4': ['2024-10-10','2024-11-15','2024-12-18','2024-10-28','2024-12-05']
    }

    rows = []
    sale_id = 1
    for fy in [2023, 2024]:
        for qi, q in enumerate(['Q1','Q2','Q3','Q4']):
            for region in ['North America','APAC','EMEA','LATAM']:
                target_k = won_targets[region][fy][qi]
                target_val = target_k * 1000
                num_won = random.randint(5, 12)
                total_won = 0
                for i in range(num_won):
                    rep = random.choice(reps[region])
                    prod, cat = random.choice(products)
                    cty = random.choice(countries[region])
                    if i < num_won - 1:
                        val = random.randint(30, 180) * 1000
                    else:
                        val = max(30000, target_val - total_won + random.randint(-50000, 50000))
                    total_won += val
                    dt_idx = i % len(close_dates[q])
                    cd = close_dates[q][dt_idx].replace('2024', str(fy))
                    rows.append((sale_id, region, cty, rep, prod, cat, val, 'Closed Won', q, fy, cd))
                    sale_id += 1
                num_lost = random.randint(2, 5)
                for i in range(num_lost):
                    rep = random.choice(reps[region])
                    prod, cat = random.choice(products)
                    cty = random.choice(countries[region])
                    lost_val = random.randint(20, 120) * 1000
                    dt_idx = i % len(close_dates[q])
                    close_date = close_dates[q][dt_idx].replace('2024', str(fy))
                    rows.append((sale_id, region, cty, rep, prod, cat,
                                 lost_val, 'Closed Lost', q, fy, close_date))
                    sale_id += 1

    pipeline_deals = [
        ('North America','United States','Sarah Chen','DataVault Pro','Platform',320000,'Q4',2024,'2025-01-15'),
        ('North America','Canada','Emily Rodriguez','InsightEngine','Analytics',185000,'Q4',2024,'2025-01-28'),
        ('APAC','Japan','Akiko Tanaka','DataVault Pro','Platform',275000,'Q4',2024,'2025-02-10'),
        ('APAC','Australia','Wei Zhang','CloudSync','Integration',190000,'Q4',2024,'2025-01-20'),
        ('APAC','Singapore','Priya Sharma','InsightEngine','Analytics',145000,'Q4',2024,'2025-02-05'),
        ('EMEA','Germany','James Okafor','DataVault Pro','Platform',210000,'Q4',2024,'2025-01-30'),
        ('EMEA','United Kingdom','Lena Mueller','CloudSync','Integration',165000,'Q4',2024,'2025-02-15'),
        ('LATAM','Brazil','Carlos Mendez','DataVault Pro','Platform',195000,'Q4',2024,'2025-01-25'),
        ('LATAM','Colombia','Ana Gutierrez','InsightEngine','Analytics',130000,'Q4',2024,'2025-02-08'),
        ('North America','United States','David Park','CloudSync','Integration',240000,'Q4',2024,'2025-02-20')
    ]
    for p in pipeline_deals:
        rows.append((sale_id, p[0], p[1], p[2], p[3], p[4], p[5], 'Pipeline', p[6], p[7], p[8]))
        sale_id += 1

    session.sql("DELETE FROM SPEAKFLAKE_DB.CORE.SALES_FACT").collect()
    batch = []
    for r in rows:
        vals = f"({r[0]},'{r[1]}','{r[2]}','{r[3]}','{r[4]}','{r[5]}',{r[6]},'{r[7]}','{r[8]}',{r[9]},'{r[10]}')"
        batch.append(vals)
        if len(batch) >= 50:
            session.sql("INSERT INTO SPEAKFLAKE_DB.CORE.SALES_FACT VALUES " + ",".join(batch)).collect()
            batch = []
    if batch:
        session.sql("INSERT INTO SPEAKFLAKE_DB.CORE.SALES_FACT VALUES " + ",".join(batch)).collect()

    session.sql("DELETE FROM SPEAKFLAKE_DB.CORE.SALES_REPS").collect()
    rep_id = 1
    all_reps = []
    for region, rep_list in reps.items():
        for rname in rep_list:
            team = random.choice(['Enterprise','Mid-Market'])
            hd = f"{random.randint(2018,2023)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            all_reps.append(f"({rep_id},'{rname}','{region}','{team}','{hd}',TRUE)")
            rep_id += 1
    session.sql("INSERT INTO SPEAKFLAKE_DB.CORE.SALES_REPS VALUES " + ",".join(all_reps)).collect()

    session.sql("DELETE FROM SPEAKFLAKE_DB.CORE.REGION_TARGETS").collect()
    target_rows = []
    for region in ['North America','APAC','EMEA','LATAM']:
        for fy in [2023, 2024]:
            for qi, q in enumerate(['Q1','Q2','Q3','Q4']):
                tv = won_targets[region][fy][qi] * 1000
                actual_rows = session.sql(f"""
                    SELECT COALESCE(SUM(DEAL_VALUE),0) AS S
                    FROM SPEAKFLAKE_DB.CORE.SALES_FACT
                    WHERE REGION='{region}' AND QUARTER='{q}'
                      AND FISCAL_YEAR={fy} AND DEAL_STATUS='Closed Won'
                """).collect()
                av = float(actual_rows[0]["S"])
                target_rows.append(f"('{region}','{q}',{fy},{tv},{av})")
    session.sql("INSERT INTO SPEAKFLAKE_DB.CORE.REGION_TARGETS VALUES " + ",".join(target_rows)).collect()

    return f"Demo data loaded: {len(rows)} deals, {rep_id-1} reps, {len(target_rows)} targets"
$$;

-- ----- RUN_BENCHMARK -----
CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.RUN_BENCHMARK(VERSION VARCHAR, PROC_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import json, time

QUESTIONS = [
    "Show me target vs actual for each region in FY2024",
    "Which region missed their target by the most?",
    "What is the total revenue by region?",
    "Who is the top sales rep by revenue?",
    "What is the average deal size by product?",
    "How much pipeline do we have?",
    "Did all regions hit their targets in FY2024?",
    "Show me quarterly target vs actual breakdown by region for FY2024",
    "Which region exceeded their target by the most?",
    "How many deals are Closed Won?",
    "What is APAC total revenue for FY2024?",
    "Compare North America vs EMEA revenue",
    "What is the attainment for LATAM in FY2024?",
    "Show Q3 performance across all regions",
    "Who are the top 3 reps by deal count?",
    "What product generates the most revenue?",
    "Which quarter had the highest revenue in FY2024?",
    "How many deals are still in pipeline by region?",
    "What is the win rate by region?",
    "Show me revenue by product category"
]

def main(session, VERSION: str, PROC_NAME: str) -> str:
    cu = session.sql("SELECT CURRENT_USER()").collect()[0][0]
    sid_row = session.sql(f"CALL SPEAKFLAKE_DB.APP.CREATE_SESSION('{cu}')").collect()
    sid = sid_row[0][0]
    results = []
    semantic = "@SPEAKFLAKE_DB.SEMANTIC.YAML_STAGE/semantic_model.yaml"

    for i, q in enumerate(QUESTIONS):
        q_esc = q.replace("'", "''")
        t0 = time.time()
        try:
            r = session.sql(f"CALL SPEAKFLAKE_DB.APP.{PROC_NAME}('{sid}', '{q_esc}', '{semantic}')").collect()
            elapsed_ms = int((time.time() - t0) * 1000)
            raw = r[0][0]
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            conf = float(parsed.get("confidence", 0))
            intent = parsed.get("intent_type", "UNKNOWN")
            resp_len = len(str(parsed.get("message", "")))
            success = parsed.get("type") != "error"
        except Exception:
            elapsed_ms = int((time.time() - t0) * 1000)
            conf = 0
            intent = "ERROR"
            resp_len = 0
            success = False

        session.sql(f"""
            INSERT INTO SPEAKFLAKE_DB.APP.BENCHMARK_RESULTS
            VALUES ({i+1}, '{VERSION}', '{q_esc}',
                    DATEADD(ms, -{elapsed_ms}, CURRENT_TIMESTAMP()),
                    CURRENT_TIMESTAMP(), {elapsed_ms}, {conf}, '{intent}', {resp_len}, {success})
        """).collect()
        results.append(f"Q{i+1}: {elapsed_ms}ms")

    session.sql(f"CALL SPEAKFLAKE_DB.APP.CLOSE_SESSION('{sid}')").collect()
    return f"{VERSION} benchmark complete. " + ", ".join(results)
$$;

-- ----- Utility Procedures -----

CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.UPDATE_SEMANTIC_YAML()
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
    rows = session.sql("""
        SELECT $1 AS CONTENT
        FROM @SPEAKFLAKE_DB.SEMANTIC.YAML_STAGE/semantic_model.yaml
        (FILE_FORMAT => 'SPEAKFLAKE_DB.APP.TXT_FORMAT')
    """).collect()
    yaml_content = rows[0]["CONTENT"]

    yaml_content = yaml_content.replace("Spans FY2024-FY2025", "Spans FY2023-FY2024")
    yaml_content = yaml_content.replace(
        'sample_values:\n          - "2024"\n          - "2025"',
        'sample_values:\n          - "2023"\n          - "2024"'
    )

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
    return "OK: semantic_model.yaml updated"
$$;

CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.ASSEMBLE_AND_DEPLOY_APP()
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
    rows = session.sql(
        "SELECT CONTENT FROM SPEAKFLAKE_DB.APP.APP_CODE_STAGING "
        "WHERE FILE_NAME = 'streamlit_app.py' ORDER BY CHUNK_NUM ASC"
    ).collect()

    code = "".join([r["CONTENT"] for r in rows])

    tmp_dir = tempfile.mkdtemp()
    local_path = os.path.join(tmp_dir, "streamlit_app.py")
    with open(local_path, "w") as f:
        f.write(code)

    session.file.put(
        local_path,
        "@SPEAKFLAKE_DB.APP.STREAMLIT_STAGE",
        auto_compress=False,
        overwrite=True,
    )
    os.remove(local_path)
    return "OK: streamlit_app.py assembled and uploaded (" + str(len(code)) + " bytes)"
$$;

CREATE OR REPLACE PROCEDURE SPEAKFLAKE_DB.APP.DEPLOY_ENV_YML()
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
    content = """name: sf_env
channels:
  - snowflake
dependencies:
  - streamlit=1.44.0
"""
    tmp_dir = tempfile.mkdtemp()
    local_path = os.path.join(tmp_dir, "environment.yml")
    with open(local_path, "w") as f:
        f.write(content)
    session.file.put(
        local_path,
        "@SPEAKFLAKE_DB.APP.STREAMLIT_STAGE",
        auto_compress=False,
        overwrite=True,
    )
    os.remove(local_path)
    return "OK: environment.yml uploaded"
$$;
