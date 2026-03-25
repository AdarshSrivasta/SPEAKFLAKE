import streamlit as st
import json
import datetime
from snowflake.snowpark.context import get_active_session

session = get_active_session()
SEMANTIC_PATH = "@SPEAKFLAKE_DB.SEMANTIC.YAML_STAGE/semantic_model.yaml"
DQ = chr(36) + chr(36)
NL = chr(10)
SQ = chr(39)

def _safe_rerun():
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()

st.markdown(
    "<style>"
    "[data-testid='stAppViewContainer'] {"
    "  background: linear-gradient(180deg, #f8f9ff, #eef1ff, #f8f9ff);"
    "}"
    "[data-testid='stAppViewContainer'] .stMarkdown {"
    "  color: #1a1a2e !important;"
    "}"
    "[data-testid='stAppViewContainer'] .stMarkdown h3,"
    "[data-testid='stAppViewContainer'] .stMarkdown h4 {"
    "  color: #0d0d1a !important;"
    "  font-weight: 800 !important;"
    "}"
    "[data-testid='stAppViewContainer'] p,"
    "[data-testid='stAppViewContainer'] span,"
    "[data-testid='stAppViewContainer'] li {"
    "  color: #2d2d3f !important;"
    "}"
    "section[data-testid='stSidebar'] {"
    "  background: linear-gradient(135deg, #6c5ce7, #a29bfe, #74b9ff);"
    "  color: white;"
    "}"
    "section[data-testid='stSidebar'] .stMarkdown {"
    "  color: white;"
    "}"
    "section[data-testid='stSidebar'] .stRadio label {"
    "  color: white !important;"
    "  font-weight: 600;"
    "}"
    "section[data-testid='stSidebar'] p,"
    "section[data-testid='stSidebar'] span {"
    "  color: white !important;"
    "}"
    "div[data-testid='stMetric'] {"
    "  background: linear-gradient(135deg, #a29bfe, #6c5ce7);"
    "  border-radius: 12px;"
    "  padding: 16px;"
    "  color: white;"
    "  box-shadow: 0 4px 15px rgba(108, 92, 231, 0.3);"
    "}"
    "div[data-testid='stMetric'] label {"
    "  color: #dfe6e9 !important;"
    "}"
    "div[data-testid='stMetric'] [data-testid='stMetricValue'] {"
    "  color: #ffffff !important;"
    "}"
    "div[data-testid='stMetric'] p,"
    "div[data-testid='stMetric'] span {"
    "  color: #ffffff !important;"
    "}"
    ".stButton > button {"
    "  background: linear-gradient(135deg, #6c5ce7, #a29bfe) !important;"
    "  color: white !important;"
    "  border: none !important;"
    "  border-radius: 25px !important;"
    "  padding: 8px 24px !important;"
    "  font-weight: 600 !important;"
    "  transition: all 0.3s ease !important;"
    "}"
    ".stButton > button:hover {"
    "  background: linear-gradient(135deg, #fd79a8, #e84393) !important;"
    "  transform: translateY(-2px) !important;"
    "  box-shadow: 0 5px 20px rgba(232, 67, 147, 0.4) !important;"
    "}"
    ".stDownloadButton > button {"
    "  background: linear-gradient(135deg, #6c5ce7, #a29bfe) !important;"
    "  color: white !important;"
    "  border: none !important;"
    "  border-radius: 25px !important;"
    "  padding: 8px 24px !important;"
    "  font-weight: 600 !important;"
    "  transition: all 0.3s ease !important;"
    "}"
    ".stDownloadButton > button:hover {"
    "  background: linear-gradient(135deg, #fd79a8, #e84393) !important;"
    "  transform: translateY(-2px) !important;"
    "  box-shadow: 0 5px 20px rgba(232, 67, 147, 0.4) !important;"
    "}"
    "div.stDataFrame {"
    "  border: 2px solid #a29bfe;"
    "  border-radius: 12px;"
    "  overflow: hidden;"
    "}"
    ".stTextInput > div > div > input {"
    "  border: 2px solid #a29bfe !important;"
    "  border-radius: 25px !important;"
    "  padding: 10px 20px !important;"
    "  color: #1a1a2e !important;"
    "  background-color: #ffffff !important;"
    "}"
    ".stTextInput > div > div > input:focus {"
    "  border-color: #e84393 !important;"
    "  box-shadow: 0 0 10px rgba(232, 67, 147, 0.3) !important;"
    "}"
    ".stSelectbox > div > div {"
    "  border-radius: 25px !important;"
    "}"
    "hr {"
    "  border-image: linear-gradient(90deg, #6c5ce7, #a29bfe, #74b9ff, #a29bfe) 1;"
    "}"
    "code{font-family:JetBrains Mono,Fira Code,monospace !important;}"
    "</style>",
    unsafe_allow_html=True,
)

for key, default in [
    ("session_id", None), ("history", []), ("page", "Chat"),
    ("report_session", None), ("report_content", None), ("hist_page", 0),
    ("session_title", None), ("submitted_suggestion", None),
    ("chat_input_key", 0),
]:
    if key not in st.session_state:
        st.session_state[key] = default

with st.sidebar:
    st.markdown("## SPEAKFLAKE")
    st.caption("Conversational analytics powered by Snowflake")
    st.divider()
    page = st.radio("Navigate", ["Chat", "Session History", "About"],
        index=["Chat", "Session History", "About"].index(st.session_state.page),
        label_visibility="collapsed")
    if page != st.session_state.page:
        st.session_state.page = page
        _safe_rerun()
    st.divider()
    if st.session_state.session_title:
        st.caption(st.session_state.session_title)
    elif st.session_state.session_id:
        st.caption("Session: `" + st.session_state.session_id[:8] + "...`")
    if st.button("New session", use_container_width=True):
        st.session_state.session_id = None
        st.session_state.history = []
        st.session_state.session_title = None
        st.session_state.page = "Chat"
        _safe_rerun()


def _badge(text, color="#6c5ce7"):
    st.markdown(
        "<span style=" + SQ + "background:" + color
        + ";color:white;padding:4px 14px;border-radius:20px;font-size:0.8em;font-weight:700;"
        "box-shadow:0 2px 8px " + color + "44;"
        + SQ + ">" + text + "</span>",
        unsafe_allow_html=True,
    )


def _df(data):
    st.dataframe(data, use_container_width=True)


def _call_proc(proc_name, *args):
    try:
        parts = []
        for a in args:
            parts.append(DQ + a + DQ if isinstance(a, str) else str(a))
        rows = session.sql("CALL SPEAKFLAKE_DB.APP." + proc_name + "(" + ", ".join(parts) + ")").collect()
        raw = rows[0][0] if rows else None
        if raw is None:
            return None
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                return raw
        return raw
    except Exception as e:
        st.error("Something went wrong. Try again.")
        try:
            sid = st.session_state.get("session_id", "")
            cu = session.sql("SELECT CURRENT_USER()").collect()[0][0]
            esc_msg = str(e).replace(SQ, SQ+SQ)
            esc_proc = proc_name.replace(SQ, SQ+SQ)
            esc_sid = str(sid).replace(SQ, SQ+SQ)
            esc_cu = cu.replace(SQ, SQ+SQ)
            session.sql(
                "INSERT INTO SPEAKFLAKE_DB.APP.ERROR_LOG "
                "(SESSION_ID, PROCEDURE_NAME, ERROR_MESSAGE, USER_NAME) SELECT "
                + SQ + esc_sid + SQ + ", "
                + SQ + esc_proc + SQ + ", "
                + SQ + esc_msg + SQ + ", "
                + SQ + esc_cu + SQ
            ).collect()
        except Exception:
            pass
        return None


def _ensure_session():
    if not st.session_state.session_id:
        cu = session.sql("SELECT CURRENT_USER()").collect()[0][0]
        sid = _call_proc("CREATE_SESSION", cu)
        if sid:
            st.session_state.session_id = sid if isinstance(sid, str) else str(sid)
            st.session_state.history = []
            st.session_state.session_title = None


def _auto_title(question):
    try:
        esc = question.replace(SQ, SQ+SQ)
        title = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE("
            + SQ + "llama3.1-8b" + SQ + ", "
            + SQ + "Generate a 4-6 word session title from this question: "
            + esc + ". Return only the title, no quotes." + SQ
            + ") AS R"
        ).collect()[0]["R"]
        title = title.strip().strip(chr(34)).strip(SQ).rstrip(".")[:100]
        if title and st.session_state.session_id:
            esc_t = title.replace(SQ, SQ+SQ)
            sid = st.session_state.session_id.replace(SQ, SQ+SQ)
            session.sql(
                "UPDATE SPEAKFLAKE_DB.MEMORY.SESSIONS SET TITLE = "
                + SQ + esc_t + SQ
                + " WHERE SESSION_ID = " + SQ + sid + SQ
                + " AND (TITLE IS NULL OR TITLE = " + SQ + SQ + ")"
            ).collect()
            st.session_state.session_title = title
    except Exception:
        pass


def _render_assistant(turn):
    t = turn.get("type", "")
    if t == "sql":
        interp = turn.get("analyst_interpretation", "")
        if interp:
            st.caption(interp)
        st.info(turn.get("result_summary", turn.get("message", "")))
        rd = turn.get("result_data")
        if rd and isinstance(rd, list) and len(rd) > 0:
            _df(rd)
        sql_text = turn.get("sql", "")
        if sql_text:
            with st.expander("View SQL"):
                st.code(sql_text, language="sql")
    elif t == "narrative":
        st.markdown(turn.get("message", ""))
    elif t == "simulation":
        bl = turn.get("baseline")
        sr = turn.get("sim_result")
        if bl is not None and sr is not None:
            c1, c2 = st.columns(2)
            c1.metric("Baseline", "$" + "{:,.0f}".format(float(bl)))
            c2.metric("Simulated", "$" + "{:,.0f}".format(float(sr)),
                       delta=str(turn.get("delta_pct", "")) + "%")
        narr = turn.get("narrative", turn.get("message", ""))
        if narr:
            st.markdown(narr)
    elif t == "clarify":
        st.warning(turn.get("message", "Could you be more specific?"))
    elif t == "error":
        st.error(turn.get("message", "An error occurred."))
    else:
        st.write(turn.get("message", str(turn)))


def _rebuild_history_from_db(sid):
    try:
        hist = _call_proc("GET_SESSION_HISTORY", sid)
        if not hist or not isinstance(hist, dict):
            return []
        turns = hist.get("turns", [])
        history = []
        for t in turns:
            role = t.get("ROLE", "").lower()
            msg = t.get("MESSAGE", "")
            intent = t.get("INTENT_TYPE", "")
            if role == "user":
                history.append({"role": "user", "content": msg})
            elif role == "assistant":
                atype = "text"
                if intent == "SQL_QUERY":
                    atype = "sql"
                elif intent == "NARRATIVE":
                    atype = "narrative"
                elif intent == "SIMULATION":
                    atype = "simulation"
                elif intent == "CLARIFY_NEEDED":
                    atype = "clarify"
                history.append({"role": "assistant", "type": atype, "message": msg})
        return history
    except Exception:
        return []


def _render_history():
    for turn in st.session_state.history:
        role = turn.get("role", "user")
        if role == "user":
            st.markdown("**You:** " + turn.get("content", ""))
        else:
            st.markdown("---")
            _render_assistant(turn)
    if st.session_state.history:
        st.markdown("---")


def _render_section(header, body):
    h = header.upper()
    if "EXECUTIVE SUMMARY" in h:
        st.info(body)
    elif "KEY FINDINGS" in h:
        for line in body.strip().split(NL):
            line = line.strip()
            if line and line[0].isdigit():
                st.success(line)
            elif line:
                st.write(line)
    elif "SIMULATION" in h:
        st.markdown("**" + header + "**")
        st.write(body)
    elif "RECOMMENDATION" in h:
        st.markdown("**" + header + "**")
        st.write(body)
    elif "QUESTIONS ANSWERED" in h:
        st.markdown("**" + header + "**")
        questions = [q.strip().lstrip("- ").lstrip("* ") for q in body.strip().split(NL) if q.strip()]
        if questions:
            _df([{"Question": q} for q in questions])
    else:
        st.markdown("**" + header + "**")
        st.write(body)


def _render_report(sid):
    doc_rows = session.sql(
        "SELECT TITLE, CONTENT FROM SPEAKFLAKE_DB.MEMORY.INSIGHT_DOCUMENTS "
        "WHERE SESSION_ID = " + DQ + sid + DQ + " ORDER BY DOC_ID DESC LIMIT 1"
    ).collect()
    if not doc_rows:
        with st.spinner("Generating report for this session..."):
            doc = _call_proc("GENERATE_INSIGHT_DOCUMENT", sid, "")
        if doc and isinstance(doc, dict) and "content" in doc:
            title = doc.get("title", "Report")
            content = doc["content"]
        else:
            st.warning("Could not generate report for this session.")
            return
    else:
        title = doc_rows[0]["TITLE"]
        content = doc_rows[0]["CONTENT"]

    with st.expander("Report: " + title, expanded=True):
        sections = content.split("**")
        current_header = ""
        current_body = ""
        for i, piece in enumerate(sections):
            if i % 2 == 1:
                if current_header and current_body.strip():
                    _render_section(current_header, current_body.strip())
                current_header = piece.strip()
                current_body = ""
            else:
                current_body += piece
        if current_header and current_body.strip():
            _render_section(current_header, current_body.strip())
        elif content:
            st.markdown(content)

    today = datetime.date.today().strftime("%Y%m%d")
    safe_title = title.replace(" ", "_")[:30] if title else "report"
    st.download_button(
        label="Download report",
        data=title + NL + ("=" * len(title)) + NL + NL + content,
        file_name="SPEAKFLAKE_" + safe_title + "_" + today + ".txt",
        mime="text/plain",
    )

    sim_rows = session.sql(
        "SELECT SCENARIO_DESC, BASELINE_JSON, SIM_RESULT_JSON "
        "FROM SPEAKFLAKE_DB.SIMULATION.SIM_LOG "
        "WHERE SESSION_ID = " + DQ + sid + DQ
        + " AND STATUS IN (" + SQ + "COMPLETE" + SQ + "," + SQ + "CLEANED" + SQ + ") "
        "ORDER BY CREATED_AT"
    ).collect()
    if sim_rows:
        st.markdown("**Simulation timeline**")
        sim_table = []
        for r in sim_rows:
            try:
                bj = json.loads(str(r["BASELINE_JSON"]))
                sj = json.loads(str(r["SIM_RESULT_JSON"]))
                sim_table.append({
                    "Scenario": r["SCENARIO_DESC"],
                    "Baseline": "$" + "{:,.0f}".format(bj.get("value", 0)),
                    "Result": "$" + "{:,.0f}".format(sj.get("value", 0)),
                    "Delta": str(sj.get("delta_pct", 0)) + "%",
                })
            except Exception:
                sim_table.append({"Scenario": r["SCENARIO_DESC"], "Baseline": "-", "Result": "-", "Delta": "-"})
        _df(sim_table)


def _submit_question(question):
    _ensure_session()
    st.session_state.history.append({"role": "user", "content": question})
    is_sim = any(w in question.lower() for w in ["what if", "simulate", "scenario", "hypothetical"])
    if is_sim:
        spinner_msg = "Running simulation..."
    else:
        spinner_msg = "Thinking..."
    with st.spinner(spinner_msg):
        result = _call_proc("SPEAKFLAKE_ORCHESTRATOR", st.session_state.session_id, question, SEMANTIC_PATH)
    if result and isinstance(result, dict):
        result["role"] = "assistant"
        st.session_state.history.append(result)
    elif result:
        st.session_state.history.append({"role": "assistant", "type": "text", "message": str(result)})
    if len(st.session_state.history) == 2 and not st.session_state.session_title:
        _auto_title(question)


def page_chat():
    st.markdown("### SPEAKFLAKE")
    _badge("Cortex-powered", "#00b894")
    _caption_text = ""
    _ensure_session()
    if st.session_state.session_title:
        st.caption(st.session_state.session_title)
    elif st.session_state.session_id:
        st.caption("Session `" + st.session_state.session_id[:8] + "...`")
    st.divider()

    if not st.session_state.history:
        st.markdown("#### What would you like to know?")
        st.caption("Click a suggestion or type your own question below.")
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            if st.button("Show me revenue by region", use_container_width=True):
                st.session_state.submitted_suggestion = "Show me revenue by region for FY2024"
                _safe_rerun()
        with sc2:
            if st.button("Which deals are at risk?", use_container_width=True):
                st.session_state.submitted_suggestion = "Which deals are most at risk of not closing?"
                _safe_rerun()
        with sc3:
            if st.button("What if APAC hit 110% of target?", use_container_width=True):
                st.session_state.submitted_suggestion = "What if APAC hit 110% of their revenue target? How would that change overall numbers?"
                _safe_rerun()
    else:
        _render_history()

    if st.session_state.submitted_suggestion:
        suggestion = st.session_state.submitted_suggestion
        st.session_state.submitted_suggestion = None
        _submit_question(suggestion)
        _safe_rerun()

    st.divider()
    inp_col, btn_col = st.columns([5, 1])
    with inp_col:
        user_input = st.text_input(
            "Ask your data anything",
            key="chat_text_" + str(st.session_state.chat_input_key),
            label_visibility="collapsed",
            placeholder="Ask your data anything...",
        )
    with btn_col:
        send = st.button("Send", use_container_width=True)

    if send and user_input.strip():
        st.session_state.chat_input_key += 1
        _submit_question(user_input.strip())
        _safe_rerun()

    st.divider()
    if st.session_state.session_id and st.session_state.history:
        col_a, col_b = st.columns(2)
        with col_a:
            if len(st.session_state.history) >= 4:
                if st.button("Generate report"):
                    with st.spinner("Generating insight document..."):
                        doc = _call_proc("GENERATE_INSIGHT_DOCUMENT", st.session_state.session_id, "")
                    if doc and isinstance(doc, dict) and "content" in doc:
                        st.session_state.report_session = st.session_state.session_id
                        st.session_state.report_content = doc
        with col_b:
            if st.button("Close session"):
                _call_proc("CLOSE_SESSION", st.session_state.session_id)
                st.session_state.session_id = None
                st.session_state.history = []
                st.session_state.session_title = None
                st.success("Session closed.")
                _safe_rerun()

    if (
        st.session_state.report_content
        and isinstance(st.session_state.report_content, dict)
        and st.session_state.report_session == st.session_state.session_id
    ):
        doc = st.session_state.report_content
        with st.expander("Session insight report", expanded=True):
            st.markdown("**" + doc.get("title", "Report") + "**")
            st.markdown(doc.get("content", ""))


def page_history():
    st.markdown("### Your analytics sessions")
    st.caption("Pick up where you left off. Every session is saved automatically.")
    st.divider()
    try:
        cu = session.sql("SELECT CURRENT_USER()").collect()[0][0]
    except Exception:
        st.error("Something went wrong. Try again.")
        return
    try:
        r1 = session.sql("SELECT COUNT(*) AS C FROM SPEAKFLAKE_DB.MEMORY.SESSIONS WHERE USER_NAME = " + DQ + cu + DQ).collect()
        r2 = session.sql("SELECT FLOOR(COUNT(*)/2) AS C FROM SPEAKFLAKE_DB.MEMORY.CONVERSATION_HISTORY ch JOIN SPEAKFLAKE_DB.MEMORY.SESSIONS s ON ch.SESSION_ID=s.SESSION_ID WHERE s.USER_NAME=" + DQ + cu + DQ).collect()
        r3 = session.sql("SELECT COUNT(*) AS C FROM SPEAKFLAKE_DB.SIMULATION.SIM_LOG sl JOIN SPEAKFLAKE_DB.MEMORY.SESSIONS s ON sl.SESSION_ID=s.SESSION_ID WHERE s.USER_NAME=" + DQ + cu + DQ).collect()
    except Exception:
        r1, r2, r3 = [], [], []
    m1, m2, m3 = st.columns(3)
    m1.metric("Total sessions", int(r1[0]["C"]) if r1 else 0)
    m2.metric("Questions asked", int(r2[0]["C"]) if r2 else 0)
    m3.metric("Simulations run", int(r3[0]["C"]) if r3 else 0)
    st.divider()
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        search_q = st.text_input("Search", placeholder="Search by question or topic")
    with fc2:
        status_filter = st.selectbox("Status", ["All", "ACTIVE", "CLOSED", "EXPIRED"])
    with fc3:
        sort_by = st.selectbox("Sort", ["Newest first", "Oldest first", "Most turns"])

    sessions_data = _call_proc("LIST_USER_SESSIONS", cu)
    if not sessions_data or not isinstance(sessions_data, list) or len(sessions_data) == 0:
        st.info("No sessions found. Start a conversation on the Chat page.")
        return

    filtered = []
    for s in sessions_data:
        if status_filter != "All" and s.get("STATUS", "") != status_filter:
            continue
        if search_q:
            searchable = ((s.get("TITLE") or "") + " " + str(s.get("SESSION_ID", "")) + " " + str(s.get("FIRST_QUESTION") or "")).lower()
            if search_q.lower() not in searchable:
                continue
        filtered.append(s)
    if sort_by == "Oldest first":
        filtered = list(reversed(filtered))
    elif sort_by == "Most turns":
        filtered.sort(key=lambda x: int(x.get("TURN_COUNT", 0)), reverse=True)

    page_size = 10
    total_pages = max(1, (len(filtered) + page_size - 1) // page_size)
    cp = st.session_state.hist_page
    if cp >= total_pages:
        cp = total_pages - 1
        st.session_state.hist_page = cp
    page_items = filtered[cp * page_size: (cp + 1) * page_size]

    for s in page_items:
        sid = s.get("SESSION_ID", "")
        title = s.get("TITLE") or "Untitled session"
        turns = int(s.get("TURN_COUNT", 0))
        status = s.get("STATUS", "")
        last_active = s.get("LAST_ACTIVE", "")
        st.markdown(
            "<div style=" + SQ + "border:2px solid #a29bfe;border-radius:12px;padding:14px;margin:6px 0;background:linear-gradient(135deg, rgba(162,155,254,0.05), rgba(253,121,168,0.05));" + SQ + ">",
            unsafe_allow_html=True,
        )
        lc, mc, rc = st.columns([3, 1, 1])
        with lc:
            st.markdown("**" + title + "**")
            first_q = s.get("FIRST_QUESTION", "")
            if first_q:
                st.caption(str(first_q)[:120])
            st.caption("Last active: " + str(last_active))
            badge_colors = {"ACTIVE": "#00b894", "CLOSED": "#a29bfe", "EXPIRED": "#fdcb6e"}
            _badge(status, badge_colors.get(status, "#6c757d"))
        with mc:
            st.metric("Turns", turns)
        with rc:
            if status == "ACTIVE":
                if st.button("Resume", key="res_" + sid):
                    rebuilt = _rebuild_history_from_db(sid)
                    st.session_state.session_id = sid
                    st.session_state.history = rebuilt if rebuilt else []
                    st.session_state.session_title = s.get("TITLE")
                    st.session_state.page = "Chat"
                    if not rebuilt:
                        st.warning("Could not load history. Session opened but conversation may be empty.")
                    _safe_rerun()
                if st.button("Close", key="cls_" + sid):
                    _call_proc("CLOSE_SESSION", sid)
                    if st.session_state.session_id == sid:
                        st.session_state.session_id = None
                        st.session_state.history = []
                        st.session_state.session_title = None
                    st.success("Session closed.")
                    _safe_rerun()
            if st.button("Report", key="rpt_" + sid):
                st.session_state.report_session = sid
        st.markdown("</div>", unsafe_allow_html=True)

    if total_pages > 1:
        pc1, pc2, pc3 = st.columns([1, 2, 1])
        with pc1:
            if st.button("Prev", disabled=(cp == 0)):
                st.session_state.hist_page = cp - 1
                _safe_rerun()
        with pc2:
            st.caption("Page " + str(cp + 1) + " of " + str(total_pages) + " (" + str(len(filtered)) + " sessions)")
        with pc3:
            if st.button("Next", disabled=(cp >= total_pages - 1)):
                st.session_state.hist_page = cp + 1
                _safe_rerun()

    if st.session_state.report_session:
        st.divider()
        _render_report(st.session_state.report_session)


def page_about():
    st.markdown("### SPEAKFLAKE")
    st.markdown("**Conversational analytics built 100% inside Snowflake**")
    st.divider()
    st.markdown(
        "SPEAKFLAKE turns natural language into SQL, insights, and what-if simulations "
        "--- all without leaving Snowflake."
    )

    st.markdown("#### CoCo vs SPEAKFLAKE")
    st.caption("Where Cortex Code stops, SPEAKFLAKE starts.")
    comparison = [
        {"Capability": "Ad-hoc SQL generation", "Cortex Code": "Single query", "SPEAKFLAKE": "Multi-turn with memory", "Advantage": "SPEAKFLAKE"},
        {"Capability": "Follow-up questions", "Cortex Code": "No session context", "SPEAKFLAKE": "Full conversation memory", "Advantage": "SPEAKFLAKE"},
        {"Capability": "What-if simulations", "Cortex Code": "Not available", "SPEAKFLAKE": "Zero-copy clone engine", "Advantage": "SPEAKFLAKE"},
        {"Capability": "Intent classification", "Cortex Code": "Manual routing", "SPEAKFLAKE": "LLM-powered auto-routing", "Advantage": "SPEAKFLAKE"},
        {"Capability": "Board-ready reports", "Cortex Code": "Not available", "SPEAKFLAKE": "Auto-generated documents", "Advantage": "SPEAKFLAKE"},
        {"Capability": "Session history", "Cortex Code": "Lost on close", "SPEAKFLAKE": "Persistent + resumable", "Advantage": "SPEAKFLAKE"},
        {"Capability": "Data security", "Cortex Code": "Role-based", "SPEAKFLAKE": "Role-based + in-Snowflake", "Advantage": "Both"},
        {"Capability": "Setup required", "Cortex Code": "Zero setup", "SPEAKFLAKE": "One-time deploy", "Advantage": "Cortex Code"},
    ]
    _df(comparison)

    st.markdown("#### Tech Stack")
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Query engine", "Cortex Analyst")
    t2.metric("LLM layer", "Cortex COMPLETE")
    t3.metric("Compute", "Snowpark Python")
    t4.metric("Frontend", "Streamlit in Snowflake")

    st.markdown("#### How It Works")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown("**1. Ask**")
        st.caption("Type any business question in plain English")
    with c2:
        st.markdown("**2. Route**")
        st.caption("Intent classifier picks SQL, narrative, or simulation")
    with c3:
        st.markdown("**3. Answer**")
        st.caption("Cortex Analyst generates SQL and runs it instantly")
    with c4:
        st.markdown("**4. Insight**")
        st.caption("Results stored in memory for follow-ups and reports")


if st.session_state.page == "Chat":
    page_chat()
elif st.session_state.page == "Session History":
    page_history()
elif st.session_state.page == "About":
    page_about()
