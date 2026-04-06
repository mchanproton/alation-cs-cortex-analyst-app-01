"""
Cortex Analyst — Conversational Analytics
==========================================
A local Streamlit app for natural-language queries over Snowflake data
using the Cortex Analyst API with streaming responses.
"""

import json
import os
import re
from typing import Any, Dict, Generator, Iterator, List, Optional, Tuple, Union

import pandas as pd
import requests
import snowflake.connector
import sseclient
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def get_config(key: str, default: str = "") -> str:
    """Read config from Streamlit secrets (cloud) or .env (local)."""
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return os.getenv(key, default)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT = get_config("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = get_config("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = get_config("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = get_config("SNOWFLAKE_WAREHOUSE", "CORTEX_ANALYST_WH")
SNOWFLAKE_ROLE = get_config("SNOWFLAKE_ROLE", "CORTEX_USER_ROLE")
SNOWFLAKE_DATABASE = get_config("SNOWFLAKE_DATABASE", "CORTEX_ANALYST_DEMO")
SNOWFLAKE_SCHEMA = get_config("SNOWFLAKE_SCHEMA", "REVENUE_TIMESERIES")

STAGE = "RAW_DATA"
FILE = "revenue_timeseries.yaml"

AVAILABLE_SEMANTIC_MODELS = [
    f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE}/{FILE}"
]

API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"

# Construct the Snowflake API host from the account identifier
# Snowflake URLs require underscores replaced with dashes for SSL cert validation
SNOWFLAKE_HOST = f"{SNOWFLAKE_ACCOUNT.replace('_', '-').lower()}.snowflakecomputing.com"

# ---------------------------------------------------------------------------
# Custom CSS — Snowflake branding
# ---------------------------------------------------------------------------
CUSTOM_CSS = """
<style>
    /* Global font override — Arial */
    html, body, [class*="css"], .stMarkdown, .stText,
    [data-testid="stChatMessage"], [data-testid="stSidebar"],
    input, button, select, textarea {
        font-family: Arial, Helvetica, sans-serif !important;
    }

    /* Hide native Streamlit sidebar collapse button */
    [data-testid="stSidebar"] button[kind="header"] ,
    [data-testid="collapsedControl"] {
        display: none !important;
    }
    /* Sidebar — modern dark theme */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0D1B2A 0%, #11567F 100%);
        max-width: 1000px;
        overflow: auto;
    }
    /* Sidebar width toggle button */
    .sidebar-toggle-btn button {
        width: 36px !important;
        height: 36px !important;
        min-height: 36px !important;
        padding: 0 !important;
        border-radius: 8px !important;
        background-color: rgba(255,255,255,0.15) !important;
        border: 1px solid rgba(255,255,255,0.3) !important;
        color: #FFFFFF !important;
        font-size: 18px !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
    }
    .sidebar-toggle-btn button:hover {
        background-color: rgba(41,181,232,0.4) !important;
        border-color: #29B5E8 !important;
    }
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
        color: #FFFFFF;
    }
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h2 {
        color: #FFFFFF;
    }
    /* Sidebar labels and text white */
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] p {
        color: rgba(255,255,255,0.85) !important;
    }
    /* Sidebar selectbox styling */
    [data-testid="stSidebar"] [data-testid="stSelectbox"] > div > div {
        background-color: rgba(255,255,255,0.1);
        border: 1px solid rgba(255,255,255,0.2);
        color: #FFFFFF;
        border-radius: 8px;
    }
    /* Sidebar expander styling */
    [data-testid="stSidebar"] [data-testid="stExpander"] {
        background-color: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 10px;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] summary {
        color: #FFFFFF !important;
        font-weight: 600;
        font-size: 14px !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] summary span {
        color: #FFFFFF !important;
        font-size: 14px !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] svg {
        fill: rgba(255,255,255,0.7);
    }
    /* Sidebar button styling */
    [data-testid="stSidebar"] button {
        background-color: rgba(255,255,255,0.1) !important;
        border: 1px solid rgba(255,255,255,0.25) !important;
        color: #FFFFFF !important;
        font-size: 14px !important;
        border-radius: 8px !important;
        transition: all 0.25s ease !important;
    }
    [data-testid="stSidebar"] button:hover {
        background-color: rgba(41,181,232,0.3) !important;
        border-color: #29B5E8 !important;
    }
    /* Sidebar divider */
    [data-testid="stSidebar"] hr {
        border-color: rgba(255,255,255,0.15);
    }
    /* Sidebar inline code blocks (account details etc.) */
    [data-testid="stSidebar"] code {
        background-color: rgba(0,0,0,0.3) !important;
        color: #29B5E8 !important;
        border: none !important;
        border-radius: 6px;
    }
    [data-testid="stSidebar"] pre:not(.yaml-viewer) {
        background-color: rgba(0,0,0,0.3) !important;
        border-radius: 6px;
    }
    /* YAML viewer inside expander — GitHub Light theme */
    [data-testid="stSidebar"] [data-testid="stExpander"] pre,
    [data-testid="stSidebar"] [data-testid="stExpander"] code {
        background-color: #F6F8FA !important;
        color: #24292e !important;
        border-radius: 8px !important;
        border: 1px solid #e1e4e8 !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] code span {
        color: inherit !important;
    }
    /* YAML token colors on light background */
    [data-testid="stSidebar"] [data-testid="stExpander"] .token.key { color: #005cc5 !important; }
    [data-testid="stSidebar"] [data-testid="stExpander"] .token.string { color: #032f62 !important; }
    [data-testid="stSidebar"] [data-testid="stExpander"] .token.boolean { color: #d73a49 !important; }
    [data-testid="stSidebar"] [data-testid="stExpander"] .token.number { color: #005cc5 !important; }
    [data-testid="stSidebar"] [data-testid="stExpander"] .token.comment { color: #6a737d !important; }
    [data-testid="stSidebar"] [data-testid="stExpander"] .token.punctuation { color: #24292e !important; }
    [data-testid="stSidebar"] [data-testid="stExpander"] .token.atrule { color: #005cc5 !important; }
    [data-testid="stSidebar"] [data-testid="stExpander"] .token.important { color: #d73a49 !important; }

    /* Snowflake accent on analyst chat bubbles */
    [data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) {
        background-color: #F0F8FF;
        border-left: 3px solid #29B5E8;
    }

    /* Suggestion buttons — outline style */
    [data-testid="stChatMessage"] button[kind="secondary"] {
        border: 1px solid #29B5E8;
        color: #11567F;
        background-color: white;
        border-radius: 8px;
        transition: all 0.2s;
    }
    [data-testid="stChatMessage"] button[kind="secondary"]:hover {
        background-color: #E8F7FC;
        border-color: #11567F;
    }

    /* Title accent — only for h1 outside the hero banner */
    .main [data-testid="stMarkdownContainer"] > h1 {
        color: #11567F !important;
    }
    .hero-banner h1 {
        color: #FFFFFF !important;
    }

    /* Expander headers */
    [data-testid="stExpander"] summary {
        font-weight: 600;
        color: #11567F;
    }

    /* Feedback popover */
    [data-testid="stPopover"] {
        border: 1px solid #29B5E8;
    }

    /* Styled data tables — Consolas monospace */
    .styled-table {
        width: 100%;
        border-collapse: collapse;
        font-family: Arial, Helvetica, sans-serif !important;
        font-size: 14px;
        margin: 8px 0;
    }
    .styled-table thead th {
        background-color: #F0F2F6;
        color: #11567F;
        font-weight: 700;
        text-align: left;
        padding: 10px 14px;
        border-bottom: 2px solid #29B5E8;
        position: sticky;
        top: 0;
    }
    .styled-table tbody td {
        padding: 8px 14px;
        border-bottom: 1px solid #E8E8E8;
    }
    .styled-table tbody tr:hover {
        background-color: #F0F8FF;
    }
    .styled-table tbody tr:nth-child(even) {
        background-color: #FAFAFA;
    }
    .table-container {
        max-height: 400px;
        overflow-y: auto;
        border: 1px solid #E8E8E8;
        border-radius: 6px;
    }

    /* Thicker chat input bar with equal padding */
    [data-testid="stChatInput"] textarea {
        min-height: 72px !important;
        font-size: 16px !important;
        padding: 30px !important;
    }
    [data-testid="stChatInput"] {
        padding-top: 10px;
        padding-bottom: 10px;
    }
</style>
"""


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------
def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Create or retrieve a cached Snowflake connection."""
    if "CONN" not in st.session_state or st.session_state.CONN is None:
        st.session_state.CONN = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
    return st.session_state.CONN


# ---------------------------------------------------------------------------
# Session state helpers
# ---------------------------------------------------------------------------
def reset_session_state():
    """Reset conversation and UI state."""
    st.session_state.messages = []
    st.session_state.active_suggestion = None
    st.session_state.warnings = []
    st.session_state.form_submitted = {}
    st.session_state.status = "Interpreting question"
    st.session_state.error = None
    st.session_state.last_suggestions = []


# ---------------------------------------------------------------------------
# API functions
# ---------------------------------------------------------------------------
def get_conversation_history() -> List[Dict[str, Any]]:
    """Convert session messages to the Cortex Analyst API format."""
    history: List[Dict[str, Any]] = []
    for msg in st.session_state.messages:
        role = "analyst" if msg["role"] == "analyst" else "user"
        # Extract text content from structured content
        text_parts = []
        for item in msg.get("content", []):
            if isinstance(item, dict) and item.get("type") == "text":
                text_parts.append(item["text"])
            elif isinstance(item, str):
                text_parts.append(item)
        text_content = "\n".join(text_parts)
        history.append({
            "role": role,
            "content": [{"type": "text", "text": text_content}],
        })
    return history


def send_message() -> requests.Response:
    """Send conversation history to the Cortex Analyst API and return a streaming response."""
    conn = get_snowflake_connection()
    request_body = {
        "messages": get_conversation_history(),
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
        "stream": True,
    }
    resp = requests.post(
        url=f"https://{SNOWFLAKE_HOST}{API_ENDPOINT}",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{conn.rest.token}"',
            "Content-Type": "application/json",
        },
        stream=True,
    )
    if resp.status_code < 400:
        return resp
    else:
        raise Exception(f"Failed request with status {resp.status_code}: {resp.text}")


def stream(events: Iterator[sseclient.Event]) -> Generator[Any, Any, Any]:
    """Parse SSE events and yield display content."""
    prev_index = -1
    prev_type = ""
    prev_suggestion_index = -1
    while True:
        event = next(events, None)
        if not event:
            return
        data = json.loads(event.data)
        new_content_block = (
            event.event != "message.content.delta" or data["index"] != prev_index
        )

        if prev_type == "sql" and new_content_block:
            yield "\n```\n\n"

        if event.event == "message.content.delta":
            dtype = data["type"]
            if dtype == "sql":
                if new_content_block:
                    yield "```sql\n"
                yield data["statement_delta"]
            elif dtype == "text":
                yield data["text_delta"]
            elif dtype == "suggestions":
                suggestion_delta = data["suggestions_delta"]
                idx = suggestion_delta["index"]
                while len(st.session_state.last_suggestions) <= idx:
                    st.session_state.last_suggestions.append("")
                st.session_state.last_suggestions[idx] += suggestion_delta[
                    "suggestion_delta"
                ]
                if new_content_block:
                    yield "\nHere are some example questions you could ask:\n\n"
                    yield "\n- "
                elif prev_suggestion_index != idx:
                    yield "\n- "
                yield suggestion_delta["suggestion_delta"]
                prev_suggestion_index = idx
            prev_index = data["index"]
            prev_type = data["type"]
        elif event.event == "status":
            st.session_state.status = data["status_message"]
            return
        elif event.event == "error":
            st.session_state.error = data
            return


def submit_feedback(
    request_id: str, positive: bool, feedback_message: str
) -> Optional[str]:
    """Submit feedback for a generated SQL query. Returns error message or None."""
    conn = get_snowflake_connection()
    resp = requests.post(
        url=f"https://{SNOWFLAKE_HOST}{FEEDBACK_API_ENDPOINT}",
        json={
            "request_id": request_id,
            "positive": positive,
            "feedback_message": feedback_message,
        },
        headers={
            "Authorization": f'Snowflake Token="{conn.rest.token}"',
            "Content-Type": "application/json",
        },
    )
    if resp.status_code == 200:
        return None
    parsed = resp.json()
    return (
        f"Feedback API error (status {resp.status_code}): "
        f"{parsed.get('message', resp.text)}"
    )


# ---------------------------------------------------------------------------
# Display functions
# ---------------------------------------------------------------------------
def render_styled_table(df: pd.DataFrame) -> None:
    """Render a DataFrame as a styled HTML table with Arial font."""
    font = "font-family: Arial, Helvetica, sans-serif;"
    html = f'<div class="table-container"><table class="styled-table" style="{font}"><thead><tr>'
    for col in df.columns:
        html += f'<th style="{font}">{col}</th>'
    html += "</tr></thead><tbody>"
    for _, row in df.iterrows():
        html += "<tr>"
        for val in row:
            html += f'<td style="{font}">{val}</td>'
        html += "</tr>"
    html += "</tbody></table></div>"
    st.markdown(html, unsafe_allow_html=True)


def display_charts_tab(df: pd.DataFrame, message_index: int) -> None:
    """Render interactive chart builder with axis and type selection."""
    if len(df.columns) >= 2:
        all_cols = list(df.columns)
        col1, col2 = st.columns(2)
        x_col = col1.selectbox(
            "X axis", all_cols, key=f"x_col_select_{message_index}"
        )
        remaining = [c for c in all_cols if c != x_col]
        y_col = col2.selectbox(
            "Y axis", remaining, key=f"y_col_select_{message_index}"
        )
        chart_type = st.selectbox(
            "Select chart type",
            options=["Line Chart", "Bar Chart"],
            key=f"chart_type_{message_index}",
        )
        chart_data = df.set_index(x_col)[y_col]
        if chart_type == "Line Chart":
            st.line_chart(chart_data)
        else:
            st.bar_chart(chart_data)
    else:
        st.write("At least 2 columns are required to draw a chart.")


def display_sql_query(
    sql: str,
    message_index: int,
    request_id: Optional[str] = None,
    confidence: Optional[dict] = None,
) -> None:
    """Display SQL, execute it, and show results in Data/Chart tabs."""
    with st.expander("SQL Query", expanded=False):
        st.code(sql, language="sql")
        if confidence and confidence.get("verified_query_used"):
            vq = confidence["verified_query_used"]
            with st.popover("Verified Query Used"):
                st.text(f"Name: {vq.get('name', 'N/A')}")
                st.text(f"Question: {vq.get('question', 'N/A')}")
                st.text(f"Verified by: {vq.get('verified_by', 'N/A')}")
                if vq.get("sql"):
                    st.code(vq["sql"], language="sql", wrap_lines=True)

    with st.expander("Results", expanded=True):
        with st.spinner("Running SQL..."):
            try:
                df = pd.read_sql(sql, get_snowflake_connection())
            except Exception as e:
                st.error(f"Could not execute generated SQL query. Error: {e}")
                return
        if df.empty:
            st.write("Query returned no data.")
        else:
            data_tab, chart_tab = st.tabs(["Data", "Chart"])
            with data_tab:
                render_styled_table(df)
            with chart_tab:
                display_charts_tab(df, message_index)

    if request_id:
        display_feedback_section(request_id)


def display_feedback_section(request_id: str) -> None:
    """Render a feedback popover for a given request."""
    with st.popover("Query Feedback"):
        if request_id not in st.session_state.form_submitted:
            with st.form(f"feedback_form_{request_id}", clear_on_submit=True):
                positive = st.radio(
                    "Rate the generated SQL",
                    options=["👍", "👎"],
                    horizontal=True,
                )
                feedback_message = st.text_input("Optional feedback message")
                submitted = st.form_submit_button("Submit")
                if submitted:
                    is_positive = positive == "👍"
                    err_msg = submit_feedback(
                        request_id, is_positive, feedback_message
                    )
                    st.session_state.form_submitted[request_id] = {"error": err_msg}
                    st.rerun()
        elif st.session_state.form_submitted[request_id]["error"] is None:
            st.success("Feedback submitted", icon="✅")
        else:
            st.error(st.session_state.form_submitted[request_id]["error"])


def display_message(
    content: List[Dict[str, Any]],
    message_index: int,
    request_id: Optional[str] = None,
) -> None:
    """Render a single message's content items (text, sql, suggestions)."""
    for item in content:
        if not isinstance(item, dict):
            continue
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            for si, suggestion in enumerate(item["suggestions"]):
                if st.button(
                    suggestion,
                    key=f"suggestion_{message_index}_{si}",
                ):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            display_sql_query(
                item["statement"],
                message_index,
                request_id,
                item.get("confidence"),
            )


def display_conversation() -> None:
    """Render the full conversation history."""
    for idx, message in enumerate(st.session_state.messages):
        role = "assistant" if message["role"] == "analyst" else "user"
        with st.chat_message(role):
            if message["role"] == "analyst":
                display_message(message["content"], idx, message.get("request_id"))
            else:
                display_message(message["content"], idx)


# ---------------------------------------------------------------------------
# Message processing (streaming)
# ---------------------------------------------------------------------------
def process_message(prompt: str) -> None:
    """Process a user prompt: send to API, stream response, execute SQL, store history."""
    st.session_state.warnings = []
    st.session_state.last_suggestions = []

    user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
    }
    st.session_state.messages.append(user_message)

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Sending request..."):
            response = send_message()

        request_id = response.headers.get("X-Snowflake-Request-Id")
        events = sseclient.SSEClient(response).events()

        accumulated_content: List[Any] = []

        while st.session_state.status.lower() != "done":
            with st.spinner(st.session_state.status):
                written_content = st.write_stream(stream(events))
                accumulated_content.append(written_content)

            if st.session_state.error:
                st.error(
                    f"Error while processing request:\n{st.session_state.error}",
                    icon="🚨",
                )
                st.session_state.error = None
                st.session_state.status = "Interpreting question"
                st.session_state.messages.pop()
                return

            # Extract and execute SQL blocks
            sql_blocks = re.findall(
                r"```sql\s*(.*?)\s*```", written_content, re.DOTALL | re.IGNORECASE
            )
            if sql_blocks:
                for sql_query in sql_blocks:
                    with st.spinner("Executing query..."):
                        try:
                            df = pd.read_sql(sql_query, get_snowflake_connection())
                            display_df_inline(df)
                        except Exception as e:
                            st.error(f"Query execution error: {e}")

        # Build structured content for history replay
        content_items: List[Dict[str, Any]] = []
        full_text = "\n".join(
            [c for c in accumulated_content if isinstance(c, str)]
        )

        # Extract SQL from accumulated text
        sql_matches = re.findall(
            r"```sql\s*(.*?)\s*```", full_text, re.DOTALL | re.IGNORECASE
        )
        # Remove SQL blocks from text for the text content item
        clean_text = re.sub(
            r"```sql\s*.*?\s*```", "", full_text, flags=re.DOTALL | re.IGNORECASE
        ).strip()
        # Remove suggestion text (rendered as buttons instead)
        clean_text = re.sub(
            r"\nHere are some example questions you could ask:\n.*",
            "",
            clean_text,
            flags=re.DOTALL,
        ).strip()

        if clean_text:
            content_items.append({"type": "text", "text": clean_text})
        for sql in sql_matches:
            content_items.append({"type": "sql", "statement": sql.strip()})
        if st.session_state.last_suggestions:
            content_items.append({
                "type": "suggestions",
                "suggestions": st.session_state.last_suggestions,
            })
            # Render suggestion buttons immediately
            for si, s in enumerate(st.session_state.last_suggestions):
                if st.button(s, key=f"sug_live_{si}"):
                    st.session_state.active_suggestion = s

        st.session_state.status = "Interpreting question"
        st.session_state.messages.append({
            "role": "analyst",
            "content": content_items,
            "request_id": request_id,
        })


def display_df_inline(df: pd.DataFrame) -> None:
    """Quick inline display of a dataframe with basic charts."""
    if df.empty:
        st.write("Query returned no data.")
        return
    if len(df.index) > 1:
        data_tab, line_tab, bar_tab = st.tabs(["Data", "Line Chart", "Bar Chart"])
        with data_tab:
            render_styled_table(df)
        chart_df = df.set_index(df.columns[0]) if len(df.columns) > 1 else df
        with line_tab:
            st.line_chart(chart_df)
        with bar_tab:
            st.bar_chart(chart_df)
    else:
        render_styled_table(df)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
def load_yaml_content(model_path: str) -> str:
    """Load the YAML file content for display in the sidebar."""
    filename = model_path.split("/")[-1]
    local_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "semantic_models", filename
    )
    try:
        with open(local_path, "r") as f:
            return f.read()
    except FileNotFoundError:
        return f"File not found: {local_path}"


def show_sidebar() -> None:
    """Render the sidebar with model selector, YAML viewer, and clear chat button."""
    with st.sidebar:
        # Sidebar width toggle
        if "sidebar_expanded" not in st.session_state:
            st.session_state.sidebar_expanded = False

        is_expanded = st.session_state.sidebar_expanded
        sidebar_width = 600 if is_expanded else 400
        arrow = "◀" if is_expanded else "▶"

        # Inject dynamic width CSS
        st.markdown(
            f"""
            <style>
                [data-testid="stSidebar"] {{
                    min-width: {sidebar_width}px !important;
                    max-width: 1000px !important;
                }}
            </style>
            """,
            unsafe_allow_html=True,
        )

        # Toggle button right-aligned at top
        st.markdown(
            '<div style="display: flex; justify-content: flex-end; margin-bottom: 4px;">',
            unsafe_allow_html=True,
        )
        st.markdown('<div class="sidebar-toggle-btn">', unsafe_allow_html=True)
        if st.button(arrow, key="sidebar_toggle"):
            st.session_state.sidebar_expanded = not st.session_state.sidebar_expanded
            st.rerun()
        st.markdown('</div></div>', unsafe_allow_html=True)

        # Logo + branding header
        st.markdown(
            """
            <div style="
                display: flex; align-items: center; gap: 12px;
                padding: 8px 0 20px 0;
                border-bottom: 1px solid rgba(255,255,255,0.12);
                margin-bottom: 24px;
            ">
                <div style="
                    width: 40px; height: 40px;
                    background: rgba(41,181,232,0.15);
                    border-radius: 10px;
                    display: flex; align-items: center; justify-content: center;
                ">
                    <span style="font-size: 22px;">&#10052;</span>
                </div>
                <div>
                    <div style="
                        font-family: Arial, Helvetica, sans-serif;
                        font-size: 18px; font-weight: 700; color: #FFFFFF;
                        line-height: 1.2;
                    ">Cortex Analyst</div>
                    <div style="
                        font-family: Arial, Helvetica, sans-serif;
                        font-size: 11px; color: rgba(255,255,255,0.5);
                        letter-spacing: 0.5px; text-transform: uppercase;
                    ">Conversational Analytics</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Section: Model Configuration
        st.markdown(
            """
            <div style="
                font-family: Arial, Helvetica, sans-serif;
                font-size: 12px; font-weight: 700; color: #FFFFFF;
                letter-spacing: 1.5px; text-transform: uppercase;
                margin-bottom: 12px;
            ">CONFIGURATION</div>
            """,
            unsafe_allow_html=True,
        )

        # Hidden selectbox to maintain session state
        if "selected_semantic_model_path" not in st.session_state:
            st.session_state.selected_semantic_model_path = AVAILABLE_SEMANTIC_MODELS[0]

        # YAML file viewer
        with st.expander("View Semantic Model (YAML)", expanded=False):
            yaml_content = load_yaml_content(
                st.session_state.get(
                    "selected_semantic_model_path", AVAILABLE_SEMANTIC_MODELS[0]
                )
            )
            st.code(yaml_content, language="yaml")

        st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

        # Section: Actions
        st.markdown(
            """
            <div style="
                font-family: Arial, Helvetica, sans-serif;
                font-size: 12px; font-weight: 700; color: #FFFFFF;
                letter-spacing: 1.5px; text-transform: uppercase;
                margin-bottom: 12px;
            ">ACTIONS</div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("Clear Chat History", use_container_width=True):
            reset_session_state()
            st.rerun()

        # Spacer
        st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)

        # Section: Connection
        st.markdown(
            """
            <div style="
                font-family: Arial, Helvetica, sans-serif;
                font-size: 12px; font-weight: 700; color: #FFFFFF;
                letter-spacing: 1.5px; text-transform: uppercase;
                margin-bottom: 12px;
            ">CONNECTION</div>
            """,
            unsafe_allow_html=True,
        )

        # Connection status badge
        st.markdown(
            """
            <div style="
                display: flex; align-items: center; gap: 8px;
                padding: 10px 14px;
                background: rgba(46,204,113,0.12);
                border: 1px solid rgba(46,204,113,0.3);
                border-radius: 8px;
                margin-bottom: 12px;
            ">
                <div style="
                    width: 8px; height: 8px;
                    background: #2ECC71;
                    border-radius: 50%;
                    box-shadow: 0 0 6px rgba(46,204,113,0.5);
                "></div>
                <span style="
                    font-family: Arial, Helvetica, sans-serif;
                    font-size: 12px; font-weight: 600; color: rgba(255,255,255,0.85);
                ">Connected to Snowflake</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        with st.expander("Account Details", expanded=False):
            account_info = [
                ("Account", SNOWFLAKE_ACCOUNT or "N/A"),
                ("User", SNOWFLAKE_USER or "N/A"),
                ("Role", SNOWFLAKE_ROLE or "N/A"),
                ("Warehouse", SNOWFLAKE_WAREHOUSE or "N/A"),
                ("Database", SNOWFLAKE_DATABASE or "N/A"),
                ("Schema", SNOWFLAKE_SCHEMA or "N/A"),
            ]
            info_html = ""
            for label, value in account_info:
                info_html += f"""
                <div style="
                    display: flex; justify-content: space-between; align-items: center;
                    padding: 6px 0;
                    border-bottom: 1px solid rgba(255,255,255,0.06);
                    font-family: Arial, Helvetica, sans-serif;
                ">
                    <span style="font-size: 11px; color: rgba(255,255,255,0.5); text-transform: uppercase; letter-spacing: 0.5px;">{label}</span>
                    <span style="font-size: 12px; color: #29B5E8; font-weight: 600;">{value}</span>
                </div>
                """
            st.markdown(info_html, unsafe_allow_html=True)

        # Footer
        st.markdown(
            """
            <div style="
                margin-top: 32px;
                padding-top: 16px;
                border-top: 1px solid rgba(255,255,255,0.08);
                text-align: center;
            ">
                <span style="
                    font-family: Arial, Helvetica, sans-serif;
                    font-size: 10px; color: rgba(255,255,255,0.3);
                    letter-spacing: 0.5px;
                ">Powered by Snowflake Cortex</span>
                <br>
                <span style="
                    font-family: Arial, Helvetica, sans-serif;
                    font-size: 10px; color: rgba(255,255,255,0.3);
                    letter-spacing: 0.5px;
                ">Deployed by Mendelsohn Chan</span>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    st.set_page_config(
        page_title="Cortex Analyst",
        page_icon="❄️",
        layout="wide",
    )
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    # Validate config
    if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD]):
        st.error(
            "Missing Snowflake credentials. "
            "Please create a `.env` file from `.env.example` and fill in your credentials."
        )
        st.stop()

    get_snowflake_connection()
    show_sidebar()

    # Dark blue hero header
    st.markdown(
        """
        <div class="hero-banner" style="
            background: linear-gradient(135deg, #11567F 0%, #1B2A4A 50%, #0D1B2A 100%);
            padding: 40px 48px;
            border-radius: 12px;
            margin-bottom: 24px;
            position: relative;
            overflow: hidden;
        ">
            <!-- Subtle dot pattern overlay -->
            <div style="
                position: absolute; top: 0; left: 0; right: 0; bottom: 0;
                background-image: radial-gradient(rgba(255,255,255,0.08) 1px, transparent 1px);
                background-size: 20px 20px;
                pointer-events: none;
            "></div>
            <div style="position: relative; z-index: 1;">
                <span style="
                    display: inline-block;
                    background-color: #29B5E8;
                    color: white;
                    font-family: Arial, Helvetica, sans-serif;
                    font-size: 11px;
                    font-weight: 700;
                    letter-spacing: 1.2px;
                    text-transform: uppercase;
                    padding: 4px 12px;
                    border-radius: 4px;
                    margin-bottom: 16px;
                ">Certified Solution</span>
                <h1 style="
                    color: #FFFFFF !important;
                    font-family: Arial, Helvetica, sans-serif !important;
                    font-size: 36px !important;
                    font-weight: 800 !important;
                    line-height: 1.2 !important;
                    margin: 12px 0 16px 0 !important;
                    padding: 0 !important;
                ">Snowflake Cortex Analyst</h1>
                <div style="display: flex; align-items: center; gap: 24px;">
                    <span style="
                        color: #FFFFFF;
                        font-family: Arial, Helvetica, sans-serif;
                        font-size: 14px;
                        display: flex; align-items: center; gap: 6px;
                    ">&#128196; Cortex Analyst</span>
                    <span style="
                        color: #FFFFFF;
                        font-family: Arial, Helvetica, sans-serif;
                        font-size: 14px;
                        display: flex; align-items: center; gap: 6px;
                    ">&#128100; Snowflake Developer Community</span>
                    <a href="https://www.snowflake.com/en/developers/guides/getting-started-with-cortex-analyst/"
                       target="_blank"
                       style="
                        color: #FFFFFF;
                        font-family: Arial, Helvetica, sans-serif;
                        font-size: 14px;
                        display: flex; align-items: center; gap: 6px;
                        text-decoration: underline;
                    ">&#128279; Source URL</a>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Database tables info
    with st.expander("Tables in Semantic Model", expanded=False):
        try:
            conn = get_snowflake_connection()
            table_info_df = pd.read_sql(
                """
                SELECT
                    TABLE_NAME AS "NAME",
                    CASE WHEN TABLE_NAME = 'DAILY_REVENUE' THEN 'Fact Table' ELSE 'Dimension Table' END AS "TYPE",
                    '🥇 GOLD-LAYER' AS "CLASSIFICATION",
                    'MENDELSOHN NEIL CHAN' AS "OWNER",
                    ROW_COUNT AS "ROWS",
                    ROUND(BYTES / 1024, 1) || 'KB' AS "BYTES"
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
                ORDER BY TABLE_NAME
                """,
                conn,
            )
            render_styled_table(table_info_df)
        except Exception as e:
            st.error(f"Could not fetch table info: {e}")

    if "messages" not in st.session_state:
        reset_session_state()

    display_conversation()

    # Handle suggestion clicks
    if st.session_state.get("active_suggestion"):
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_message(suggestion)

    # Handle chat input
    if user_input := st.chat_input("What is your question?"):
        process_message(user_input)


if __name__ == "__main__":
    main()
