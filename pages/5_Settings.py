"""
Settings & Configuration — Lite Edition
Includes telemetry opt-out toggle.
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.snowflake_client import SnowflakeClient
from utils.styles import apply_global_styles
from utils.feature_gate import render_lite_badge, render_sidebar_upgrade

apply_global_styles()
from utils.styles import render_sidebar
render_sidebar()

try:
    from utils.analytics import track_page_view, disable_telemetry, enable_telemetry
    _HAS_ANALYTICS = True
    track_page_view("Settings")
except Exception:
    _HAS_ANALYTICS = False


def get_snowflake_client():
    if 'snowflake_client' not in st.session_state:
        st.session_state.snowflake_client = SnowflakeClient()
    return st.session_state.snowflake_client


def main():
    with st.sidebar:
        render_lite_badge()
        render_sidebar_upgrade()

    st.title("⚙️ Settings")
    st.markdown("*Platform configuration and preferences*")

    client = get_snowflake_client()
    if not client.session:
        st.error("⚠️ Could not connect to Snowflake")
        return

    tab1, tab2, tab3 = st.tabs([
        "💲 Cost Settings",
        "📊 Telemetry",
        "ℹ️ About",
    ])

    with tab1:
        render_cost_settings(client)

    with tab2:
        render_telemetry_settings(client)

    with tab3:
        render_about()


def render_cost_settings(client):
    """Cost per credit and budget configuration."""
    st.subheader("💲 Cost Configuration")

    # Read current settings
    try:
        result = client.execute_query(
            "SELECT SETTING_KEY, SETTING_VALUE FROM APP_CONTEXT.PLATFORM_SETTINGS "
            "WHERE SETTING_KEY IN ('COST_PER_CREDIT', 'MONTHLY_BUDGET_CREDITS')"
        )
        settings = {row['SETTING_KEY']: row['SETTING_VALUE'] for _, row in result.iterrows()} if result is not None else {}
    except Exception:
        settings = {}

    col1, col2 = st.columns(2)
    with col1:
        cost_per_credit = st.number_input(
            "Cost per Credit ($)",
            value=float(settings.get('COST_PER_CREDIT', '3.00')),
            step=0.25,
            format="%.2f",
            help="Your Snowflake contract rate per credit"
        )
    with col2:
        monthly_budget = st.number_input(
            "Monthly Budget (Credits)",
            value=int(float(settings.get('MONTHLY_BUDGET_CREDITS', '1000'))),
            step=100,
            help="Monthly credit budget for alerts"
        )

    if st.button("💾 Save Cost Settings", type="primary"):
        try:
            client.execute_query(f"""
                MERGE INTO APP_CONTEXT.PLATFORM_SETTINGS t
                USING (SELECT 'COST_PER_CREDIT' AS SETTING_KEY, '{cost_per_credit}' AS SETTING_VALUE) s
                ON t.SETTING_KEY = s.SETTING_KEY
                WHEN MATCHED THEN UPDATE SET SETTING_VALUE = s.SETTING_VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (SETTING_KEY, SETTING_VALUE) VALUES (s.SETTING_KEY, s.SETTING_VALUE)
            """)
            client.execute_query(f"""
                MERGE INTO APP_CONTEXT.PLATFORM_SETTINGS t
                USING (SELECT 'MONTHLY_BUDGET_CREDITS' AS SETTING_KEY, '{monthly_budget}' AS SETTING_VALUE) s
                ON t.SETTING_KEY = s.SETTING_KEY
                WHEN MATCHED THEN UPDATE SET SETTING_VALUE = s.SETTING_VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (SETTING_KEY, SETTING_VALUE) VALUES (s.SETTING_KEY, s.SETTING_VALUE)
            """)
            st.success("✅ Settings saved!")
        except Exception as e:
            st.error(f"Error saving: {e}")


def render_telemetry_settings(client):
    """Telemetry opt-in/out toggle."""
    st.subheader("📊 Telemetry & Analytics")

    st.markdown("""
    SnowOps Intel Lite collects **anonymous usage data** to help us understand which features are most 
    valuable and improve the product. Here's exactly what we track:
    
    **✅ What we collect:**
    - Which pages/tabs you open (e.g., "Cost Overview", "Warehouse Monitoring")
    - Which Pro features you click on (helps us prioritize development)
    - Snowflake edition (Standard/Enterprise) and role
    - App version
    
    **❌ What we NEVER collect:**
    - Email, name, or company name
    - Snowflake account identifier
    - Query text or table names
    - Actual cost/credit numbers
    - IP address (PostHog strips this automatically)
    """)

    # Read current state
    try:
        result = client.execute_query(
            "SELECT SETTING_VALUE FROM APP_CONTEXT.PLATFORM_SETTINGS "
            "WHERE SETTING_KEY = 'TELEMETRY_ENABLED'"
        )
        current = result.iloc[0, 0].upper() if result is not None and len(result) > 0 else 'TRUE'
    except Exception:
        current = 'TRUE'

    is_enabled = current in ('TRUE', '1', 'YES', 'ON')
    session_disabled = st.session_state.get("_telemetry_disabled", False)

    if session_disabled:
        is_enabled = False

    new_state = st.toggle(
        "Enable anonymous telemetry",
        value=is_enabled,
        help="Toggle off to completely disable all telemetry"
    )

    if new_state != is_enabled:
        new_val = 'TRUE' if new_state else 'FALSE'
        try:
            client.execute_query(f"""
                MERGE INTO APP_CONTEXT.PLATFORM_SETTINGS t
                USING (SELECT 'TELEMETRY_ENABLED' AS SETTING_KEY, '{new_val}' AS SETTING_VALUE) s
                ON t.SETTING_KEY = s.SETTING_KEY
                WHEN MATCHED THEN UPDATE SET SETTING_VALUE = s.SETTING_VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (SETTING_KEY, SETTING_VALUE, DESCRIPTION) 
                VALUES (s.SETTING_KEY, s.SETTING_VALUE, 'Enable anonymous usage telemetry')
            """)
            if new_state:
                st.session_state["_telemetry_disabled"] = False
                if _HAS_ANALYTICS:
                    enable_telemetry()
                st.success("✅ Telemetry enabled. Thank you for helping us improve!")
            else:
                st.session_state["_telemetry_disabled"] = True
                if _HAS_ANALYTICS:
                    disable_telemetry()
                st.success("✅ Telemetry disabled. No data will be sent.")
        except Exception as e:
            st.error(f"Error updating setting: {e}")

    if not new_state:
        st.info("💡 Telemetry is disabled. No anonymous data is being sent.")


def render_about():
    """About section."""
    st.subheader("ℹ️ About SnowOps Intel Lite")
    st.markdown("""
    **SnowOps Intelligence** is an open-source Snowflake operations intelligence platform.
    
    - **Version:** Lite (Community Edition)
    - **License:** Open Source
    - **GitHub:** [snowflake_ops_intelligence](https://github.com/devbysatyam/snowflake_ops_intelligence)
    - **Built by:** [DevBySatyam](https://github.com/devbysatyam)
    
    ---
    
    **Lite includes:** Cost Overview, Warehouse Costs, Ingestion, User/Role Attribution, 
    Usage Patterns, Anomaly Detection, Basic Forecasting, Query History, Warehouse Monitoring, 
    Data Observability, and Settings.
    
    **Upgrade to Pro** for AI-powered features: Cost Guardian, Alert Builder, Warehouse Optimizer, 
    Query Optimizer, AI Analyst, BI Builder, Governance, and more.
    """)

    st.markdown(f"""
    <div style="text-align:center; margin-top:32px; color:#666; font-size:0.8rem;">
        Made with ❄️ by <a href="https://github.com/devbysatyam" target="_blank" style="color:#29B5E8;">@devbysatyam</a>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
