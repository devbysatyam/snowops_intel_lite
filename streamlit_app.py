"""
SnowOps Intelligence — LITE Edition
Main Streamlit Application Entry Point
"""

import streamlit as st
import pandas as pd
import os
import sys
from datetime import datetime, timedelta

# --- PATH FIX ---
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    from utils.snowflake_client import SnowflakeClient
    from utils.styles import apply_global_styles, render_metric_card, render_page_header, COLORS
    from utils.data_service import get_account_metrics, get_daily_credits, get_daily_credits_by_warehouse
    from utils.feature_gate import render_lite_badge, render_sidebar_upgrade
except ImportError:
    st.error("Error: Could not find 'utils' folder. Please ensure it is uploaded to the same stage.")
    st.stop()

# PostHog analytics (optional — fails gracefully)
try:
    from utils.analytics import track_page_view, track_session_start, track_feature_use
    _HAS_ANALYTICS = True
except ImportError:
    _HAS_ANALYTICS = False

import time

# Page configuration
st.set_page_config(
    page_title="SnowOps Intel Lite",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply Snowflake Design System
apply_global_styles()

# ── First-Run Consent Banner ──
if not st.session_state.get("_consent_shown"):
    st.session_state["_consent_shown"] = True
    if not st.session_state.get("_consent_answered"):
        with st.container():
            st.info(
                "📊 **Anonymous Usage Telemetry** — SnowOps Intel Lite collects anonymous feature usage data "
                "to help us improve the product. No PII, query text, or cost data is ever collected. "
                "You can disable this anytime in **Settings → Telemetry**.",
                icon="ℹ️"
            )
            col1, col2, col3 = st.columns([1, 1, 4])
            with col1:
                if st.button("✅ Got it", key="consent_ok"):
                    st.session_state["_consent_answered"] = True
                    st.rerun()
            with col2:
                if st.button("❌ Disable", key="consent_no"):
                    st.session_state["_consent_answered"] = True
                    st.session_state["_telemetry_disabled"] = True
                    st.rerun()

# Track session start (PostHog)
if _HAS_ANALYTICS and st.session_state.get("_consent_answered", True):
    track_session_start()
    track_page_view("Dashboard")


def get_snowflake_client():
    """Get or create Snowflake client singleton."""
    if 'snowflake_client' not in st.session_state:
        st.session_state.snowflake_client = SnowflakeClient()
    return st.session_state.snowflake_client


def main():
    """Main dashboard page."""
    client = get_snowflake_client()

    # Sidebar
    with st.sidebar:
        render_lite_badge()
        st.markdown("### ❄️ SnowOps Intel")
        st.markdown("**Lite Edition**")
        st.caption(f"Connected: {datetime.now().strftime('%H:%M')}")
        render_sidebar_upgrade()

    if not client.session:
        st.warning("⚠️ Connecting to Snowflake...")
        st.info("If running locally, ensure your `~/.snowflake/connections.toml` or `.streamlit/secrets.toml` is configured.")
        return

    # Store session for analytics
    st.session_state["snowpark_session"] = client.session

    # ── Dashboard Header ──
    render_page_header(
        "Dashboard Overview",
        "Real-time Snowflake account health at a glance"
    )

    # ── Key Metrics ──
    try:
        col1, col2, col3, col4 = st.columns(4)
        metrics = get_account_metrics(client, days=30)

        if metrics:
            with col1:
                render_metric_card("Total Credits (30d)", f"{metrics.get('total_credits', 0):,.1f}", "credits")
            with col2:
                render_metric_card("Total Queries (30d)", f"{metrics.get('total_queries', 0):,}", "queries")
            with col3:
                render_metric_card("Active Warehouses", f"{metrics.get('warehouse_count', 0)}", "warehouses")
            with col4:
                cost = metrics.get('total_credits', 0) * 3.0
                render_metric_card("Est. Cost (30d)", f"${cost:,.0f}", "USD")
    except Exception as e:
        st.error(f"Error loading metrics: {e}")

    # ── Credit Trend Chart ──
    st.markdown("---")
    st.subheader("📈 Daily Credit Consumption")

    try:
        daily = get_daily_credits(client, days=30)
        if daily is not None and not daily.empty:
            import altair as alt
            date_col = daily.columns[0]
            credit_col = daily.columns[1]
            chart = alt.Chart(daily).mark_area(
                opacity=0.3,
                line={'color': '#29B5E8'}
            ).encode(
                x=alt.X(f'{date_col}:T', title='Date'),
                y=alt.Y(f'{credit_col}:Q', title='Credits'),
                color=alt.value('#29B5E8')
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No daily credit data available yet.")
    except Exception as e:
        st.warning(f"Could not load credit trends: {e}")

    # ── By Warehouse ──
    st.subheader("🏭 Credits by Warehouse")
    try:
        wh_data = get_daily_credits_by_warehouse(client, days=30)
        if wh_data is not None and not wh_data.empty:
            st.dataframe(wh_data.head(10), use_container_width=True)
        else:
            st.info("No warehouse data available.")
    except Exception as e:
        st.warning(f"Could not load warehouse data: {e}")


if __name__ == "__main__":
    main()
