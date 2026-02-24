"""
Feature Gate — Lite/Pro Check & Upgrade CTA
============================================
Controls which features are available in the Lite version.
Pro-locked features show a styled upgrade card with CTA.
"""

import streamlit as st

# ── Lite Features (fully functional) ──
LITE_FEATURES = {
    "cost_overview", "warehouse_costs", "ingestion_costs",
    "by_user", "by_role", "usage_patterns", "anomaly_detection",
    "basic_forecast", "basic_query_history", "warehouse_monitoring",
    "data_observability", "settings",
}

# ── Pro Features (upgrade CTA shown) ──
PRO_FEATURES = {
    "scenario_forecasting": {
        "name": "Scenario Forecasting & Budget Runway",
        "desc": "Monte Carlo simulations, optimistic/pessimistic projections, budget runway calculator, and what-if scenario modeling.",
        "icon": "🔮",
    },
    "user_scorecard": {
        "name": "User Performance Scorecard",
        "desc": "Per-user efficiency scores (0-100), fail rate analysis, cache hit tracking, and spill detection.",
        "icon": "👤",
    },
    "cost_guardian": {
        "name": "Cost Guardian & Burst Protection",
        "desc": "Real-time burst detection with Z-Score severity, automatic warehouse suspension, and anomaly alerting.",
        "icon": "🚨",
    },
    "alert_builder": {
        "name": "Alert Builder & Notifications",
        "desc": "Custom metric triggers with Slack/Teams/PagerDuty integration, budget sentinel automation.",
        "icon": "🔔",
    },
    "warehouse_optimizer": {
        "name": "Warehouse Health Optimizer",
        "desc": "AI-driven health scoring (0-100), right-sizing recommendations, auto-suspend optimization.",
        "icon": "🏥",
    },
    "dbt_costs": {
        "name": "dbt Model Cost Tracking",
        "desc": "Per-model cost attribution, pipeline cost trends, and optimization recommendations for dbt workflows.",
        "icon": "🟧",
    },
    "deep_dive": {
        "name": "Deep Dive Query Attribution",
        "desc": "Per-query cost attribution table with full SQL inspection, compilation/execution/queue time breakdown.",
        "icon": "🕵️",
    },
    "ai_query_optimizer": {
        "name": "AI Query Optimizer",
        "desc": "Cortex AI-powered query rewriting, automatic index suggestions, and execution plan analysis.",
        "icon": "🤖",
    },
    "ai_analyst": {
        "name": "AI Analyst (Natural Language)",
        "desc": "Ask questions in plain English, get SQL + charts. Powered by Snowflake Cortex.",
        "icon": "🧠",
    },
    "ai_bi_builder": {
        "name": "AI BI Dashboard Builder",
        "desc": "Create dashboards from natural language prompts. Drag-and-drop widget editing with AI assistance.",
        "icon": "📊",
    },
    "agent_builder": {
        "name": "Autonomous Agent Builder",
        "desc": "Build and deploy AI agents that monitor, analyze, and act on your Snowflake environment 24/7.",
        "icon": "🏗️",
    },
    "governance": {
        "name": "Security & Governance",
        "desc": "CIS benchmark scoring, PII scanner, login forensics, access auditing, and compliance reporting.",
        "icon": "🔐",
    },
    "privacy_guard": {
        "name": "Privacy Guard (PII Scanner)",
        "desc": "Automatic PII column detection across all databases, masking policy recommendations.",
        "icon": "🛡️",
    },
    "monitoring": {
        "name": "24/7 Monitoring & Notifications",
        "desc": "Background task monitoring, webhook notifications, enforcement audit trail.",
        "icon": "📧",
    },
    "pipelines": {
        "name": "Pipeline Manager",
        "desc": "Monitor Snowpipe, COPY INTO operations, and data loading pipelines with cost tracking.",
        "icon": "📥",
    },
    "resource_explorer": {
        "name": "Resource Explorer",
        "desc": "Interactive resource browser with cost allocation, tag-based filtering, and usage heatmaps.",
        "icon": "🗺️",
    },
    "cluster_benchmark": {
        "name": "Cluster Key Benchmark",
        "desc": "Automated cluster key analysis, re-clustering ROI calculator, and partition depth monitoring.",
        "icon": "📐",
    },
    "workbench": {
        "name": "SQL Workbench",
        "desc": "Full-featured SQL editor with cost estimation, execution history, and AI-powered suggestions.",
        "icon": "💻",
    },
    "data_quality": {
        "name": "Data Quality Scanner",
        "desc": "Null analysis, distinct count profiling, schema drift detection, and freshness SLA monitoring.",
        "icon": "✅",
    },
    "waste_manager": {
        "name": "Waste Manager",
        "desc": "Identify unused tables, idle warehouses, redundant queries, and optimize storage costs.",
        "icon": "♻️",
    },
}

UPGRADE_URL = "https://snowops.anktechsol.com"


def is_lite_feature(feature_key: str) -> bool:
    """Check if a feature is available in Lite."""
    return feature_key in LITE_FEATURES


def render_upgrade_cta(feature_key: str):
    """Render a styled upgrade card for a Pro-locked feature."""
    info = PRO_FEATURES.get(feature_key, {
        "name": feature_key.replace("_", " ").title(),
        "desc": "This feature is available in the Pro version.",
        "icon": "🔒",
    })

    # Track in PostHog
    try:
        from utils.analytics import track_feature_use
        track_feature_use("pro_upgrade_cta_view", {"feature": feature_key})
    except Exception:
        pass

    st.markdown(f"""
    <div style="
        border: 1px solid #333;
        border-radius: 4px;
        padding: 48px 40px;
        text-align: center;
        margin: 40px auto;
        max-width: 600px;
        background: linear-gradient(135deg, rgba(41,181,232,0.03) 0%, transparent 100%);
    ">
        <div style="font-size: 3rem; margin-bottom: 16px;">{info['icon']}</div>
        <div style="font-size: 0.7rem; font-weight: 700; letter-spacing: 0.15em; text-transform: uppercase; color: #29B5E8; margin-bottom: 12px;">
            PRO FEATURE
        </div>
        <h2 style="font-size: 1.4rem; font-weight: 700; margin-bottom: 12px;">
            {info['name']}
        </h2>
        <p style="color: #888; font-size: 0.9rem; line-height: 1.6; margin-bottom: 28px;">
            {info['desc']}
        </p>
        <a href="{UPGRADE_URL}" target="_blank" style="
            display: inline-block;
            padding: 12px 32px;
            background: #29B5E8;
            color: #000;
            font-weight: 700;
            font-size: 0.85rem;
            text-decoration: none;
            border-radius: 2px;
            transition: opacity 0.2s;
        ">Upgrade to Pro →</a>
        <div style="margin-top: 16px;">
            <a href="{UPGRADE_URL}" target="_blank" style="color: #666; font-size: 0.78rem; text-decoration: none;">
                View all Pro features
            </a>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_lite_badge():
    """Render a 'LITE' badge in the sidebar."""
    st.sidebar.markdown("""
    <div style="
        display: inline-block;
        padding: 4px 12px;
        background: rgba(41,181,232,0.1);
        border: 1px solid rgba(41,181,232,0.3);
        color: #29B5E8;
        font-size: 0.65rem;
        font-weight: 700;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        border-radius: 2px;
        margin-bottom: 8px;
    ">LITE</div>
    """, unsafe_allow_html=True)


def render_sidebar_upgrade():
    """Render an upgrade prompt in the sidebar."""
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"""
    <div style="
        padding: 16px;
        background: rgba(41,181,232,0.05);
        border: 1px solid rgba(41,181,232,0.15);
        border-radius: 2px;
        text-align: center;
    ">
        <div style="font-size: 0.72rem; font-weight: 700; letter-spacing: 0.1em; color: #29B5E8; margin-bottom: 8px;">
            UNLOCK ALL FEATURES
        </div>
        <div style="font-size: 0.8rem; color: #888; margin-bottom: 12px;">
            Get AI-powered analytics, alerts, governance, and more.
        </div>
        <a href="{UPGRADE_URL}" target="_blank" style="
            display: inline-block;
            padding: 8px 20px;
            background: #29B5E8;
            color: #000;
            font-weight: 600;
            font-size: 0.78rem;
            text-decoration: none;
            border-radius: 2px;
        ">Upgrade to Pro →</a>
    </div>
    """, unsafe_allow_html=True)
