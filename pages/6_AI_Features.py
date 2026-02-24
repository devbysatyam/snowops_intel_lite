"""
AI & Advanced Analytics — PRO FEATURES
Upgrade to Pro for AI Query Optimizer, AI Analyst, AI BI Builder, and Agent Builder.
"""

import streamlit as st
import sys, os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.styles import apply_global_styles
apply_global_styles()
from utils.styles import render_sidebar
render_sidebar()
from utils.feature_gate import render_upgrade_cta, render_lite_badge, render_sidebar_upgrade

try:
    from utils.analytics import track_page_view
    track_page_view("AI Features (Pro Gate)")
except Exception:
    pass

with st.sidebar:
    render_lite_badge()
    render_sidebar_upgrade()

st.title("🤖 AI-Powered Analytics")
st.markdown("*Cortex AI-powered query optimization, natural language analytics, and autonomous agents*")

tab1, tab2, tab3, tab4 = st.tabs([
    "🤖 AI Query Optimizer",
    "🧠 AI Analyst",
    "📊 AI BI Builder",
    "🏗️ Agent Builder",
])

with tab1:
    render_upgrade_cta("ai_query_optimizer")

with tab2:
    render_upgrade_cta("ai_analyst")

with tab3:
    render_upgrade_cta("ai_bi_builder")

with tab4:
    render_upgrade_cta("agent_builder")
