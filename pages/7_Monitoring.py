"""
Monitoring & Alerts — PRO FEATURE
Upgrade to Pro for custom alert builder, webhook notifications, and 24/7 monitoring.
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
    track_page_view("Monitoring (Pro Gate)")
except Exception:
    pass

with st.sidebar:
    render_lite_badge()
    render_sidebar_upgrade()

st.title("🔔 Monitoring & Alerts")
st.markdown("*Custom alert rules, webhook notifications, and enforcement automation*")

tab1, tab2 = st.tabs([
    "🔔 Alert Builder",
    "📧 Notifications",
])

with tab1:
    render_upgrade_cta("alert_builder")

with tab2:
    render_upgrade_cta("monitoring")
