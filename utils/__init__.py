"""
Utils package initialization
"""

from .snowflake_client import SnowflakeClient, get_snowflake_client
from .visualization_agent import VisualizationAgent
from .setup_wizard import SetupWizard, render_setup_wizard
from .data_service import get_account_metrics, get_daily_credits, get_daily_credits_by_warehouse
from .formatters import (
    format_credits,
    format_bytes,
    format_duration_ms,
    format_number,
    format_percentage,
    format_timestamp,
    format_time_ago,
    truncate_query,
    get_status_color,
    get_risk_color,
    dataframe_to_excel_bytes
)

__all__ = [
    'SnowflakeClient',
    'get_snowflake_client',
    'VisualizationAgent',
    'SetupWizard',
    'render_setup_wizard',
    'get_account_metrics',
    'get_daily_credits',
    'get_daily_credits_by_warehouse',
    'format_credits',
    'format_bytes',
    'format_duration_ms',
    'format_number',
    'format_percentage',
    'format_timestamp',
    'format_time_ago',
    'truncate_query',
    'get_status_color',
    'get_risk_color',
    'dataframe_to_excel_bytes'
]
