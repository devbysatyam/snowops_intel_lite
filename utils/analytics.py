"""
PostHog Analytics — Lite Version
=================================
Tracks anonymous feature usage to improve the product.
API key is stored in Snowflake PLATFORM_SETTINGS (inserted by setup_lite.sql).
Users can disable tracking via TELEMETRY_ENABLED setting or the Settings page toggle.
"""

import streamlit as st
import hashlib
import datetime
import logging

logger = logging.getLogger(__name__)

_posthog_client = None
_posthog_init_attempted = False


def _is_telemetry_enabled():
    """Check if telemetry is enabled in PLATFORM_SETTINGS."""
    # Session-level override (from Settings page toggle)
    if st.session_state.get("_telemetry_disabled"):
        return False
    try:
        session = st.session_state.get("snowpark_session")
        if session:
            result = session.sql(
                "SELECT SETTING_VALUE FROM APP_CONTEXT.PLATFORM_SETTINGS "
                "WHERE SETTING_KEY = 'TELEMETRY_ENABLED'"
            ).collect()
            if result and result[0][0].upper() in ('FALSE', '0', 'NO', 'OFF'):
                return False
    except Exception:
        pass
    return True


def _get_posthog():
    """Lazy-initialize PostHog. Key comes from Snowflake DB, never hardcoded."""
    global _posthog_client, _posthog_init_attempted

    if _posthog_init_attempted:
        return _posthog_client

    _posthog_init_attempted = True

    if not _is_telemetry_enabled():
        logger.info("Telemetry disabled by user")
        return None

    try:
        # Try PostHog SDK first, fallback to requests
        try:
            import posthog as ph
        except ImportError:
            ph = None

        api_key = None
        host = "https://us.i.posthog.com"

        try:
            session = st.session_state.get("snowpark_session")
            if session:
                result = session.sql(
                    "SELECT SETTING_KEY, SETTING_VALUE FROM APP_CONTEXT.PLATFORM_SETTINGS "
                    "WHERE SETTING_KEY IN ('POSTHOG_API_KEY', 'POSTHOG_HOST')"
                ).collect()
                for row in result:
                    key, val = row[0], row[1]
                    if key == "POSTHOG_API_KEY":
                        api_key = val
                    elif key == "POSTHOG_HOST":
                        host = val
        except Exception:
            pass

        if not api_key:
            logger.info("PostHog API key not in PLATFORM_SETTINGS — analytics disabled")
            return None

        if ph:
            ph.api_key = api_key
            ph.host = host
            ph.debug = False
            ph.on_error = lambda e, items: None
            _posthog_client = ph
        else:
            # Minimal fallback using requests
            _posthog_client = _RequestsFallback(api_key, host)

        logger.info("PostHog initialized")
        return _posthog_client

    except Exception as e:
        logger.warning(f"PostHog init failed: {e}")
        return None


class _RequestsFallback:
    """Minimal PostHog client using requests when SDK is unavailable."""
    def __init__(self, api_key, host):
        self.api_key = api_key
        self.host = host

    def capture(self, distinct_id, event, properties=None):
        try:
            import requests
            requests.post(
                f"{self.host}/capture/",
                json={
                    "api_key": self.api_key,
                    "distinct_id": distinct_id,
                    "event": event,
                    "properties": properties or {},
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                },
                timeout=2,
            )
        except Exception:
            pass

    def identify(self, distinct_id, properties=None):
        self.capture(distinct_id, "$identify", {"$set": properties or {}})


def _get_user_id():
    """Anonymous user ID from Snowflake session context."""
    try:
        session = st.session_state.get("snowpark_session")
        if session:
            user = session.sql("SELECT CURRENT_USER()").collect()[0][0]
            account = session.sql("SELECT CURRENT_ACCOUNT()").collect()[0][0]
            return hashlib.sha256(f"{account}:{user}".encode()).hexdigest()[:16]
    except Exception:
        pass
    return "anonymous"


def _get_context():
    """Common context properties (no PII)."""
    ctx = {"platform": "streamlit_in_snowflake", "app_version": "lite", "app": "snowops_intel_lite"}
    try:
        session = st.session_state.get("snowpark_session")
        if session:
            ctx["role"] = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
            ctx["warehouse"] = session.sql("SELECT CURRENT_WAREHOUSE()").collect()[0][0]
            try:
                ctx["edition"] = session.sql("SELECT CURRENT_EDITION()").collect()[0][0]
            except Exception:
                pass
    except Exception:
        pass
    return ctx


# ── Public API ──

def track_page_view(page_name: str, properties: dict = None):
    ph = _get_posthog()
    if not ph: return
    try:
        ph.capture(_get_user_id(), "page_view", {"page_name": page_name, **_get_context(), **(properties or {})})
    except Exception:
        pass


def track_feature_use(feature_name: str, properties: dict = None):
    ph = _get_posthog()
    if not ph: return
    try:
        ph.capture(_get_user_id(), "feature_use", {"feature": feature_name, **_get_context(), **(properties or {})})
    except Exception:
        pass


def track_export(export_type: str, page: str = "", row_count: int = 0):
    ph = _get_posthog()
    if not ph: return
    try:
        ph.capture(_get_user_id(), "data_export", {"export_type": export_type, "page": page, "row_count": row_count, **_get_context()})
    except Exception:
        pass


def track_error(error_type: str, error_message: str, page: str = ""):
    ph = _get_posthog()
    if not ph: return
    try:
        ph.capture(_get_user_id(), "error", {"error_type": error_type, "error_message": error_message[:500], "page": page, **_get_context()})
    except Exception:
        pass


def track_session_start():
    ph = _get_posthog()
    if not ph: return
    if not st.session_state.get("_posthog_session_tracked"):
        try:
            uid = _get_user_id()
            ctx = _get_context()
            ph.identify(uid, {"platform": ctx.get("platform"), "role": ctx.get("role"), "edition": ctx.get("edition")})
            ph.capture(uid, "session_start", ctx)
            st.session_state["_posthog_session_tracked"] = True
        except Exception:
            pass


def disable_telemetry():
    """Disable telemetry for the current session."""
    global _posthog_client, _posthog_init_attempted
    st.session_state["_telemetry_disabled"] = True
    _posthog_client = None
    _posthog_init_attempted = True  # Prevent re-init


def enable_telemetry():
    """Re-enable telemetry for the current session."""
    global _posthog_client, _posthog_init_attempted
    st.session_state["_telemetry_disabled"] = False
    _posthog_client = None
    _posthog_init_attempted = False  # Allow re-init
