
import streamlit as st
import pandas as pd
import altair as alt
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.snowflake_client import get_snowflake_client
from utils.formatters import format_bytes
from utils.styles import apply_global_styles, render_sidebar, COLORS
from utils.auth import verify_page_access

st.set_page_config(
    page_title="Data Observability | Snowflake Ops",
    page_icon="👁️",
    layout="wide"
)

# Apply styles
apply_global_styles()
render_sidebar()

# Verify Access
verify_page_access('ADMIN')

st.title("👁️ Data Observability")
st.markdown("*Deep dive into data access patterns, lineage, and asset lifecycle.*")

client = get_snowflake_client()
if not client.session:
    st.error("⚠️ Not connected to Snowflake")
    st.stop()


def main():
    tab_access, tab_cold, tab_privacy, tab_quality = st.tabs([
        "🕸️ Access Patterns", 
        "🧊 Cold Assets", 
        "🛡️ Privacy Guard",
        "✅ Freshness & Quality"
    ])
    
    with tab_access:
        render_access_history(client)
        
    with tab_cold:
        render_cold_assets(client)
        
    with tab_privacy:
        render_privacy_guard(client)
        
    with tab_quality:
        render_quality_tab(client)


def render_access_history(client):
    """Show data access patterns — who accessed what, when, and how often."""
    st.markdown("### 🕸️ Data Access Patterns")
    st.caption("*Track which tables are accessed most, by whom, and identify stale or hot assets.*")
    
    try:
        access_query = """
        SELECT 
            DIRECT_OBJECTS_ACCESSED[0]:objectName::STRING AS TABLE_NAME,
            USER_NAME,
            COUNT(*) AS ACCESS_COUNT,
            MAX(QUERY_START_TIME) AS LAST_ACCESSED,
            COUNT(DISTINCT QUERY_ID) AS UNIQUE_QUERIES
        FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
        WHERE QUERY_START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
            AND DIRECT_OBJECTS_ACCESSED IS NOT NULL
            AND ARRAY_SIZE(DIRECT_OBJECTS_ACCESSED) > 0
        GROUP BY 1, 2
        ORDER BY ACCESS_COUNT DESC
        LIMIT 200
        """
        access_df = client.execute_query(access_query)
        
        if not access_df.empty:
            # Summary metrics
            m1, m2, m3 = st.columns(3)
            with m1:
                unique_tables = access_df['TABLE_NAME'].nunique() if 'TABLE_NAME' in access_df.columns else 0
                st.metric("Unique Tables Accessed", unique_tables)
            with m2:
                unique_users = access_df['USER_NAME'].nunique() if 'USER_NAME' in access_df.columns else 0
                st.metric("Users With Access", unique_users)
            with m3:
                total_queries = access_df['UNIQUE_QUERIES'].sum() if 'UNIQUE_QUERIES' in access_df.columns else 0
                st.metric("Total Queries", int(total_queries))
            
            st.dataframe(
                access_df, use_container_width=True, hide_index=True,
                column_config={
                    "ACCESS_COUNT": st.column_config.NumberColumn("Accesses"),
                    "UNIQUE_QUERIES": st.column_config.NumberColumn("Unique Queries"),
                    "LAST_ACCESSED": st.column_config.DatetimeColumn("Last Access", format="D MMM HH:mm")
                }
            )
        else:
            st.info("No access history data available. Requires Enterprise Edition with ACCESS_HISTORY enabled.")
    except Exception as e:
        st.warning(f"Access History requires Enterprise Edition: {e}")


def render_cold_assets(client):
    """Identify tables and schemas that haven't been accessed or modified recently."""
    st.markdown("### 🧊 Cold / Unused Assets")
    st.caption("*Find tables that haven't been touched in weeks — candidates for archival or deletion.*")
    
    cold_days = st.slider("Stale Threshold (days)", 7, 90, 30, key="cold_threshold",
                           help="Tables not modified in this many days are flagged")
    
    try:
        cold_query = f"""
        SELECT 
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA,
            TABLE_NAME,
            ROW_COUNT,
            ROUND(BYTES / POWER(1024, 2), 2) AS SIZE_MB,
            LAST_ALTERED,
            CREATED,
            TIMEDIFF(day, LAST_ALTERED, CURRENT_TIMESTAMP()) AS DAYS_SINCE_MODIFIED,
            CASE 
                WHEN TIMEDIFF(day, LAST_ALTERED, CURRENT_TIMESTAMP()) > {cold_days * 2} THEN '🔴 Frozen'
                WHEN TIMEDIFF(day, LAST_ALTERED, CURRENT_TIMESTAMP()) > {cold_days} THEN '🟡 Cold' 
                ELSE '🟢 Active'
            END AS STATUS
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
        WHERE DELETED IS NULL
            AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY DAYS_SINCE_MODIFIED DESC
        LIMIT 200
        """
        cold_df = client.execute_query(cold_query)
        
        if not cold_df.empty:
            # Counts
            frozen_count = len(cold_df[cold_df['STATUS'].str.contains('Frozen')]) if 'STATUS' in cold_df.columns else 0
            cold_count = len(cold_df[cold_df['STATUS'].str.contains('Cold')]) if 'STATUS' in cold_df.columns else 0
            
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.metric("Total Tables", len(cold_df))
            with m2:
                st.metric("🔴 Frozen", frozen_count)
            with m3:
                st.metric("🟡 Cold", cold_count)
            with m4:
                cold_storage_mb = cold_df[cold_df['STATUS'] != '🟢 Active']['SIZE_MB'].sum() if 'SIZE_MB' in cold_df.columns else 0
                st.metric("Cold Storage", f"{cold_storage_mb:.1f} MB")
            
            st.dataframe(
                cold_df, use_container_width=True, hide_index=True,
                column_config={
                    "SIZE_MB": st.column_config.NumberColumn("Size (MB)", format="%.2f"),
                    "DAYS_SINCE_MODIFIED": st.column_config.NumberColumn("Days Idle"),
                    "LAST_ALTERED": st.column_config.DatetimeColumn("Last Modified", format="D MMM YY"),
                    "CREATED": st.column_config.DatetimeColumn("Created", format="D MMM YY")
                }
            )
        else:
            st.info("No table metadata available.")
    except Exception as e:
        st.error(f"Error: {e}")


def render_privacy_guard(client):
    """Show tables/columns containing potentially sensitive data (PII detection)."""
    st.markdown("### 🛡️ Privacy Guard — PII Scanner")
    st.caption("*Identify columns that may contain personal or sensitive data based on naming conventions.*")
    
    pii_patterns = ['EMAIL', 'PHONE', 'SSN', 'ADDRESS', 'CREDIT_CARD', 'DOB', 'BIRTH', 
                     'PASSWORD', 'SECRET', 'TOKEN', 'SALARY', 'NAME', 'FIRST_NAME', 'LAST_NAME',
                     'SOCIAL', 'NATIONAL_ID', 'PASSPORT', 'LICENSE']
    
    try:
        cols_query = """
        SELECT 
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE
        FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
        WHERE DELETED IS NULL
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        LIMIT 5000
        """
        cols_df = client.execute_query(cols_query)
        
        if not cols_df.empty and 'COLUMN_NAME' in cols_df.columns:
            # Flag PII columns
            pattern_str = '|'.join(pii_patterns)
            pii_mask = cols_df['COLUMN_NAME'].str.upper().str.contains(pattern_str, na=False)
            pii_df = cols_df[pii_mask].copy()
            
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("Total Columns Scanned", len(cols_df))
            with m2:
                st.metric("🔴 Potential PII Columns", len(pii_df))
            with m3:
                pii_tables = pii_df['TABLE_NAME'].nunique() if not pii_df.empty else 0
                st.metric("Tables With PII", pii_tables)
            
            if not pii_df.empty:
                st.warning(f"⚠️ Found {len(pii_df)} columns that may contain sensitive data. Review for masking or governance policies.")
                st.dataframe(pii_df, use_container_width=True, hide_index=True)
            else:
                st.success("✅ No potentially sensitive columns detected based on naming patterns.")
        else:
            st.info("No column metadata available.")
    except Exception as e:
        st.error(f"Error scanning columns: {e}")

def render_quality_tab(client):
    """Data Quality and Freshness Monitoring"""
    st.markdown("### Data Freshness & Quality")
    st.caption("Monitor table updates and profile data quality.")
    
    # 1. Freshness Monitor (Metadata)
    st.subheader("⏱️ Freshness Monitor")
    
    try:
        query = f"""
        SELECT 
            TABLE_SCHEMA,
            TABLE_NAME,
            ROW_COUNT,
            BYTES,
            LAST_ALTERED,
            TIMEDIFF(hour, LAST_ALTERED, CURRENT_TIMESTAMP()) as HOURS_SINCE_UPDATE
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_SCHEMA != 'INFORMATION_SCHEMA'
        ORDER BY LAST_ALTERED ASC
        LIMIT 1000
        """
        tables_df = client.execute_query(query)
        
        if not tables_df.empty:
            # SLA Config
            col1, col2 = st.columns([1, 3])
            with col1:
                sla_hours = st.slider("Freshness SLA (Hours)", 1, 168, 24, help="Tables older than this will be flagged.")
            
            # Apply Logic
            tables_df['STATUS'] = tables_df['HOURS_SINCE_UPDATE'].apply(
                lambda x: '🔴 Stale' if x > sla_hours else '🟢 Fresh'
            )
            
            # Summary Metrics
            stale_count = tables_df[tables_df['HOURS_SINCE_UPDATE'] > sla_hours].shape[0]
            total_count = tables_df.shape[0]
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Tables", total_count)
            m2.metric("Stale Tables", stale_count, delta=-stale_count, delta_color="inverse")
            m3.metric("Freshness SLA", f"{sla_hours} Hours")
            
            # Display Table
            st.dataframe(
                tables_df[['STATUS', 'TABLE_SCHEMA', 'TABLE_NAME', 'HOURS_SINCE_UPDATE', 'ROW_COUNT', 'LAST_ALTERED']],
                use_container_width=True,
                column_config={
                    "STATUS": st.column_config.TextColumn("Status"),
                    "HOURS_SINCE_UPDATE": st.column_config.NumberColumn("Hours Idle", format="%.1f"),
                    "ROW_COUNT": st.column_config.NumberColumn("Rows"),
                    "LAST_ALTERED": st.column_config.DatetimeColumn("Last Updated", format="D MMM, HH:mm")
                }
            )
        else:
            st.info("No tables found or permission denied.")
            
    except Exception as e:
        st.error(f"Error fetching table metadata: {e}")
        
    st.divider()
    
    # 2. Data Profiler
    st.subheader("🔬 Quick Data Profiler")
    st.markdown("Select a table to run a basic quality scan (Access required).")
    
    col_sel1, col_sel2 = st.columns(2)
    with col_sel1:
        # Schema Selector
        schemas = client.execute_query("SHOW SCHEMAS")
        schema_list = []
        if not schemas.empty:
            schemas.columns = [c.upper() for c in schemas.columns]
            if 'NAME' in schemas.columns:
                schema_list = schemas[schemas['NAME'] != 'INFORMATION_SCHEMA']['NAME'].tolist()
        
        target_schema = st.selectbox("Select Schema", schema_list, key="dq_schema")
        
    with col_sel2:
        # Table Selector
        target_table = None
        if target_schema:
            t_df = client.execute_query(f"SHOW TABLES IN SCHEMA {target_schema}")
            if not t_df.empty:
                t_df.columns = [c.upper() for c in t_df.columns]
                table_list = t_df['NAME'].tolist() if 'NAME' in t_df.columns else []
                target_table = st.selectbox("Select Table", table_list, key="dq_table")
                
    if target_schema and target_table:
        if st.button("Run Profile", key="btn_profile"):
            with st.spinner(f"Profiling {target_schema}.{target_table}..."):
                try:
                    # 1. Get Columns
                    desc_df = client.execute_query(f"DESC TABLE {target_schema}.{target_table}")
                    cols = desc_df['name'].tolist()
                    
                    # 2. Build Profile Query (1-Pass)
                    # Limit to first 20 columns to avoid massive queries
                    scan_cols = cols[:20] 
                    
                    selects = [f"COUNT(*) as TOTAL_ROWS"]
                    for c in scan_cols:
                        selects.append(f"COUNT({c}) as COUNT_{c}")
                        selects.append(f"COUNT(DISTINCT {c}) as DISTINCT_{c}")
                    
                    query = f"SELECT {', '.join(selects)} FROM {target_schema}.{target_table}"
                    res = client.execute_query(query)
                    
                    if not res.empty:
                        row = res.iloc[0]
                        total_rows = row['TOTAL_ROWS']
                        
                        profile_data = []
                        for c in scan_cols:
                            filled = row[f'COUNT_{c}']
                            unique = row[f'DISTINCT_{c}']
                            nulls = total_rows - filled
                            null_pct = (nulls / total_rows) * 100 if total_rows > 0 else 0
                            distinct_pct = (unique / total_rows) * 100 if total_rows > 0 else 0
                            
                            profile_data.append({
                                "Column": c,
                                "Nulls": nulls,
                                "Null %": null_pct,
                                "Distinct": unique,
                                "Distinct %": distinct_pct,
                                "Completeness": 100 - null_pct
                            })
                            
                        prof_df = pd.DataFrame(profile_data)
                        
                        st.write(f"**Total Rows:** {total_rows}")
                        
                        # Display Profile
                        st.dataframe(
                            prof_df.style.background_gradient(subset=['Null %'], cmap='Reds', vmin=0, vmax=100)
                                         .background_gradient(subset=['Completeness'], cmap='Greens', vmin=0, vmax=100),
                            use_container_width=True,
                            column_config={
                                "Null %": st.column_config.NumberColumn(format="%.1f%%"),
                                "Distinct %": st.column_config.NumberColumn(format="%.1f%%"),
                                "Completeness": st.column_config.ProgressColumn("Completeness", min_value=0, max_value=100, format="%.1f%%")
                            }
                        )
                    
                except Exception as e:
                    st.error(f"Profiling failed: {e}")


# Execute main
main()
