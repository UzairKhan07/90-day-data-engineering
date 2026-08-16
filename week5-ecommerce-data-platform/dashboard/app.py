# E-Commerce Analytics Dashboard
from __future__ import annotations
import os
from datetime import date
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# Config
st.set_page_config(
    page_title="E-Commerce Analytics Platform",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# DB connection
@st.cache_resource
def get_engine():
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")

    if not all([host, user, password, db]):
        raise RuntimeError("Missing Postgres env vars for Streamlit")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)
    

@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


# Sidebar filters
st.sidebar.title("🛒 E-Commerce Platform")
st.sidebar.markdown("### Filters")

# Load date range from daily summary
date_bounds = run_query(
    "SELECT MIN(sales_date) AS min_d, MAX(sales_date) AS max_d FROM gold.daily_sales_summary"
)
min_d = date_bounds["min_d"].iloc[0] if not date_bounds.empty else date(2019, 1, 1)
max_d = date_bounds["max_d"].iloc[0] if not date_bounds.empty else date(2025, 12, 31)

date_range = st.sidebar.date_input(
    "Sales date range",
    value=(min_d, max_d),
    min_value=min_d,
    max_value=max_d,
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_d, max_d

st.sidebar.markdown("---")
st.sidebar.caption("Data source: Gold layer (PostgreSQL)")
st.sidebar.caption("Week 5 – Enterprise Data Platform")

# Header
st.title("🛒 E-Commerce Analytics Platform")
st.markdown(f"**Period:** {start_date} → {end_date}")

# KPI queries
kpi_sql = f"""
SELECT
    COALESCE(SUM(total_orders), 0)              AS total_orders,
    COALESCE(SUM(net_revenue), 0)               AS net_revenue,
    COALESCE(SUM(gross_revenue), 0)             AS gross_revenue,
    COALESCE(SUM(total_returns), 0)             AS total_returns,
    COALESCE(SUM(total_refund_amount), 0)       AS total_refunds,
    COALESCE(SUM(delivered_orders), 0)          AS delivered_orders,
    COALESCE(SUM(cancelled_orders), 0)          AS cancelled_orders,
    COALESCE(SUM(delayed_orders), 0)            AS delayed_orders
FROM gold.daily_sales_summary
WHERE sales_date BETWEEN '{start_date}' AND '{end_date}'
"""
kpi = run_query(kpi_sql).iloc[0]

total_orders = int(kpi["total_orders"])
net_revenue = float(kpi["net_revenue"])
gross_revenue = float(kpi["gross_revenue"])
total_returns = int(kpi["total_returns"])
delivered = int(kpi["delivered_orders"])
cancelled = int(kpi["cancelled_orders"])
delayed = int(kpi["delayed_orders"])

aov = net_revenue / total_orders if total_orders else 0
return_rate = total_returns / total_orders if total_orders else 0
delivery_rate = delivered / total_orders if total_orders else 0

# KPI cards
c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Net Revenue", f"${net_revenue:,.0f}")
c2.metric("Total Orders", f"{total_orders:,}")
c3.metric("Avg Order Value", f"${aov:,.2f}")
c4.metric("Return Rate", f"{return_rate:.1%}")
c5.metric("Delivery Rate", f"{delivery_rate:.1%}")

st.markdown("---")

# Row 1: Revenue trend + Orders trend
col1, col2 = st.columns(2)

daily_sql = f"""
SELECT
    sales_date,
    net_revenue,
    gross_revenue,
    total_orders,
    avg_order_value,
    return_rate
FROM gold.daily_sales_summary
WHERE sales_date BETWEEN '{start_date}' AND '{end_date}'
ORDER BY sales_date
"""
daily = run_query(daily_sql)

with col1:
    st.subheader("📈 Daily Net Revenue")
    if not daily.empty:
        fig = px.line(
            daily,
            x="sales_date",
            y="net_revenue",
            labels={"sales_date": "Date", "net_revenue": "Net Revenue ($)"},
        )
        fig.update_layout(margin=dict(l=10, r=10, t=30, b=10), height=350)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for selected period.")

with col2:
    st.subheader("📦 Daily Orders")
    if not daily.empty:
        fig = px.bar(
            daily,
            x="sales_date",
            y="total_orders",
            labels={"sales_date": "Date", "total_orders": "Orders"},
        )
        fig.update_layout(margin=dict(l=10, r=10, t=30, b=10), height=350)
        st.plotly_chart(fig, use_container_width=True)


# Row 2: Top products + Return reasons
col3, col4 = st.columns(2)

top_products_sql = """
SELECT
    COALESCE(d.product_name, LEFT(p.product_id, 8)) AS product_label,
    p.product_id,
    p.product_category_name,
    p.net_revenue,
    p.times_ordered,
    p.return_rate
FROM gold.product_performance p
LEFT JOIN gold.dim_products d
       ON d.product_id = p.product_id
ORDER BY p.net_revenue DESC NULLS LAST
LIMIT 15
"""
top_products = run_query(top_products_sql)

with col3:
    st.subheader("🏆 Top 15 Products by Net Revenue")
    if not top_products.empty:
        fig = px.bar(
            top_products.sort_values("net_revenue"),
            x="net_revenue",
            y="product_label",
            color="product_category_name",
            orientation="h",
            labels={"net_revenue": "Net Revenue ($)", "product_label": "Product"},
        )
        fig.update_layout(margin=dict(l=10, r=10, t=30, b=10), height=450, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No product data.")

reasons_sql = """
SELECT return_reason, return_count, total_refund_amount, pct_of_all_returns
FROM gold.return_rate_by_reason
ORDER BY return_count DESC
"""
reasons = run_query(reasons_sql)

with col4:
    st.subheader("↩️ Returns by Reason")
    if not reasons.empty:
        fig = px.pie(
            reasons,
            names="return_reason",
            values="return_count",
            hole=0.35,
        )
        fig.update_layout(margin=dict(l=10, r=10, t=30, b=10), height=450)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No return data.")


# Row 3: Delivery performance + Category performance
col5, col6 = st.columns(2)

delivery_sql = """
SELECT *
FROM gold.delivery_performance
ORDER BY snapshot_date DESC
LIMIT 1
"""
delivery = run_query(delivery_sql)

with col5:
    st.subheader("🚚 Delivery Performance")
    if not delivery.empty:
        d = delivery.iloc[0]
        st.metric("Delivery Rate", f"{float(d['delivery_rate']):.1%}")
        st.metric("Cancellation Rate", f"{float(d['cancellation_rate']):.1%}")
        st.metric("Delay Rate", f"{float(d['delay_rate']):.1%}")
        avg_delay = d["avg_delay_days"]
        st.metric("Avg Delay (days)", f"{float(avg_delay):.1f}" if pd.notna(avg_delay) else "N/A")
    else:
        st.info("No delivery metrics.")

category_sql = """
SELECT
    COALESCE(product_category_name, 'unknown') AS category,
    SUM(net_revenue) AS net_revenue,
    SUM(times_ordered) AS times_ordered,
    SUM(times_returned) AS times_returned
FROM gold.product_performance
GROUP BY 1
ORDER BY net_revenue DESC NULLS LAST
LIMIT 12
"""
categories = run_query(category_sql)

with col6:
    st.subheader("📂 Revenue by Category")
    if not categories.empty:
        fig = px.bar(
            categories,
            x="category",
            y="net_revenue",
            labels={"category": "Category", "net_revenue": "Net Revenue ($)"},
        )
        fig.update_layout(margin=dict(l=10, r=10, t=30, b=10), height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No category data.")


# Data tables (expandable)
st.markdown("---")
with st.expander("📋 Daily Sales Summary (table)"):
    st.dataframe(daily, use_container_width=True)

with st.expander("📋 Top Products (table)"):
    st.dataframe(top_products, use_container_width=True)

with st.expander("📋 Return Reasons (table)"):
    st.dataframe(reasons, use_container_width=True)

st.caption("Built for Week 5 – Enterprise E-Commerce Data Platform | Gold layer → Streamlit")