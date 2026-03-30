import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SentinelFlow",
    page_icon="🛡️",
    layout="wide"
)

# ── Snowflake connection ────────────────────────────────────────────────────────
def get_snowflake_connection():
    from snowflake.connector import connect
    creds = st.secrets["snowflake"]
    return connect(
        account=creds["account"],
        user=creds["user"],
        private_key_file=creds["private_key_path"],
        role=creds["role"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"],
    )

@st.cache_data(ttl=300)
def load_data():
    """Load from Snowflake, fall back to mock data if unavailable."""
    try:
        conn = get_snowflake_connection()
        query = """
            SELECT
                type,
                risk_level,
                hour_of_day,
                amount,
                is_fraud,
                transaction_date
            FROM SENTINEL_DB.ANALYTICS.FCT_FINANCIAL_TRANSACTIONS
            LIMIT 50000
        """
        df = pd.read_sql(query, conn)
        conn.close()
        df.columns = [c.lower() for c in df.columns]
        return df, "live"
    except Exception:
        return mock_data(), "mock"


def mock_data():
    import numpy as np
    rng = np.random.default_rng(42)
    n = 5000
    types = rng.choice(["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"], n,
                       p=[0.22, 0.18, 0.15, 0.30, 0.15])
    risk_levels = rng.choice(
        ["LOW", "CONFIRMED_FRAUD", "SUSPICIOUS_DRAIN", "SUSPICIOUS_SPIKE", "FLAGGED", "HIGH_VELOCITY"],
        n, p=[0.75, 0.05, 0.06, 0.07, 0.04, 0.03]
    )
    return pd.DataFrame({
        "type": types,
        "risk_level": risk_levels,
        "hour_of_day": rng.integers(0, 24, n),
        "amount": rng.uniform(10, 5000, n).round(2),
        "is_fraud": (risk_levels == "CONFIRMED_FRAUD").astype(int),
        "transaction_date": pd.date_range("2024-01-01", periods=n, freq="1min").date,
    })


# ── Load data ──────────────────────────────────────────────────────────────────
df, source = load_data()

# ── Header ─────────────────────────────────────────────────────────────────────
st.title("🛡️ SentinelFlow — Transaction Risk Dashboard")
st.caption(
    f"Data source: **{'Snowflake (live)' if source == 'live' else 'Mock data (Snowflake unavailable)'}** · "
    f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}"
)

if source == "mock":
    st.info("Snowflake connection unavailable — displaying mock data for demonstration.", icon="ℹ️")

st.divider()

# ── KPI row ────────────────────────────────────────────────────────────────────
total = len(df)
fraud_count = int(df["is_fraud"].sum())
fraud_pct = fraud_count / total * 100 if total else 0
high_risk = df[df["risk_level"].isin(["CONFIRMED_FRAUD", "SUSPICIOUS_DRAIN", "SUSPICIOUS_SPIKE"])].shape[0]

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Transactions", f"{total:,}")
k2.metric("Confirmed Fraud", f"{fraud_count:,}", f"{fraud_pct:.2f}% of total")
k3.metric("High Risk Transactions", f"{high_risk:,}")
k4.metric("Transaction Types", df["type"].nunique())

st.divider()

# ── Tile 1: Transaction distribution by type and risk_level ────────────────────
st.subheader("Tile 1 — Transaction Distribution by Type & Risk Level")

risk_order = ["CONFIRMED_FRAUD", "SUSPICIOUS_DRAIN", "SUSPICIOUS_SPIKE",
              "FLAGGED", "HIGH_VELOCITY", "LOW"]
risk_colours = {
    "CONFIRMED_FRAUD":   "#d62728",
    "SUSPICIOUS_DRAIN":  "#ff7f0e",
    "SUSPICIOUS_SPIKE":  "#ffbb78",
    "FLAGGED":           "#9467bd",
    "HIGH_VELOCITY":     "#8c564b",
    "LOW":               "#2ca02c",
}

tile1_data = (
    df.groupby(["type", "risk_level"])
    .size()
    .reset_index(name="count")
)

fig1 = px.bar(
    tile1_data,
    x="type",
    y="count",
    color="risk_level",
    category_orders={"risk_level": risk_order},
    color_discrete_map=risk_colours,
    labels={"type": "Transaction Type", "count": "Number of Transactions", "risk_level": "Risk Level"},
    title="Transaction Volume by Type, segmented by Risk Level",
)
fig1.update_layout(
    legend_title="Risk Level",
    xaxis_title="Transaction Type",
    yaxis_title="Transaction Count",
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)
st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ── Tile 2: Transaction volume by hour of day ──────────────────────────────────
st.subheader("Tile 2 — Transaction Volume by Hour of Day")

tile2_data = (
    df.groupby(["hour_of_day", "risk_level"])
    .size()
    .reset_index(name="count")
)

fig2 = px.bar(
    tile2_data,
    x="hour_of_day",
    y="count",
    color="risk_level",
    category_orders={"risk_level": risk_order},
    color_discrete_map=risk_colours,
    labels={"hour_of_day": "Hour of Day (UTC)", "count": "Number of Transactions", "risk_level": "Risk Level"},
    title="Transaction Volume by Hour of Day — Temporal Fraud Pattern Analysis",
)
fig2.update_layout(
    xaxis=dict(tickmode="linear", tick0=0, dtick=1),
    legend_title="Risk Level",
    xaxis_title="Hour of Day (0–23)",
    yaxis_title="Transaction Count",
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)
st.plotly_chart(fig2, use_container_width=True)

st.divider()
st.caption("SentinelFlow · MSc Data Engineering Capstone · Built with Streamlit + Snowflake + dbt")
