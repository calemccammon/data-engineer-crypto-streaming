"""
Real-time crypto streaming dashboard.

Reads from the DuckDB Gold layer and auto-refreshes every 5 seconds.

Usage:
    uv run streamlit run dashboard/app.py

Panels:
    1. Live Price Ticker    — latest price + session change per symbol
    2. OHLCV Candlestick    — last 60 minutes of 1m candles (Plotly)
    3. Rolling Volatility   — 5m / 15m / 1h annualized vol
    4. Order Flow           — buy/sell volume bar + OFI over time
"""

import os
import time
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/crypto.duckdb")
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "5"))
SYMBOLS = [s.upper() for s in os.getenv("SYMBOLS", "btcusdt,ethusdt,solusdt").split(",")]

st.set_page_config(
    page_title="Crypto Streaming Pipeline",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Sidebar ---
st.sidebar.title("⚙️ Controls")
selected_symbol = st.sidebar.selectbox("Symbol", SYMBOLS, index=0)
candle_lookback = st.sidebar.slider("Candle history (minutes)", min_value=10, max_value=240, value=60)
auto_refresh = st.sidebar.toggle("Auto-refresh", value=True)
st.sidebar.caption(f"Refreshing every {REFRESH_SECONDS}s from DuckDB Gold")


def query(sql: str, params: list | None = None) -> pd.DataFrame:
    """
    Run a read-only query. Returns an empty frame when the database or the table
    has not been built yet, or when a writer holds the file for this refresh.
    Any other error is left for Streamlit to display.
    """
    if not Path(DUCKDB_PATH).exists():
        return pd.DataFrame()
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    except duckdb.IOException:
        st.caption("The database is locked by a writer. Retrying on the next refresh.")
        return pd.DataFrame()
    try:
        return conn.execute(sql, params or []).df()
    except duckdb.CatalogException:
        return pd.DataFrame()
    finally:
        conn.close()


# --- Main layout ---
st.title("📈 Crypto Streaming Pipeline — Live Dashboard")
st.caption("Data flows: Binance WS → Kafka → Bronze → Silver → Gold (dbt) → here")

# ---- Row 1: Live Price Ticker ----
st.subheader("🔴 Live Prices")
summary_df = query("SELECT * FROM gold.gold_symbol_summary ORDER BY symbol")

if summary_df.empty:
    st.warning("No Gold data yet. Start the pipeline and wait for candles to accumulate.")
else:
    cols = st.columns(len(summary_df))
    for col, (_, row) in zip(cols, summary_df.iterrows()):
        change_pct = row.get("session_change_pct", 0) or 0
        delta_str = f"{change_pct * 100:+.2f}%"
        col.metric(
            label=row["symbol"],
            value=f"${row['latest_price']:,.2f}",
            delta=delta_str,
        )

st.divider()

# ---- Row 2: Candlestick chart ----
st.subheader(f"🕯️ {selected_symbol} — 1-Minute OHLCV (last {candle_lookback}m)")

candle_df = query(
    """
    SELECT candle_open, open_price, high_price, low_price, close_price, volume, vwap
    FROM silver.ohlcv_1m
    WHERE symbol = ?
      AND candle_open >= NOW() - INTERVAL (?) MINUTE
    ORDER BY candle_open
    """,
    [selected_symbol, candle_lookback],
)

if candle_df.empty:
    st.info(f"No candles yet for {selected_symbol}.")
else:
    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=candle_df["candle_open"],
            open=candle_df["open_price"],
            high=candle_df["high_price"],
            low=candle_df["low_price"],
            close=candle_df["close_price"],
            name="OHLCV",
            increasing_line_color="#00c853",
            decreasing_line_color="#ff1744",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=candle_df["candle_open"],
            y=candle_df["vwap"],
            mode="lines",
            line=dict(color="orange", width=1, dash="dot"),
            name="VWAP",
        )
    )
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        height=400,
        margin=dict(l=0, r=0, t=20, b=0),
        legend=dict(orientation="h"),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---- Row 3: Volatility + Order Flow ----
left, right = st.columns(2)

with left:
    st.subheader(f"📊 Rolling Volatility — {selected_symbol}")
    vol_df = query(
        """
        SELECT candle_open, vol_5m_annualized, vol_15m_annualized, vol_1h_annualized
        FROM gold.gold_rolling_volatility
        WHERE symbol = ?
          AND candle_open >= NOW() - INTERVAL (?) MINUTE
        ORDER BY candle_open
        """,
        [selected_symbol, candle_lookback],
    )
    if vol_df.empty:
        st.info("No volatility data yet.")
    else:
        fig_vol = go.Figure()
        for col, color, name in [
            ("vol_5m_annualized", "#1976d2", "5m Vol"),
            ("vol_15m_annualized", "#7b1fa2", "15m Vol"),
            ("vol_1h_annualized", "#e65100", "1h Vol"),
        ]:
            fig_vol.add_trace(
                go.Scatter(
                    x=vol_df["candle_open"],
                    y=(vol_df[col] * 100).round(2),
                    mode="lines",
                    name=name,
                    line=dict(color=color),
                )
            )
        fig_vol.update_layout(
            yaxis_title="Annualized Vol (%)",
            height=300,
            margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig_vol, use_container_width=True)

with right:
    st.subheader(f"⚖️ Order Flow — {selected_symbol}")
    flow_df = query(
        """
        SELECT candle_open, buy_volume, sell_volume, order_flow_imbalance
        FROM gold.gold_trade_flow
        WHERE symbol = ?
          AND candle_open >= NOW() - INTERVAL (?) MINUTE
        ORDER BY candle_open
        """,
        [selected_symbol, candle_lookback],
    )
    if flow_df.empty:
        st.info("No trade flow data yet.")
    else:
        fig_flow = go.Figure()
        fig_flow.add_trace(
            go.Bar(x=flow_df["candle_open"], y=flow_df["buy_volume"], name="Buy Volume", marker_color="#00c853")
        )
        fig_flow.add_trace(
            go.Bar(x=flow_df["candle_open"], y=-flow_df["sell_volume"], name="Sell Volume", marker_color="#ff1744")
        )
        fig_flow.add_trace(
            go.Scatter(
                x=flow_df["candle_open"],
                y=flow_df["order_flow_imbalance"],
                mode="lines",
                name="OFI",
                yaxis="y2",
                line=dict(color="orange", width=2),
            )
        )
        fig_flow.update_layout(
            barmode="relative",
            yaxis2=dict(overlaying="y", side="right", range=[-1.2, 1.2], title="OFI"),
            height=300,
            margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig_flow, use_container_width=True)

# ---- Auto-refresh ----
if auto_refresh:
    time.sleep(REFRESH_SECONDS)
    st.rerun()
