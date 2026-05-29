#!/usr/bin/env python3
"""Streamlit real-time dashboard"""

import random
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta

import streamlit as st
import plotly.express as px
import pandas as pd


class RealtimeStats:
    def __init__(self, max_points: int = 100):
        self.timestamps = deque(maxlen=max_points)
        self.rates = deque(maxlen=max_points)
        self.status_counts = defaultdict(int)
        self.path_counts = defaultdict(int)

    def add(self, entry: dict):
        self.timestamps.append(datetime.now())
        self.status_counts[entry.get("status", 0)] += 1
        path = self._extract_path(entry.get("request", ""))
        self.path_counts[path] += 1
        cutoff = datetime.now() - timedelta(minutes=1)
        recent = sum(1 for ts in self.timestamps if ts > cutoff)
        self.rates.append(recent)

    def _extract_path(self, request: str) -> str:
        parts = request.split()
        return parts[1] if len(parts) > 1 else "/"

    def get_rate(self) -> float:
        return self.rates[-1] if self.rates else 0

    def get_status_df(self) -> pd.DataFrame:
        return pd.DataFrame([{"status": k, "count": v} for k, v in self.status_counts.items()])

    def get_path_df(self) -> pd.DataFrame:
        top = sorted(self.path_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        return pd.DataFrame([{"path": k, "count": v} for k, v in top])


def main():
    st.set_page_config(page_title="Log Analytics Dashboard", layout="wide")
    st.title("📊 Real-time Log Analytics Dashboard")
    st.markdown("### Web Server Log Monitoring (5-minute sliding window)")

    with st.sidebar:
        st.header("Settings")
        update_interval = st.slider("Update interval (seconds)", 1, 10, 2)
        st.markdown("---")
        st.markdown("**Student:** Evstigneev Fedor")
        st.markdown("**Group:** 220032-11")
        st.markdown("**Variant:** #5 (Web Server Logs)")
        st.markdown("**Level:** Advanced")

    if "stats" not in st.session_state:
        st.session_state.stats = RealtimeStats()
        st.session_state.last_update = time.time()

    col1, col2, col3, col4 = st.columns(4)
    rate_placeholder = col1.empty()
    total_placeholder = col2.empty()
    top_path_placeholder = col3.empty()
    status_placeholder = col4.empty()

    status_chart = st.empty()
    path_chart = st.empty()

    auto_update = st.checkbox("Auto-update (simulate real-time)", value=True)

    if auto_update:
        paths = ["/", "/api/users", "/api/products", "/admin", "/login", "/static/style.css"]
        statuses = [200, 200, 200, 200, 301, 400, 404, 500]

        if time.time() - st.session_state.last_update >= update_interval:
            for _ in range(random.randint(5, 20)):
                entry = {
                    "request": f"GET {random.choice(paths)} HTTP/1.1",
                    "status": random.choice(statuses),
                    "response_time": random.uniform(0.01, 0.5),
                }
                st.session_state.stats.add(entry)
            st.session_state.last_update = time.time()

        rate_placeholder.metric("Requests/sec", f"{st.session_state.stats.get_rate():.1f}")
        total_placeholder.metric("Total requests (last min)", sum(st.session_state.stats.rates))

        status_df = st.session_state.stats.get_status_df()
        path_df = st.session_state.stats.get_path_df()

        if not status_df.empty:
            top_status = status_df.loc[status_df["count"].idxmax()]
            top_path_placeholder.metric("Top status", f"{top_status['status']} ({top_status['count']})")
            status_placeholder.metric("Unique paths", len(st.session_state.stats.path_counts))

        if not status_df.empty:
            fig1 = px.pie(status_df, values="count", names="status", title="Status Code Distribution")
            status_chart.plotly_chart(fig1, use_container_width=True)

        if not path_df.empty:
            fig2 = px.bar(path_df, x="path", y="count", title="Top 10 Request Paths")
            path_chart.plotly_chart(fig2, use_container_width=True)

        time.sleep(0.5)
        st.rerun()
    else:
        st.info("Toggle 'Auto-update' to see real-time simulation")


if __name__ == "__main__":
    main()
