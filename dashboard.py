"""Azure Cost Optimizer — Streamlit Dashboard."""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Azure Cost Optimizer",
    page_icon="☁️",
    layout="wide",
    initial_sidebar_state="expanded",
)

OUTPUT_DIR = Path("outputs")
RAW_DIR = OUTPUT_DIR / "raw"
REPORTS_DIR = OUTPUT_DIR / "reports"
RECS_DIR = OUTPUT_DIR / "recommendations"

PRIORITY_COLORS = {
    "critical": "#d62728",
    "high": "#ff7f0e",
    "medium": "#ffdd57",
    "low": "#2ca02c",
}
CATEGORY_COLORS = {
    "Cost": "#2196F3",
    "Security": "#F44336",
    "HighAvailability": "#FF9800",
    "Performance": "#4CAF50",
    "OperationalExcellence": "#9C27B0",
}


# ─────────────────────────────────────────────
# Data loaders
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def list_raw_files() -> list[Path]:
    return sorted(RAW_DIR.glob("raw_*.json"), reverse=True) if RAW_DIR.exists() else []


@st.cache_data(ttl=60)
def list_report_files() -> list[Path]:
    return sorted(REPORTS_DIR.glob("report_*.json"), reverse=True) if REPORTS_DIR.exists() else []


@st.cache_data(ttl=60)
def list_optimization_files() -> list[Path]:
    return sorted(RECS_DIR.glob("optimization_*.json"), reverse=True) if RECS_DIR.exists() else []


@st.cache_data
def load_raw(path: str) -> dict:
    return json.loads(Path(path).read_text())


@st.cache_data
def load_report(path: str) -> dict:
    return json.loads(Path(path).read_text())


@st.cache_data
def load_optimization(path: str) -> dict:
    return json.loads(Path(path).read_text())


def raw_to_dataframes(raw: dict) -> dict[str, pd.DataFrame]:
    """Convert raw JSON into analysis-ready DataFrames."""
    cost_df = pd.DataFrame(raw.get("cost_entries", []))
    if not cost_df.empty:
        cost_df["date"] = pd.to_datetime(cost_df["date"], format="%Y%m%d", errors="coerce")
        cost_df["cost_usd"] = cost_df["cost_usd"].astype(float)
        cost_df["resource_group"] = cost_df["resource_group"].fillna("(none)").replace("", "(none)")
        cost_df["service_name"] = cost_df["service_name"].fillna("Unknown")

    resource_df = pd.DataFrame(raw.get("resources", []))
    advisor_df = pd.DataFrame(raw.get("advisor_recommendations", []))

    return {"costs": cost_df, "resources": resource_df, "advisor": advisor_df}


# ─────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/a/a8/Microsoft_Azure_Logo.svg",
        width=160,
    )
    st.title("Azure Cost Optimizer")
    st.caption("Multi-agent analysis powered by Claude")
    st.divider()

    raw_files = list_raw_files()
    opt_files = list_optimization_files()

    # File selectors
    selected_raw = None
    if raw_files:
        raw_labels = {f.name: str(f) for f in raw_files}
        chosen_raw_label = st.selectbox(
            "Raw data snapshot",
            options=list(raw_labels.keys()),
            help="Select a collected Azure snapshot",
        )
        selected_raw = raw_labels[chosen_raw_label]
    else:
        st.warning("No raw data found. Run the collector first.")

    selected_opt = None
    if opt_files:
        opt_labels = {f.name: str(f) for f in opt_files}
        chosen_opt_label = st.selectbox(
            "Optimization report",
            options=list(opt_labels.keys()),
            help="Select a Claude optimization report",
        )
        selected_opt = opt_labels[chosen_opt_label]
    else:
        st.info("No optimization report yet. Run the full pipeline to generate one.")

    st.divider()

    # Run pipeline button
    st.subheader("Run Analysis")
    run_col1, run_col2 = st.columns(2)
    run_collect = run_col1.button("Full Pipeline", type="primary", use_container_width=True,
                                   help="Collect fresh Azure data + run Claude agents")
    run_agents = run_col2.button("Agents Only", use_container_width=True,
                                  help="Re-run Claude agents on existing raw data (faster)",
                                  disabled=selected_raw is None)

    if run_collect:
        with st.spinner("Running full pipeline (this takes a few minutes)…"):
            result = subprocess.run(
                [sys.executable, "main.py"],
                capture_output=True, text=True, cwd="."
            )
        if result.returncode == 0:
            st.success("Pipeline complete!")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Pipeline failed")
            st.code(result.stderr[-3000:])

    if run_agents and selected_raw:
        with st.spinner("Running Claude agents on existing data…"):
            result = subprocess.run(
                [sys.executable, "main.py", "--raw-file", selected_raw],
                capture_output=True, text=True, cwd="."
            )
        if result.returncode == 0:
            st.success("Agents complete!")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Agents failed")
            st.code(result.stderr[-3000:])

    st.divider()
    st.caption("Data refreshes automatically every 60s")


# ─────────────────────────────────────────────
# Main content — requires raw data
# ─────────────────────────────────────────────
if not selected_raw:
    st.info("👈 No data found. Use the sidebar to run the pipeline.")
    st.stop()

raw = load_raw(selected_raw)
dfs = raw_to_dataframes(raw)
costs = dfs["costs"]
resources = dfs["resources"]
advisor = dfs["advisor"]

collected_at = datetime.fromisoformat(raw["collected_at"])
total_cost = raw.get("total_cost_usd", 0.0)
resource_count = raw.get("resource_count", 0)
lookback = raw.get("lookback_days", 30)
sub_id = raw.get("subscription_id", "")

# Load optimization if available
opt_data = load_optimization(selected_opt) if selected_opt else None

# ─────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────
tab_overview, tab_costs, tab_resources, tab_advisor, tab_optimization = st.tabs([
    "📊 Overview",
    "💰 Cost Analysis",
    "🖥️ Resources",
    "💡 Advisor",
    "🚀 Optimization",
])


# ═══════════════════════════════════════════
# TAB 1 — OVERVIEW
# ═══════════════════════════════════════════
with tab_overview:
    st.header("Subscription Overview")
    st.caption(
        f"Subscription `{sub_id}` · Snapshot: {collected_at.strftime('%Y-%m-%d %H:%M UTC')} · "
        f"Period: last {lookback} days"
    )

    # KPI row
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Spend", f"${total_cost:,.2f}", help=f"Last {lookback} days")
    k2.metric("Resources", resource_count)
    k3.metric("Cost Entries", len(costs))
    k4.metric("Advisor Alerts", len(advisor))

    pot_savings = opt_data.get("total_potential_savings_usd", 0) if opt_data else None
    if pot_savings is not None:
        k5.metric("Potential Savings", f"${pot_savings:,.2f}/mo", delta=f"-${pot_savings:,.2f}",
                  delta_color="inverse")
    else:
        k5.metric("Potential Savings", "Run agents →")

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Cost by Service")
        if not costs.empty:
            by_service = (
                costs.groupby("service_name")["cost_usd"]
                .sum()
                .sort_values(ascending=False)
                .head(8)
                .reset_index()
            )
            fig = px.pie(
                by_service,
                values="cost_usd",
                names="service_name",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(showlegend=False, margin=dict(t=10, b=10, l=10, r=10), height=320)
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Cost by Resource Group")
        if not costs.empty:
            by_rg = (
                costs.groupby("resource_group")["cost_usd"]
                .sum()
                .sort_values(ascending=False)
                .head(8)
                .reset_index()
            )
            fig = px.bar(
                by_rg,
                x="cost_usd",
                y="resource_group",
                orientation="h",
                color="cost_usd",
                color_continuous_scale="Blues",
                labels={"cost_usd": "Cost (USD)", "resource_group": ""},
            )
            fig.update_layout(
                coloraxis_showscale=False,
                margin=dict(t=10, b=10, l=10, r=10),
                height=320,
                yaxis={"categoryorder": "total ascending"},
            )
            st.plotly_chart(fig, use_container_width=True)

    # Daily trend
    st.subheader("Daily Spend Trend")
    if not costs.empty and costs["date"].notna().any():
        daily = costs.groupby("date")["cost_usd"].sum().reset_index()
        daily = daily.sort_values("date")
        fig = px.area(
            daily,
            x="date",
            y="cost_usd",
            labels={"date": "Date", "cost_usd": "Daily Cost (USD)"},
            color_discrete_sequence=["#0078D4"],
        )
        fig.update_layout(margin=dict(t=10, b=10), height=250)
        st.plotly_chart(fig, use_container_width=True)

    # Resource type breakdown
    st.subheader("Resource Inventory by Type")
    if not resources.empty:
        by_type = resources["type"].value_counts().reset_index()
        by_type.columns = ["Resource Type", "Count"]
        fig = px.bar(
            by_type.head(12),
            x="Count",
            y="Resource Type",
            orientation="h",
            color="Count",
            color_continuous_scale="Teal",
        )
        fig.update_layout(
            coloraxis_showscale=False,
            margin=dict(t=10, b=10, l=10, r=10),
            height=max(200, len(by_type.head(12)) * 30),
            yaxis={"categoryorder": "total ascending"},
        )
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════
# TAB 2 — COST ANALYSIS
# ═══════════════════════════════════════════
with tab_costs:
    st.header("Cost Analysis")

    if costs.empty:
        st.warning("No cost data available.")
    else:
        # Filters
        fc1, fc2, fc3 = st.columns(3)
        all_services = sorted(costs["service_name"].unique())
        all_rgs = sorted(costs["resource_group"].unique())

        sel_services = fc1.multiselect("Filter by Service", all_services, default=[])
        sel_rgs = fc2.multiselect("Filter by Resource Group", all_rgs, default=[])
        min_cost = fc3.slider("Min cost (USD)", 0.0, float(costs["cost_usd"].max()), 0.0, step=0.01)

        filtered = costs.copy()
        if sel_services:
            filtered = filtered[filtered["service_name"].isin(sel_services)]
        if sel_rgs:
            filtered = filtered[filtered["resource_group"].isin(sel_rgs)]
        filtered = filtered[filtered["cost_usd"] >= min_cost]

        st.caption(f"Showing {len(filtered):,} of {len(costs):,} cost entries")

        # Service vs RG heatmap
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Daily Cost by Service")
            if filtered["date"].notna().any():
                daily_service = (
                    filtered.groupby(["date", "service_name"])["cost_usd"]
                    .sum()
                    .reset_index()
                )
                top_svcs = (
                    daily_service.groupby("service_name")["cost_usd"]
                    .sum()
                    .nlargest(6)
                    .index.tolist()
                )
                daily_service = daily_service[daily_service["service_name"].isin(top_svcs)]
                fig = px.line(
                    daily_service,
                    x="date",
                    y="cost_usd",
                    color="service_name",
                    labels={"cost_usd": "Cost (USD)", "date": "Date", "service_name": "Service"},
                )
                fig.update_layout(height=300, margin=dict(t=10, b=10))
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Spend by Service (Total)")
            by_svc = (
                filtered.groupby("service_name")["cost_usd"]
                .sum()
                .sort_values(ascending=False)
                .reset_index()
            )
            fig = px.bar(
                by_svc.head(10),
                x="service_name",
                y="cost_usd",
                color="cost_usd",
                color_continuous_scale="Blues",
                labels={"cost_usd": "Total Cost (USD)", "service_name": ""},
            )
            fig.update_layout(
                coloraxis_showscale=False,
                height=300,
                margin=dict(t=10, b=10),
                xaxis_tickangle=-30,
            )
            st.plotly_chart(fig, use_container_width=True)

        # Detailed table
        st.subheader("Cost Entry Detail")
        display = (
            filtered.groupby(["service_name", "resource_group", "meter_category"])["cost_usd"]
            .sum()
            .reset_index()
            .sort_values("cost_usd", ascending=False)
        )
        display.columns = ["Service", "Resource Group", "Meter Category", "Cost (USD)"]
        display["Cost (USD)"] = display["Cost (USD)"].map("${:,.4f}".format)
        st.dataframe(display, use_container_width=True, height=400)


# ═══════════════════════════════════════════
# TAB 3 — RESOURCES
# ═══════════════════════════════════════════
with tab_resources:
    st.header("Resource Inventory")

    if resources.empty:
        st.warning("No resource data available.")
    else:
        # Summary stats
        r1, r2, r3 = st.columns(3)
        r1.metric("Total Resources", len(resources))
        r2.metric("Resource Groups", resources["resource_group"].nunique())
        r3.metric("Locations", resources["location"].nunique())

        # Filters
        rf1, rf2 = st.columns(2)
        all_types = sorted(resources["type"].unique())
        all_locs = sorted(resources["location"].unique())
        sel_types = rf1.multiselect("Filter by Type", all_types)
        sel_locs = rf2.multiselect("Filter by Location", all_locs)

        res_filtered = resources.copy()
        if sel_types:
            res_filtered = res_filtered[res_filtered["type"].isin(sel_types)]
        if sel_locs:
            res_filtered = res_filtered[res_filtered["location"].isin(sel_locs)]

        # Location map (bubble)
        st.subheader("Resources by Location")
        loc_counts = res_filtered["location"].value_counts().reset_index()
        loc_counts.columns = ["location", "count"]
        fig = px.bar(
            loc_counts,
            x="location",
            y="count",
            color="count",
            color_continuous_scale="Teal",
            labels={"location": "Azure Region", "count": "Resource Count"},
        )
        fig.update_layout(coloraxis_showscale=False, height=250, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

        # Resource table
        st.subheader(f"All Resources ({len(res_filtered)})")
        display_cols = ["name", "type", "resource_group", "location"]
        available = [c for c in display_cols if c in res_filtered.columns]
        st.dataframe(
            res_filtered[available].sort_values("resource_group"),
            use_container_width=True,
            height=450,
        )


# ═══════════════════════════════════════════
# TAB 4 — ADVISOR
# ═══════════════════════════════════════════
with tab_advisor:
    st.header("Azure Advisor Recommendations")

    if advisor.empty:
        st.info("No Advisor recommendations found.")
    else:
        # KPI row
        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Total Recommendations", len(advisor))
        a2.metric("High Impact", len(advisor[advisor["impact"] == "High"]))
        a3.metric("With Savings Estimate",
                  len(advisor[advisor["potential_savings_usd"].notna()]))
        total_sav = advisor["potential_savings_usd"].sum()
        a4.metric("Total Advisor Savings", f"${total_sav:,.2f}" if total_sav > 0 else "N/A")

        # Charts
        ac1, ac2 = st.columns(2)
        with ac1:
            st.subheader("By Category")
            cat_counts = advisor["category"].value_counts().reset_index()
            cat_counts.columns = ["category", "count"]
            colors = [CATEGORY_COLORS.get(c, "#999") for c in cat_counts["category"]]
            fig = px.pie(
                cat_counts,
                values="count",
                names="category",
                color_discrete_sequence=colors,
                hole=0.35,
            )
            fig.update_layout(height=280, margin=dict(t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

        with ac2:
            st.subheader("By Impact Level")
            imp_counts = advisor["impact"].value_counts().reset_index()
            imp_counts.columns = ["impact", "count"]
            impact_order = {"High": 0, "Medium": 1, "Low": 2}
            imp_counts["order"] = imp_counts["impact"].map(impact_order)
            imp_counts = imp_counts.sort_values("order")
            fig = px.bar(
                imp_counts,
                x="impact",
                y="count",
                color="impact",
                color_discrete_map={"High": "#d62728", "Medium": "#ff7f0e", "Low": "#2ca02c"},
                labels={"count": "Recommendations", "impact": "Impact"},
            )
            fig.update_layout(showlegend=False, height=280, margin=dict(t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

        # Filters + table
        af1, af2 = st.columns(2)
        sel_cat = af1.multiselect(
            "Filter by Category",
            sorted(advisor["category"].unique()),
            default=sorted(advisor["category"].unique()),
        )
        sel_impact = af2.multiselect(
            "Filter by Impact",
            ["High", "Medium", "Low"],
            default=["High", "Medium", "Low"],
        )

        adv_filtered = advisor[
            advisor["category"].isin(sel_cat) & advisor["impact"].isin(sel_impact)
        ]

        st.subheader(f"Recommendations ({len(adv_filtered)})")
        display_advisor = adv_filtered[
            ["category", "impact", "short_description", "impacted_resource_type",
             "potential_savings_usd"]
        ].copy()
        display_advisor.columns = [
            "Category", "Impact", "Description", "Resource Type", "Savings (USD)"
        ]
        display_advisor["Savings (USD)"] = display_advisor["Savings (USD)"].apply(
            lambda x: f"${x:,.2f}" if pd.notna(x) else "—"
        )
        st.dataframe(display_advisor, use_container_width=True, height=450)


# ═══════════════════════════════════════════
# TAB 5 — OPTIMIZATION
# ═══════════════════════════════════════════
with tab_optimization:
    st.header("Cost Optimization Recommendations")

    if not opt_data:
        st.info(
            "No optimization report found yet.\n\n"
            "Add your `ANTHROPIC_API_KEY` to `.env`, then click **Agents Only** in the sidebar "
            "to run Claude's analysis on the existing collected data."
        )
        st.code(
            "# In .env:\nANTHROPIC_API_KEY=sk-ant-api03-...\n\n"
            "# Or set it in your shell:\nexport ANTHROPIC_API_KEY=sk-ant-api03-..."
        )
        st.stop()

    recs = opt_data.get("recommendations", [])
    total_savings = opt_data.get("total_potential_savings_usd", 0)
    summary = opt_data.get("executive_summary", "")
    gen_at = opt_data.get("generated_at", "")

    # KPIs
    ok1, ok2, ok3, ok4 = st.columns(4)
    ok1.metric("Total Recommendations", len(recs))
    ok2.metric("Potential Savings", f"${total_savings:,.2f}/mo")
    ok3.metric("Annual Opportunity", f"${total_savings * 12:,.2f}")
    critical_count = sum(1 for r in recs if r.get("priority") == "critical")
    ok4.metric("Critical Items", critical_count)

    if gen_at:
        try:
            gen_dt = datetime.fromisoformat(gen_at)
            st.caption(f"Report generated: {gen_dt.strftime('%Y-%m-%d %H:%M UTC')}")
        except Exception:
            pass

    st.divider()

    # Executive summary
    if summary:
        with st.expander("📋 Executive Summary", expanded=True):
            st.markdown(summary)

    st.divider()

    # Charts row
    if recs:
        oc1, oc2 = st.columns(2)

        with oc1:
            st.subheader("Savings by Priority")
            pri_df = pd.DataFrame([
                {
                    "priority": r.get("priority", "unknown").capitalize(),
                    "savings": r.get("estimated_monthly_savings_usd", 0),
                }
                for r in recs
            ])
            if not pri_df.empty:
                pri_sum = pri_df.groupby("priority")["savings"].sum().reset_index()
                color_map = {
                    "Critical": "#d62728", "High": "#ff7f0e",
                    "Medium": "#ffdd57", "Low": "#2ca02c",
                }
                fig = px.bar(
                    pri_sum,
                    x="priority",
                    y="savings",
                    color="priority",
                    color_discrete_map=color_map,
                    labels={"savings": "Monthly Savings (USD)", "priority": "Priority"},
                )
                fig.update_layout(showlegend=False, height=280, margin=dict(t=10, b=10))
                st.plotly_chart(fig, use_container_width=True)

        with oc2:
            st.subheader("Savings by Category")
            cat_df = pd.DataFrame([
                {
                    "category": r.get("category", "other").replace("_", " ").title(),
                    "savings": r.get("estimated_monthly_savings_usd", 0),
                }
                for r in recs
            ])
            if not cat_df.empty:
                cat_sum = cat_df.groupby("category")["savings"].sum().reset_index()
                fig = px.pie(
                    cat_sum,
                    values="savings",
                    names="category",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                fig.update_layout(height=280, margin=dict(t=10, b=10))
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Recommendations detail
        st.subheader("All Recommendations")

        # Filter
        pf1, pf2 = st.columns(2)
        all_priorities = sorted({r.get("priority", "unknown") for r in recs})
        all_categories = sorted({r.get("category", "unknown") for r in recs})
        sel_pri = pf1.multiselect("Priority", all_priorities, default=all_priorities)
        sel_cat_opt = pf2.multiselect("Category", all_categories, default=all_categories)

        filtered_recs = [
            r for r in recs
            if r.get("priority") in sel_pri and r.get("category") in sel_cat_opt
        ]

        for rec in filtered_recs:
            priority = rec.get("priority", "low")
            color = PRIORITY_COLORS.get(priority, "#999")
            savings = rec.get("estimated_monthly_savings_usd", 0)
            effort = rec.get("implementation_effort", "medium")
            effort_icons = {"low": "🟢", "medium": "🟡", "high": "🔴"}

            with st.expander(
                f"{'🔴' if priority == 'critical' else '🟠' if priority == 'high' else '🟡' if priority == 'medium' else '🟢'} "
                f"[{priority.upper()}] {rec.get('title', 'Recommendation')} — "
                f"${savings:,.2f}/mo",
                expanded=(priority in ("critical", "high")),
            ):
                col_desc, col_meta = st.columns([3, 1])

                with col_desc:
                    st.markdown(rec.get("description", ""))

                    steps = rec.get("steps", [])
                    if steps:
                        st.markdown("**Implementation Steps:**")
                        for i, step in enumerate(steps, 1):
                            st.markdown(f"{i}. {step}")

                    affected = rec.get("affected_resources", [])
                    if affected:
                        st.markdown("**Affected Resources:**")
                        for res in affected[:5]:
                            st.code(res, language=None)
                        if len(affected) > 5:
                            st.caption(f"… and {len(affected) - 5} more")

                with col_meta:
                    st.metric("Monthly Savings", f"${savings:,.2f}")
                    st.metric("Annual Savings", f"${savings * 12:,.2f}")
                    st.markdown(
                        f"**Category:** {rec.get('category', '').replace('_', ' ').title()}"
                    )
                    st.markdown(
                        f"**Effort:** {effort_icons.get(effort, '⚪')} {effort.capitalize()}"
                    )

        # Download Markdown report
        md_files = list(RECS_DIR.glob("optimization_*.md"))
        if md_files:
            latest_md = sorted(md_files, reverse=True)[0]
            st.divider()
            st.download_button(
                label="⬇️ Download Full Markdown Report",
                data=latest_md.read_text(),
                file_name=latest_md.name,
                mime="text/markdown",
            )
    else:
        st.info("No recommendations in the selected report.")
