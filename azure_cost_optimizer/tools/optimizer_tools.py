"""Tool definitions and handlers for the Optimizer Agent (Agent 3)."""

import json
from collections import defaultdict
from datetime import datetime, timezone

from ..models.raw_data import RawAzureData
from ..models.structured import StructuredReport

# Azure VM family downsizing map: current_size -> recommended_smaller_size
# Maps within the same family/generation
VM_DOWNSIZE_MAP: dict[str, str] = {
    # D-series v5
    "Standard_D64s_v5": "Standard_D32s_v5",
    "Standard_D32s_v5": "Standard_D16s_v5",
    "Standard_D16s_v5": "Standard_D8s_v5",
    "Standard_D8s_v5": "Standard_D4s_v5",
    "Standard_D4s_v5": "Standard_D2s_v5",
    # D-series v4
    "Standard_D64s_v4": "Standard_D32s_v4",
    "Standard_D32s_v4": "Standard_D16s_v4",
    "Standard_D16s_v4": "Standard_D8s_v4",
    "Standard_D8s_v4": "Standard_D4s_v4",
    "Standard_D4s_v4": "Standard_D2s_v4",
    # E-series v5 (memory optimized)
    "Standard_E64s_v5": "Standard_E32s_v5",
    "Standard_E32s_v5": "Standard_E16s_v5",
    "Standard_E16s_v5": "Standard_E8s_v5",
    "Standard_E8s_v5": "Standard_E4s_v5",
    "Standard_E4s_v5": "Standard_E2s_v5",
    # F-series (compute optimized)
    "Standard_F72s_v2": "Standard_F36s_v2",
    "Standard_F36s_v2": "Standard_F16s_v2",
    "Standard_F16s_v2": "Standard_F8s_v2",
    "Standard_F8s_v2": "Standard_F4s_v2",
    "Standard_F4s_v2": "Standard_F2s_v2",
    # B-series (burstable)
    "Standard_B8ms": "Standard_B4ms",
    "Standard_B4ms": "Standard_B2ms",
    "Standard_B2ms": "Standard_B2s",
    "Standard_B2s": "Standard_B1ms",
}

# 1-year Reserved Instance discount rates by VM family prefix
RI_DISCOUNT_RATES: dict[str, float] = {
    "Standard_D": 0.36,
    "Standard_E": 0.38,
    "Standard_F": 0.34,
    "Standard_B": 0.28,
    "Standard_M": 0.42,
    "Standard_L": 0.35,
    "Standard_N": 0.40,
    "Standard_H": 0.38,
    "default": 0.32,
}

# Azure Compute Savings Plan discounts (conservative estimates)
SAVINGS_PLAN_DISCOUNTS = {1: 0.17, 3: 0.33}

OPTIMIZER_TOOLS = [
    {
        "name": "get_right_sizing_candidates",
        "description": (
            "Returns VMs that are candidates for right-sizing based on CPU utilization "
            "thresholds. Includes current size, recommended smaller size, and estimated "
            "monthly savings. Sizes are matched within the same VM family."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cpu_threshold_percent": {
                    "type": "number",
                    "description": "Flag VMs with avg CPU below this threshold. Default: 20.0",
                    "default": 20.0,
                }
            },
            "required": [],
        },
    },
    {
        "name": "calculate_reservation_savings",
        "description": (
            "Analyzes consistently-running VMs and estimates savings from 1-year "
            "Reserved Instances vs pay-as-you-go. Uses standard Azure RI discount rates "
            "by VM family (typically 30-45% savings)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "min_monthly_cost_usd": {
                    "type": "number",
                    "description": "Only consider VMs costing at least this per month. Default: 50.",
                    "default": 50,
                }
            },
            "required": [],
        },
    },
    {
        "name": "calculate_savings_plan_estimate",
        "description": (
            "Estimates potential savings from an Azure Compute Savings Plan. "
            "Azure Savings Plans provide 15-33% savings on compute. "
            "Returns commitment amount and estimated annual savings."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "commitment_term_years": {
                    "type": "integer",
                    "description": "Commitment term (1 or 3 years). 3-year gives higher discounts.",
                    "default": 1,
                }
            },
            "required": [],
        },
    },
    {
        "name": "get_orphaned_resource_cleanup_plan",
        "description": (
            "Returns a detailed cleanup plan for orphaned resources with step-by-step "
            "instructions and total estimated monthly savings."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "analyze_cost_anomalies",
        "description": (
            "Detects services or resource groups with unusual cost spikes. "
            "Compares recent 7-day spend rate vs prior period. "
            "Returns list of anomalies with spike percentage and absolute delta."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "spike_threshold_percent": {
                    "type": "number",
                    "description": "Flag if recent rate is this % higher than baseline. Default: 50.",
                    "default": 50.0,
                }
            },
            "required": [],
        },
    },
    {
        "name": "get_performance_tracking_report",
        "description": (
            "Returns a performance summary for VMs: average/max CPU, disk I/O, and network "
            "throughput over the lookback period. Identifies over-provisioned and at-capacity resources."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sort_by": {
                    "type": "string",
                    "description": "Sort results by this field: cpu_avg, cost. Default: cpu_avg",
                    "default": "cpu_avg",
                }
            },
            "required": [],
        },
    },
    {
        "name": "analyze_ai_model_costs",
        "description": (
            "Analyzes Azure AI and Cognitive Services resources to identify cost optimization "
            "opportunities. Checks for: over-provisioned model deployments, expensive model tiers "
            "that could be replaced by cheaper alternatives (e.g. GPT-4o → GPT-4o-mini), unused "
            "endpoints, consolidation opportunities across multiple accounts, and SKU downgrades. "
            "Also evaluates Azure ML workspace compute costs."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "analyze_data_factory_costs",
        "description": (
            "Analyzes Azure Data Factory pipelines and integration runtimes for cost optimization. "
            "Checks for: idle integration runtimes (auto-shutdown), over-sized IR compute, "
            "pay-per-use vs fixed IR selection, pipeline scheduling inefficiencies, "
            "and opportunities to replace ADF with cheaper alternatives like Fabric Pipelines."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "analyze_databricks_usage",
        "description": (
            "Analyzes Azure Databricks workspaces for cost optimization. "
            "Checks for: clusters without auto-termination, spot/low-priority instance usage, "
            "cluster pool utilization, workspace consolidation opportunities, "
            "job cluster vs all-purpose cluster usage, and DBU optimization."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "analyze_synapse_and_fabric_costs",
        "description": (
            "Analyzes Azure Synapse Analytics and Microsoft Fabric capacity costs. "
            "For Synapse: checks dedicated SQL pool auto-pause settings, DWU sizing, "
            "dedicated vs serverless pool selection. "
            "For Fabric: checks capacity SKU right-sizing, reserved capacity discounts, "
            "and workload distribution. Compares costs between Synapse and Fabric."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "analyze_sql_migration_opportunity",
        "description": (
            "Analyzes Azure SQL databases, elastic pools, and managed instances to assess "
            "migration opportunities to open-source alternatives (MySQL, PostgreSQL). "
            "Provides cost comparison (Azure SQL is typically 2-4x more expensive), "
            "compatibility assessment, migration complexity rating, and step-by-step "
            "migration guidance for suitable workloads."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "include_managed_instance": {
                    "type": "boolean",
                    "description": "Include Managed Instance in migration analysis. Default: true.",
                    "default": True,
                }
            },
            "required": [],
        },
    },
    {
        "name": "finalize_optimization_report",
        "description": (
            "Called when all analysis is complete. Pass the full list of recommendations "
            "and the executive summary. This ends the optimization phase."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "executive_summary": {
                    "type": "string",
                    "description": "2-3 paragraph executive summary of findings and top actions.",
                },
                "recommendations": {
                    "type": "array",
                    "description": "List of OptimizationRecommendation objects as dicts.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "priority": {
                                "type": "string",
                                "enum": ["critical", "high", "medium", "low"],
                            },
                            "category": {"type": "string"},
                            "title": {"type": "string"},
                            "description": {"type": "string"},
                            "affected_resources": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "estimated_monthly_savings_usd": {"type": "number"},
                            "implementation_effort": {
                                "type": "string",
                                "enum": ["low", "medium", "high"],
                            },
                            "steps": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["priority", "category", "title", "description", "steps"],
                    },
                },
                "total_potential_savings_usd": {"type": "number"},
            },
            "required": ["executive_summary", "recommendations", "total_potential_savings_usd"],
        },
    },
]


def handle_optimizer_tool(
    tool_name: str,
    tool_input: dict,
    structured_report: StructuredReport,
    raw_data: RawAzureData,
) -> str:
    """Dispatch optimizer tool calls from Claude. Returns a JSON string."""
    handlers = {
        "get_right_sizing_candidates": lambda: _get_right_sizing_candidates(
            structured_report, raw_data, tool_input.get("cpu_threshold_percent", 20.0)
        ),
        "calculate_reservation_savings": lambda: _calculate_reservation_savings(
            structured_report, raw_data, tool_input.get("min_monthly_cost_usd", 50)
        ),
        "calculate_savings_plan_estimate": lambda: _calculate_savings_plan_estimate(
            structured_report, tool_input.get("commitment_term_years", 1)
        ),
        "get_orphaned_resource_cleanup_plan": lambda: _get_orphaned_resource_cleanup_plan(
            structured_report
        ),
        "analyze_cost_anomalies": lambda: _analyze_cost_anomalies(
            raw_data, tool_input.get("spike_threshold_percent", 50.0)
        ),
        "get_performance_tracking_report": lambda: _get_performance_tracking_report(
            raw_data, structured_report, tool_input.get("sort_by", "cpu_avg")
        ),
        "analyze_ai_model_costs": lambda: _analyze_ai_model_costs(raw_data),
        "analyze_data_factory_costs": lambda: _analyze_data_factory_costs(raw_data),
        "analyze_databricks_usage": lambda: _analyze_databricks_usage(raw_data),
        "analyze_synapse_and_fabric_costs": lambda: _analyze_synapse_and_fabric_costs(raw_data),
        "analyze_sql_migration_opportunity": lambda: _analyze_sql_migration_opportunity(
            raw_data, tool_input.get("include_managed_instance", True)
        ),
    }
    handler = handlers.get(tool_name)
    if handler is None:
        return json.dumps({"error": f"Unknown tool: {tool_name}"})
    try:
        result = handler()
        return json.dumps(result, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Handler implementations
# ---------------------------------------------------------------------------


def _get_right_sizing_candidates(
    structured_report: StructuredReport,
    raw_data: RawAzureData,
    cpu_threshold_percent: float,
) -> list[dict]:
    candidates = []
    for vm in structured_report.underutilized_resources:
        if vm.avg_cpu_percent is None or vm.avg_cpu_percent >= cpu_threshold_percent:
            continue

        # Find VM size from inventory
        vm_info = next(
            (v for v in structured_report.vm_inventory if v.get("resource_id") == vm.resource_id),
            None,
        )
        current_size = vm_info.get("vm_size") if vm_info else None
        recommended_size = VM_DOWNSIZE_MAP.get(current_size) if current_size else None

        # Estimate ~50% savings on downsizing one tier (halving vCPUs ≈ halving cost)
        estimated_savings = vm.monthly_cost_usd * 0.5 if recommended_size else 0.0

        candidates.append(
            {
                "resource_id": vm.resource_id,
                "name": vm.name,
                "resource_group": vm.resource_group,
                "current_size": current_size,
                "recommended_size": recommended_size,
                "avg_cpu_percent": vm.avg_cpu_percent,
                "current_monthly_cost_usd": round(vm.monthly_cost_usd, 2),
                "estimated_monthly_savings_usd": round(estimated_savings, 2),
                "note": "No downsize mapping available" if not recommended_size else None,
            }
        )

    return sorted(candidates, key=lambda x: x["estimated_monthly_savings_usd"], reverse=True)


def _calculate_reservation_savings(
    structured_report: StructuredReport,
    raw_data: RawAzureData,
    min_monthly_cost_usd: float,
) -> dict:
    # Only recommend RIs for VMs that are running consistently
    eligible_vms = [
        vm for vm in structured_report.vm_inventory
        if vm.get("power_state") == "running"
        and vm.get("monthly_cost_usd", 0) >= min_monthly_cost_usd
    ]

    total_payg_monthly = sum(v.get("monthly_cost_usd", 0) for v in eligible_vms)

    # Determine discount rate based on VM family
    def get_discount(vm_size: str | None) -> float:
        if not vm_size:
            return RI_DISCOUNT_RATES["default"]
        for prefix, rate in RI_DISCOUNT_RATES.items():
            if prefix != "default" and vm_size.startswith(prefix):
                return rate
        return RI_DISCOUNT_RATES["default"]

    vm_recommendations = []
    total_savings = 0.0
    for vm in eligible_vms:
        discount = get_discount(vm.get("vm_size"))
        monthly_savings = vm.get("monthly_cost_usd", 0) * discount
        total_savings += monthly_savings
        vm_recommendations.append(
            {
                "name": vm.get("name"),
                "resource_group": vm.get("resource_group"),
                "vm_size": vm.get("vm_size"),
                "current_monthly_cost_usd": round(vm.get("monthly_cost_usd", 0), 2),
                "ri_discount_rate": f"{discount:.0%}",
                "estimated_monthly_savings_usd": round(monthly_savings, 2),
            }
        )

    return {
        "eligible_vm_count": len(eligible_vms),
        "total_payg_monthly_spend_usd": round(total_payg_monthly, 2),
        "total_estimated_monthly_savings_usd": round(total_savings, 2),
        "total_estimated_annual_savings_usd": round(total_savings * 12, 2),
        "commitment_term": "1-year",
        "vm_recommendations": sorted(
            vm_recommendations, key=lambda x: x["estimated_monthly_savings_usd"], reverse=True
        )[:20],
    }


def _calculate_savings_plan_estimate(
    structured_report: StructuredReport, commitment_term_years: int
) -> dict:
    # Compute total monthly compute spend (VMs + related services)
    compute_services = {"Virtual Machines", "Compute", "Azure Compute"}
    compute_monthly = sum(
        s.total_cost_usd
        for s in structured_report.cost_by_service
        if any(keyword.lower() in s.service_name.lower() for keyword in compute_services)
    )

    if compute_monthly == 0:
        compute_monthly = structured_report.total_spend_usd * 0.6  # fallback estimate

    term = commitment_term_years if commitment_term_years in SAVINGS_PLAN_DISCOUNTS else 1
    discount = SAVINGS_PLAN_DISCOUNTS[term]
    monthly_savings = compute_monthly * discount
    annual_savings = monthly_savings * 12

    # Commitment = payg - savings = payg * (1 - discount)
    hourly_commitment = (compute_monthly * (1 - discount)) / (24 * 30)

    return {
        "total_compute_monthly_spend_usd": round(compute_monthly, 2),
        "commitment_term_years": term,
        "savings_plan_discount": f"{discount:.0%}",
        "recommended_hourly_commitment_usd": round(hourly_commitment, 4),
        "estimated_monthly_savings_usd": round(monthly_savings, 2),
        "estimated_annual_savings_usd": round(annual_savings, 2),
        "note": (
            f"Azure Compute Savings Plan {term}-year commitment. "
            "Applies to VMs, AKS, Azure Functions Premium, App Service."
        ),
    }


def _get_orphaned_resource_cleanup_plan(structured_report: StructuredReport) -> dict:
    if not structured_report.orphaned_resources:
        return {"orphaned_resources": [], "total_monthly_savings_usd": 0}

    by_type: dict[str, list] = defaultdict(list)
    for r in structured_report.orphaned_resources:
        by_type[r.reason].append(r)

    cleanup_groups = []
    total_savings = 0.0

    type_instructions = {
        "unattached_disk": {
            "title": "Delete unattached managed disks",
            "steps": [
                "Review disk contents (snapshot if needed): az disk show --ids <disk_id>",
                "Create snapshot if data might be needed: az snapshot create --source <disk_id>",
                "Delete disk: az disk delete --ids <disk_id> --yes",
                "Verify deletion in Azure Portal under Disks blade",
            ],
        },
        "unused_public_ip": {
            "title": "Release unused public IP addresses",
            "steps": [
                "Confirm IP is truly unused: az network public-ip show --ids <ip_id>",
                "Check for any DNS records pointing to this IP before releasing",
                "Delete IP: az network public-ip delete --ids <ip_id>",
            ],
        },
        "empty_nsg": {
            "title": "Remove empty network security groups",
            "steps": [
                "Confirm NSG has no associated subnets or NICs: az network nsg show --ids <nsg_id>",
                "Delete NSG: az network nsg delete --ids <nsg_id>",
            ],
        },
        "orphaned_resource": {
            "title": "Review and clean up orphaned resources",
            "steps": [
                "Review each resource in Azure Portal",
                "Confirm resource is no longer needed",
                "Delete: az resource delete --ids <resource_id>",
            ],
        },
    }

    for reason, resources in by_type.items():
        group_cost = sum(r.estimated_monthly_cost_usd for r in resources)
        total_savings += group_cost
        instructions = type_instructions.get(reason, type_instructions["orphaned_resource"])
        cleanup_groups.append(
            {
                "reason": reason,
                "title": instructions["title"],
                "resource_count": len(resources),
                "total_monthly_cost_usd": round(group_cost, 2),
                "resources": [
                    {"name": r.name, "resource_id": r.resource_id, "resource_group": r.resource_group}
                    for r in resources[:10]
                ],
                "steps": instructions["steps"],
            }
        )

    return {
        "orphaned_groups": sorted(
            cleanup_groups, key=lambda x: x["total_monthly_cost_usd"], reverse=True
        ),
        "total_orphaned_resources": len(structured_report.orphaned_resources),
        "total_estimated_monthly_savings_usd": round(total_savings, 2),
    }


def _analyze_cost_anomalies(raw_data: RawAzureData, spike_threshold_percent: float) -> list[dict]:
    # Split cost entries into recent (last 7 days) vs older
    now = datetime.now(timezone.utc)
    recent_entries = []
    baseline_entries = []

    for entry in raw_data.cost_entries:
        try:
            date_str = str(entry.date)
            # Handle both "YYYYMMDD" and "YYYY-MM-DD" formats
            if len(date_str) == 8:
                entry_date = datetime(
                    int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]), tzinfo=timezone.utc
                )
            else:
                parts = date_str[:10].split("-")
                entry_date = datetime(int(parts[0]), int(parts[1]), int(parts[2]), tzinfo=timezone.utc)

            days_ago = (now - entry_date).days
            if days_ago <= 7:
                recent_entries.append(entry)
            else:
                baseline_entries.append(entry)
        except Exception:
            baseline_entries.append(entry)

    # Compute daily rates per service
    def daily_rate(entries: list, days: int) -> dict[str, float]:
        totals: dict[str, float] = defaultdict(float)
        for e in entries:
            totals[e.service_name] += e.cost_usd
        return {k: v / max(days, 1) for k, v in totals.items()}

    recent_rates = daily_rate(recent_entries, 7)
    baseline_days = max(raw_data.lookback_days - 7, 1)
    baseline_rates = daily_rate(baseline_entries, baseline_days)

    anomalies = []
    for service, recent_rate in recent_rates.items():
        baseline_rate = baseline_rates.get(service, 0)
        if baseline_rate == 0:
            continue
        pct_change = (recent_rate - baseline_rate) / baseline_rate * 100
        if pct_change >= spike_threshold_percent:
            anomalies.append(
                {
                    "service_name": service,
                    "baseline_daily_rate_usd": round(baseline_rate, 4),
                    "recent_daily_rate_usd": round(recent_rate, 4),
                    "spike_percent": round(pct_change, 1),
                    "absolute_delta_usd_per_day": round(recent_rate - baseline_rate, 4),
                }
            )

    return sorted(anomalies, key=lambda x: x["spike_percent"], reverse=True)


def _get_performance_tracking_report(
    raw_data: RawAzureData, structured_report: StructuredReport, sort_by: str
) -> list[dict]:
    vm_metrics: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    for sample in raw_data.vm_metrics:
        if sample.average is not None:
            vm_metrics[sample.resource_id][sample.metric_name].append(sample.average)

    cost_by_vm = {v["resource_id"]: v["monthly_cost_usd"] for v in structured_report.vm_inventory}

    report = []
    for vm_id, metrics in vm_metrics.items():
        def avg(vals: list[float]) -> float | None:
            return round(sum(vals) / len(vals), 2) if vals else None

        cpu_vals = metrics.get("Percentage CPU", [])
        mem_vals = metrics.get("Available Memory Bytes", [])
        net_in = metrics.get("Network In Total", [])
        net_out = metrics.get("Network Out Total", [])
        disk_r = metrics.get("Disk Read Bytes", [])
        disk_w = metrics.get("Disk Write Bytes", [])

        vm_info = next((v for v in structured_report.vm_inventory if v["resource_id"] == vm_id), {})
        report.append(
            {
                "resource_id": vm_id,
                "name": vm_info.get("name", vm_id.split("/")[-1]),
                "vm_size": vm_info.get("vm_size"),
                "resource_group": vm_info.get("resource_group"),
                "monthly_cost_usd": cost_by_vm.get(vm_id, 0),
                "cpu_avg_percent": avg(cpu_vals),
                "cpu_max_percent": round(max(cpu_vals), 2) if cpu_vals else None,
                "memory_available_avg_gb": round(avg(mem_vals) / 1e9, 2) if avg(mem_vals) else None,
                "network_in_avg_mbps": round(avg(net_in) / 1e6, 3) if avg(net_in) else None,
                "network_out_avg_mbps": round(avg(net_out) / 1e6, 3) if avg(net_out) else None,
                "disk_read_avg_mbps": round(avg(disk_r) / 1e6, 3) if avg(disk_r) else None,
                "disk_write_avg_mbps": round(avg(disk_w) / 1e6, 3) if avg(disk_w) else None,
            }
        )

    key_map = {"cpu_avg": "cpu_avg_percent", "cost": "monthly_cost_usd"}
    sort_key = key_map.get(sort_by, "cpu_avg_percent")
    return sorted(report, key=lambda x: x.get(sort_key) or 0)


# ---------------------------------------------------------------------------
# AI / Cognitive Services optimizer
# ---------------------------------------------------------------------------

# Approximate monthly cost ratios for common model transitions
# Based on Azure OpenAI public pricing (tokens/hour)
MODEL_DOWNGRADE_MAP = [
    {
        "from_model": "gpt-4",
        "to_model": "gpt-4o",
        "savings_pct": 0.50,
        "note": "GPT-4o is 50% cheaper than GPT-4 with similar quality",
    },
    {
        "from_model": "gpt-4o",
        "to_model": "gpt-4o-mini",
        "savings_pct": 0.94,
        "note": "GPT-4o-mini is ~16x cheaper for tasks not requiring full reasoning",
    },
    {
        "from_model": "gpt-4-turbo",
        "to_model": "gpt-4o",
        "savings_pct": 0.80,
        "note": "GPT-4o is 5x cheaper than GPT-4-Turbo with comparable performance",
    },
    {
        "from_model": "text-embedding-ada-002",
        "to_model": "text-embedding-3-small",
        "savings_pct": 0.80,
        "note": "text-embedding-3-small is 5x cheaper and higher quality",
    },
]

# Cognitive Services SKU upgrade paths (F0 = free tier, S0 = standard)
CS_SKU_NOTES = {
    "F0": "Free tier — limited to 5,000 transactions/month. Upgrade to S0 when in production.",
    "S0": "Standard tier — pay-per-use. Good for most workloads.",
    "S1": "Check if S0 meets needs — S1 has reserved capacity at higher cost.",
}


def _analyze_ai_model_costs(raw_data: RawAzureData) -> dict:
    if not raw_data.ai_services:
        # Fall back to resource list
        ai_resources = [
            r for r in raw_data.resources
            if r.type.lower().startswith("microsoft.cognitiveservices")
            or r.type.lower().startswith("microsoft.machinelearningservices")
        ]
        return {
            "ai_services_found": len(ai_resources),
            "ai_service_names": [r.name for r in ai_resources],
            "total_ai_monthly_cost_usd": sum(
                e.cost_usd for e in raw_data.cost_entries
                if any(kw in e.service_name.lower() for kw in
                       ["cognitive", "openai", "foundry", "machine learning", "ai"])
            ),
            "model_downgrade_opportunities": MODEL_DOWNGRADE_MAP,
            "general_recommendations": [
                "Enable token usage monitoring in Azure OpenAI Studio",
                "Use GPT-4o-mini for classification, summarization, and extraction tasks",
                "Reserve capacity for predictable high-volume workloads (saves 30-50%)",
                "Consolidate multiple Cognitive Services accounts to reduce management overhead",
                "Enable content caching to avoid redundant API calls",
            ],
            "note": "No detailed deployment data available — SDK access may be limited",
        }

    findings = []
    total_ai_cost = sum(s.monthly_cost_usd for s in raw_data.ai_services)
    downgrade_opportunities = []

    for svc in raw_data.ai_services:
        svc_findings: dict = {
            "name": svc.name,
            "kind": svc.kind,
            "sku": svc.sku_name,
            "resource_group": svc.resource_group,
            "monthly_cost_usd": round(svc.monthly_cost_usd, 2),
            "deployments": svc.deployments,
            "issues": [],
            "recommendations": [],
        }

        # SKU check
        if svc.sku_name in CS_SKU_NOTES:
            svc_findings["issues"].append(CS_SKU_NOTES[svc.sku_name])

        # Deployment model analysis
        for dep in svc.deployments:
            model_name = (dep.get("model") or "").lower()
            capacity = dep.get("capacity_k_tpm") or 0

            for transition in MODEL_DOWNGRADE_MAP:
                if transition["from_model"] in model_name:
                    estimated_savings = svc.monthly_cost_usd * transition["savings_pct"]
                    downgrade_opportunities.append({
                        "service": svc.name,
                        "deployment": dep.get("name"),
                        "current_model": dep.get("model"),
                        "recommended_model": transition["to_model"],
                        "savings_percent": f"{transition['savings_pct']:.0%}",
                        "estimated_monthly_savings_usd": round(estimated_savings, 2),
                        "note": transition["note"],
                    })

            # Over-provisioned capacity check
            if capacity and capacity > 100:
                svc_findings["recommendations"].append(
                    f"Deployment '{dep.get('name')}' has {capacity}K TPM capacity — "
                    "reduce if average utilization < 50%"
                )

        findings.append(svc_findings)

    return {
        "ai_services_found": len(raw_data.ai_services),
        "total_ai_monthly_cost_usd": round(total_ai_cost, 2),
        "services": findings,
        "model_downgrade_opportunities": downgrade_opportunities,
        "general_best_practices": [
            "Use GPT-4o-mini for: classification, extraction, summarization, Q&A on structured data",
            "Use GPT-4o for: complex reasoning, code generation, nuanced analysis",
            "Enable Provisioned Throughput (PTU) for steady-state workloads > 100K tokens/min",
            "Use Azure OpenAI batch API for non-real-time workloads (50% discount)",
            "Consolidate multiple OpenAI accounts into one with multiple deployments",
            "Monitor with Azure OpenAI usage metrics in Azure Monitor",
        ],
    }


# ---------------------------------------------------------------------------
# Azure Data Factory optimizer
# ---------------------------------------------------------------------------

ADF_COMPUTE_TYPE_COST = {
    "General": 0.174,   # $/DIU-hour — General Purpose
    "MemoryOptimized": 0.348,   # $/DIU-hour
    "ComputeOptimized": 0.0948,  # $/DIU-hour
}


def _analyze_data_factory_costs(raw_data: RawAzureData) -> dict:
    adf_cost = sum(
        e.cost_usd for e in raw_data.cost_entries
        if "data factory" in e.service_name.lower() or "datafactory" in e.meter_category.lower()
    )

    if not raw_data.data_factories:
        adf_resources = [
            r for r in raw_data.resources
            if r.type.lower() == "microsoft.datafactory/factories"
        ]
        return {
            "factories_found": len(adf_resources),
            "factory_names": [r.name for r in adf_resources],
            "total_adf_monthly_cost_usd": round(adf_cost, 2),
            "recommendations": [
                "Use Azure IR (auto-resolve) instead of fixed-size IR for variable workloads",
                "Enable 'TTL' (time-to-live) on Azure IR to reuse warm clusters — saves 1-2 min startup per pipeline",
                "Switch batch/non-urgent pipelines to scheduled triggers during off-peak hours",
                "Use Copy Activity with staging for large data movements (cheaper than data flow)",
                "Consider migrating simple pipelines to Microsoft Fabric Pipelines (included in Fabric capacity)",
                "Set pipeline retry limits to avoid runaway costs from failed re-runs",
            ],
            "note": "No Integration Runtime details available — SDK access may be limited",
        }

    findings = []
    for factory in raw_data.data_factories:
        issues = []
        recommendations = []

        for ir in factory.integration_runtimes:
            ir_type = ir.get("type", "")
            compute_type = ir.get("compute_type", "General")
            cores = ir.get("core_count")

            if ir_type == "SelfHosted":
                issues.append(f"Self-hosted IR '{ir['name']}' — ensure it is not idle 24/7")

            if compute_type == "MemoryOptimized" and cores and cores > 16:
                issues.append(
                    f"IR '{ir['name']}' uses {cores}-core Memory Optimized compute "
                    f"(${ADF_COMPUTE_TYPE_COST['MemoryOptimized'] * cores:.2f}/hr) — "
                    "validate if memory-optimized is required"
                )
                recommendations.append(
                    f"Switch IR '{ir['name']}' to General Purpose if data transformations "
                    "are not memory-intensive — saves ~50% on compute cost"
                )

        findings.append({
            "name": factory.name,
            "resource_group": factory.resource_group,
            "monthly_cost_usd": round(factory.monthly_cost_usd, 2),
            "integration_runtimes": factory.integration_runtimes,
            "issues": issues,
            "recommendations": recommendations,
        })

    return {
        "factories_found": len(raw_data.data_factories),
        "total_adf_monthly_cost_usd": round(adf_cost, 2),
        "factories": findings,
        "best_practices": [
            "Use 'Auto' core count for Azure IR — ADF picks optimal size per activity",
            "Enable TTL on IR (e.g. 10 min) to reuse warm clusters across sequential activities",
            "Use scheduled triggers instead of tumbling window for batch workloads",
            "Replace ADF Data Flows with Databricks notebooks for complex transformations (often cheaper at scale)",
            "Consider migrating to Microsoft Fabric Pipelines — included in Fabric F SKU capacity",
        ],
    }


# ---------------------------------------------------------------------------
# Azure Databricks optimizer
# ---------------------------------------------------------------------------

def _analyze_databricks_usage(raw_data: RawAzureData) -> dict:
    databricks_cost = sum(
        e.cost_usd for e in raw_data.cost_entries
        if "databricks" in e.service_name.lower() or "databricks" in e.meter_category.lower()
    )

    databricks_resources = [
        svc for svc in raw_data.analytics_services
        if svc.resource_type == "Databricks"
    ]

    if not databricks_resources:
        # Check raw resources
        db_raw = [r for r in raw_data.resources
                  if r.type.lower() == "microsoft.databricks/workspaces"]
        return {
            "databricks_workspaces_found": len(db_raw),
            "workspace_names": [r.name for r in db_raw],
            "total_databricks_monthly_cost_usd": round(databricks_cost, 2),
            "optimization_opportunities": [
                {
                    "title": "Enable spot/low-priority instances for all job clusters",
                    "estimated_savings_pct": "60-90%",
                    "effort": "low",
                    "steps": [
                        "In cluster policy or job config, set: spark.databricks.cluster.profile singleNode",
                        "Set node_type to spot-compatible instance (e.g. Standard_D4s_v3)",
                        "Enable spot with fallback: azure_attributes.spot_bid_max_price = -1",
                    ],
                },
                {
                    "title": "Set auto-termination on all-purpose clusters",
                    "estimated_savings_pct": "30-50%",
                    "effort": "low",
                    "steps": [
                        "Go to Clusters → Edit → Auto Termination → set to 30 minutes",
                        "Or via API: clusters/edit with autotermination_minutes: 30",
                        "Use cluster policies to enforce auto-termination org-wide",
                    ],
                },
                {
                    "title": "Use job clusters instead of all-purpose clusters for automated jobs",
                    "estimated_savings_pct": "40-70%",
                    "effort": "medium",
                    "steps": [
                        "Audit workflows: Workflows → Jobs → check cluster type per job",
                        "Change job cluster type from 'Existing cluster' to 'New job cluster'",
                        "Job clusters start/stop per run — no idle cost",
                    ],
                },
                {
                    "title": "Use cluster pools to reduce startup time on job clusters",
                    "estimated_savings_pct": "10-20% (via faster pipelines)",
                    "effort": "medium",
                    "steps": [
                        "Create a pool: Compute → Pools → Create Pool",
                        "Set min idle instances to 1-2 for fast cold-start",
                        "Attach job clusters to the pool",
                    ],
                },
                {
                    "title": "Enable Photon engine for SQL and ETL workloads",
                    "estimated_savings_pct": "variable (3-10x faster = less DBU hours)",
                    "effort": "low",
                    "steps": [
                        "Select a Photon-enabled runtime (13.x+) when creating clusters",
                        "Photon accelerates SQL queries and Delta Lake operations significantly",
                    ],
                },
                {
                    "title": "Consider migrating to Microsoft Fabric Spark for new workloads",
                    "estimated_savings_pct": "variable",
                    "effort": "high",
                    "steps": [
                        "Evaluate Fabric Spark (included in Fabric F64+ capacity)",
                        "For net-new pipelines: use Fabric Notebooks instead of Databricks",
                        "Existing Databricks notebooks can be migrated with minimal changes",
                    ],
                },
            ],
        }

    findings = []
    for ws in databricks_resources:
        findings.append({
            "name": ws.name,
            "resource_group": ws.resource_group,
            "location": ws.location,
            "sku": ws.sku_name,
            "monthly_cost_usd": round(ws.monthly_cost_usd, 2),
        })

    return {
        "databricks_workspaces_found": len(databricks_resources),
        "total_databricks_monthly_cost_usd": round(databricks_cost, 2),
        "workspaces": findings,
        "key_optimizations": [
            "Spot instances for job clusters: 60-90% savings",
            "Auto-termination on all-purpose clusters: 30-50% savings",
            "Job clusters vs all-purpose: 40-70% savings on automated workloads",
        ],
    }


# ---------------------------------------------------------------------------
# Synapse + Fabric optimizer
# ---------------------------------------------------------------------------

# Synapse DWU pricing (approximate, East US)
SYNAPSE_DWU_MONTHLY = {
    "DW100c": 75,
    "DW200c": 150,
    "DW300c": 225,
    "DW400c": 300,
    "DW500c": 375,
    "DW1000c": 750,
    "DW1500c": 1125,
    "DW2000c": 1500,
    "DW3000c": 2250,
    "DW6000c": 4500,
}

# Fabric capacity SKU monthly costs (approximate)
FABRIC_SKU_MONTHLY = {
    "F2": 262,
    "F4": 524,
    "F8": 1048,
    "F16": 2096,
    "F32": 4192,
    "F64": 8384,
    "F128": 16768,
    "F256": 33536,
    "F512": 67072,
    "F1024": 134144,
    "F2048": 268288,
}


def _analyze_synapse_and_fabric_costs(raw_data: RawAzureData) -> dict:
    synapse_cost = sum(
        e.cost_usd for e in raw_data.cost_entries
        if "synapse" in e.service_name.lower() or "synapse" in e.meter_category.lower()
    )
    fabric_cost = sum(
        e.cost_usd for e in raw_data.cost_entries
        if "fabric" in e.service_name.lower() or "fabric" in e.meter_category.lower()
    )

    synapse_services = [s for s in raw_data.analytics_services
                        if s.resource_type in ("Synapse", "SynapseSQLPool", "SynapseSparkPool")]
    fabric_services = [s for s in raw_data.analytics_services if s.resource_type == "Fabric"]

    synapse_findings = []
    for svc in synapse_services:
        dw_units = svc.properties.get("dw_units")
        status = svc.properties.get("status")
        monthly = SYNAPSE_DWU_MONTHLY.get(str(dw_units), svc.monthly_cost_usd)

        recommendations = []
        if status and status.lower() == "online":
            recommendations.append(
                "Enable auto-pause: dedicated SQL pools incur cost even when idle. "
                "Set auto-pause after 60 minutes of inactivity."
            )
        if dw_units and dw_units in ("DW1000c", "DW1500c", "DW2000c"):
            lower = f"DW{int(dw_units.replace('DW','').replace('c','')) // 2}c"
            savings = monthly - SYNAPSE_DWU_MONTHLY.get(lower, monthly // 2)
            recommendations.append(
                f"Consider scaling down from {dw_units} to {lower} during off-peak hours "
                f"(saves ~${savings:.0f}/month if done 50% of the time)"
            )

        synapse_findings.append({
            "name": svc.name,
            "type": svc.resource_type,
            "dw_units": dw_units,
            "status": status,
            "monthly_cost_usd": round(svc.monthly_cost_usd, 2),
            "recommendations": recommendations,
        })

    fabric_findings = []
    for cap in fabric_services:
        sku = cap.sku_name or cap.properties.get("sku")
        monthly = FABRIC_SKU_MONTHLY.get(str(sku), cap.monthly_cost_usd)
        fabric_findings.append({
            "name": cap.name,
            "sku": sku,
            "monthly_cost_usd": round(monthly, 2),
            "recommendation": (
                f"Consider reserved capacity for {sku} — saves ~17% (1-year) or 33% (3-year)"
                if sku else "Enable auto-scale or downgrade SKU during off-peak hours"
            ),
        })

    return {
        "synapse_resources_found": len(synapse_services),
        "fabric_capacities_found": len(fabric_services),
        "total_synapse_monthly_cost_usd": round(synapse_cost, 2),
        "total_fabric_monthly_cost_usd": round(fabric_cost, 2),
        "synapse_findings": synapse_findings,
        "fabric_findings": fabric_findings,
        "synapse_best_practices": [
            "Enable auto-pause on dedicated SQL pools (biggest cost lever — saves 100% during paused hours)",
            "Use serverless SQL pool for ad-hoc queries — pay only per TB scanned ($5/TB)",
            "Scale DWU down during off-peak: az synapse sql pool update --name <pool> --performance-level DW500c",
            "Use workload management (workload groups) to prevent runaway queries consuming all DWU",
            "Consider migrating to Microsoft Fabric — Synapse is being superseded",
        ],
        "fabric_best_practices": [
            "Use F2/F4 for dev/test, F64+ for production workloads",
            "Fabric includes Spark, Pipelines, Power BI, and Data Warehouse — consolidate spend",
            "Enable capacity auto-scaling to handle peak loads without over-provisioning",
            "Use reserved capacity pricing for predictable workloads (17-33% discount)",
            "Pause Fabric capacity overnight for dev environments: az fabric capacity suspend",
        ],
        "synapse_vs_fabric_note": (
            "Microsoft Fabric is the strategic successor to Synapse Analytics. "
            "New analytics workloads should target Fabric. "
            "Migration from Synapse to Fabric can reduce costs by 20-40% through consolidation."
        ),
    }


# ---------------------------------------------------------------------------
# SQL Migration opportunity analyzer
# ---------------------------------------------------------------------------

# Rough monthly cost estimates for Azure SQL vs open-source equivalents
# Based on Azure public pricing for East US region
SQL_MIGRATION_ESTIMATES = {
    "AzureSQL": {
        "Standard_S3_100DTU": {"azure_sql": 150, "mysql_flexible": 55, "postgres_flexible": 55},
        "Standard_S4_200DTU": {"azure_sql": 300, "mysql_flexible": 100, "postgres_flexible": 100},
        "Standard_S6_400DTU": {"azure_sql": 600, "mysql_flexible": 180, "postgres_flexible": 180},
        "GeneralPurpose_4vcore": {"azure_sql": 370, "mysql_flexible": 140, "postgres_flexible": 140},
        "GeneralPurpose_8vcore": {"azure_sql": 740, "mysql_flexible": 270, "postgres_flexible": 270},
        "BusinessCritical_4vcore": {"azure_sql": 900, "mysql_flexible": 140, "postgres_flexible": 140},
    }
}

SQL_COMPAT_NOTES = {
    "AzureSQL": {
        "mysql": {
            "compatibility": "Medium",
            "considerations": [
                "T-SQL to MySQL SQL dialect migration required (stored procedures, triggers)",
                "IDENTITY columns → AUTO_INCREMENT",
                "No linked servers in MySQL",
                "Assess use of SQL Server-specific functions (CHARINDEX, ISNULL, etc.)",
                "Tools: SSMA for MySQL (SQL Server Migration Assistant)",
            ],
        },
        "postgresql": {
            "compatibility": "Medium-High",
            "considerations": [
                "T-SQL to PL/pgSQL migration for stored procedures",
                "Most standard SQL is compatible",
                "Better JSON support in PostgreSQL than MySQL",
                "Tools: SSMA for PostgreSQL, pgLoader, AWS Schema Conversion Tool",
                "Azure Database for PostgreSQL Flexible Server is fully managed",
            ],
        },
    },
    "ManagedInstance": {
        "mysql": {
            "compatibility": "Low",
            "considerations": [
                "Managed Instance is used for near-100% SQL Server compatibility",
                "High compatibility gap to MySQL — not recommended unless app is rewritten",
            ],
        },
        "postgresql": {
            "compatibility": "Low-Medium",
            "considerations": [
                "Managed Instance features (CLR, linked servers, SQL Agent) not available in PostgreSQL",
                "Consider only if application uses basic SQL features",
            ],
        },
    },
}


def _analyze_sql_migration_opportunity(
    raw_data: RawAzureData, include_managed_instance: bool
) -> dict:
    sql_cost = sum(
        e.cost_usd for e in raw_data.cost_entries
        if any(kw in e.service_name.lower() for kw in ["sql", "database"])
    )

    if not raw_data.sql_resources:
        # Check raw resources
        sql_raw = [
            r for r in raw_data.resources
            if any(r.type.lower().startswith(p) for p in [
                "microsoft.sql", "microsoft.dbformysql", "microsoft.dbforpostgresql"
            ])
        ]
        return {
            "sql_resources_found": len(sql_raw),
            "resource_names": [r.name for r in sql_raw],
            "total_sql_monthly_cost_usd": round(sql_cost, 2),
            "migration_opportunity": len(sql_raw) > 0,
            "general_migration_guidance": {
                "azure_sql_to_postgresql": {
                    "typical_cost_savings_pct": "50-70%",
                    "compatibility": "Medium-High",
                    "recommended_target": "Azure Database for PostgreSQL Flexible Server",
                    "migration_tool": "SSMA for PostgreSQL",
                    "steps": [
                        "1. Assess: Run SSMA to identify compatibility issues",
                        "2. Convert schema: Use SSMA schema converter",
                        "3. Migrate data: Use Azure Database Migration Service (DMS)",
                        "4. Test application: Run regression tests",
                        "5. Cut over: Use DMS for minimal downtime migration",
                    ],
                },
                "azure_sql_to_mysql": {
                    "typical_cost_savings_pct": "50-65%",
                    "compatibility": "Medium",
                    "recommended_target": "Azure Database for MySQL Flexible Server",
                    "migration_tool": "SSMA for MySQL",
                    "steps": [
                        "1. Assess: Run SSMA to identify T-SQL compatibility issues",
                        "2. Rewrite stored procedures in MySQL syntax",
                        "3. Migrate data: Use Azure DMS or mysqldump",
                        "4. Validate application queries",
                        "5. Cut over with minimal downtime using DMS online migration",
                    ],
                },
            },
            "note": "No detailed SQL resource data — install azure-mgmt-sql for detailed analysis",
        }

    candidates = []
    total_potential_savings = 0.0

    for sql in raw_data.sql_resources:
        if sql.resource_type == "ManagedInstance" and not include_managed_instance:
            continue

        compat = SQL_COMPAT_NOTES.get(sql.resource_type, SQL_COMPAT_NOTES.get("AzureSQL", {}))
        current_cost = sql.monthly_cost_usd

        # Estimate open-source equivalent cost (roughly 50-70% cheaper)
        mysql_estimated = round(current_cost * 0.35, 2)
        postgres_estimated = round(current_cost * 0.35, 2)

        savings_to_mysql = round(current_cost - mysql_estimated, 2)
        savings_to_postgres = round(current_cost - postgres_estimated, 2)
        total_potential_savings += max(savings_to_mysql, savings_to_postgres)

        migration_complexity = (
            "High" if sql.resource_type == "ManagedInstance"
            else "Medium" if (sql.tier or "").lower() in ("businesscritical", "premium")
            else "Low-Medium"
        )

        candidates.append({
            "name": sql.name,
            "resource_type": sql.resource_type,
            "resource_group": sql.resource_group,
            "current_sku": sql.sku_name,
            "tier": sql.tier,
            "vcores_or_dtus": sql.dtu_or_vcores,
            "storage_gb": sql.storage_gb,
            "monthly_cost_usd": round(current_cost, 2),
            "mysql_estimated_monthly_usd": mysql_estimated,
            "postgres_estimated_monthly_usd": postgres_estimated,
            "savings_to_mysql_usd": savings_to_mysql,
            "savings_to_postgres_usd": savings_to_postgres,
            "migration_complexity": migration_complexity,
            "recommended_target": "PostgreSQL Flexible Server" if migration_complexity != "High" else "Azure SQL MI (keep)",
            "mysql_compatibility": compat.get("mysql", {}),
            "postgresql_compatibility": compat.get("postgresql", {}),
        })

    # Also flag existing MySQL/PostgreSQL as best practice confirmation
    oss_existing = [s for s in raw_data.sql_resources
                    if s.resource_type in ("MySQL", "MySQLFlexible", "PostgreSQL", "PostgreSQLFlexible")]

    return {
        "sql_resources_analyzed": len(candidates),
        "existing_oss_databases": len(oss_existing),
        "total_sql_monthly_cost_usd": round(sql_cost, 2),
        "total_potential_monthly_savings_usd": round(total_potential_savings, 2),
        "migration_candidates": candidates,
        "recommendation_summary": (
            f"Found {len(candidates)} Azure SQL resource(s) that could be migrated to "
            "MySQL or PostgreSQL Flexible Server for 50-70% cost reduction. "
            "PostgreSQL is recommended for most workloads due to better T-SQL compatibility."
            if candidates else
            "No Azure SQL migration candidates found in this scope."
        ),
        "azure_database_migration_service": (
            "Azure Database Migration Service (DMS) provides free online migration "
            "with minimal downtime. Use it for production cutovers."
        ),
    }
