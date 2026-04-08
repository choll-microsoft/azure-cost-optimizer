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
