"""Tool definitions and handlers for the Structurer Agent (Agent 2)."""

import json
from collections import defaultdict

from ..models.raw_data import RawAzureData

STRUCTURER_TOOLS = [
    {
        "name": "aggregate_costs_by_service",
        "description": (
            "Aggregates cost entries by service name. Returns a list of services "
            "with total spend and percentage of total, sorted descending by cost. "
            "Use this to identify top spending services."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "top_n": {
                    "type": "integer",
                    "description": "Return only the top N services by cost. Default 20.",
                    "default": 20,
                }
            },
            "required": [],
        },
    },
    {
        "name": "aggregate_costs_by_resource_group",
        "description": (
            "Aggregates cost entries by resource group. Returns resource groups "
            "with total spend and their top 3 services."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "identify_underutilized_vms",
        "description": (
            "Analyzes VM metrics to find underutilized VMs. A VM is considered "
            "underutilized if average CPU < threshold% over the lookback period. "
            "Returns list of VM resource IDs, average CPU, and their monthly cost."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cpu_threshold_percent": {
                    "type": "number",
                    "description": "CPU utilization threshold. VMs below this are flagged. Default: 10.0",
                    "default": 10.0,
                }
            },
            "required": [],
        },
    },
    {
        "name": "identify_orphaned_resources",
        "description": (
            "Returns the list of likely-orphaned resources detected by the collector "
            "(unattached disks, unused public IPs, empty NSGs, etc.) with their "
            "estimated monthly cost impact."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_advisor_cost_recommendations",
        "description": (
            "Returns Azure Advisor recommendations filtered to Cost category only, "
            "sorted by potential savings descending."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "min_savings_usd": {
                    "type": "number",
                    "description": "Only return recommendations with savings >= this value. Default: 0.",
                    "default": 0,
                }
            },
            "required": [],
        },
    },
    {
        "name": "get_vm_inventory_summary",
        "description": (
            "Returns a summary of all VMs: name, size, power state, OS, resource group, "
            "and monthly cost. Useful for right-sizing analysis."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "include_deallocated": {
                    "type": "boolean",
                    "description": "Include stopped/deallocated VMs in results. Default: true.",
                    "default": True,
                }
            },
            "required": [],
        },
    },
    {
        "name": "finalize_structured_report",
        "description": (
            "Called when you have gathered all the data you need. Saves the structured "
            "report. Pass the assembled structured data as JSON. This ends the structuring phase."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "report": {
                    "type": "object",
                    "description": "The complete structured report object as a JSON dict.",
                }
            },
            "required": ["report"],
        },
    },
]


def handle_structurer_tool(
    tool_name: str,
    tool_input: dict,
    raw_data: RawAzureData,
) -> str:
    """Dispatch structurer tool calls from Claude. Returns a JSON string."""
    handlers = {
        "aggregate_costs_by_service": lambda: _aggregate_costs_by_service(
            raw_data, tool_input.get("top_n", 20)
        ),
        "aggregate_costs_by_resource_group": lambda: _aggregate_costs_by_resource_group(raw_data),
        "identify_underutilized_vms": lambda: _identify_underutilized_vms(
            raw_data, tool_input.get("cpu_threshold_percent", 10.0)
        ),
        "identify_orphaned_resources": lambda: _identify_orphaned_resources(raw_data),
        "get_advisor_cost_recommendations": lambda: _get_advisor_cost_recommendations(
            raw_data, tool_input.get("min_savings_usd", 0)
        ),
        "get_vm_inventory_summary": lambda: _get_vm_inventory_summary(
            raw_data, tool_input.get("include_deallocated", True)
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


def _aggregate_costs_by_service(raw_data: RawAzureData, top_n: int) -> list[dict]:
    totals: dict[str, float] = defaultdict(float)
    for entry in raw_data.cost_entries:
        service = entry.service_name or "Unknown"
        totals[service] += entry.cost_usd

    grand_total = sum(totals.values()) or 1.0
    sorted_services = sorted(totals.items(), key=lambda x: x[1], reverse=True)[:top_n]

    return [
        {
            "service_name": name,
            "total_cost_usd": round(cost, 4),
            "percentage_of_total": round(cost / grand_total * 100, 2),
        }
        for name, cost in sorted_services
    ]


def _aggregate_costs_by_resource_group(raw_data: RawAzureData) -> list[dict]:
    rg_costs: dict[str, float] = defaultdict(float)
    rg_services: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for entry in raw_data.cost_entries:
        rg = entry.resource_group or "(none)"
        rg_costs[rg] += entry.cost_usd
        rg_services[rg][entry.service_name or "Unknown"] += entry.cost_usd

    result = []
    for rg, cost in sorted(rg_costs.items(), key=lambda x: x[1], reverse=True):
        top3 = sorted(rg_services[rg].items(), key=lambda x: x[1], reverse=True)[:3]
        result.append(
            {
                "resource_group": rg,
                "total_cost_usd": round(cost, 4),
                "top_services": [s[0] for s in top3],
            }
        )
    return result


def _identify_underutilized_vms(
    raw_data: RawAzureData, cpu_threshold_percent: float
) -> list[dict]:
    # Build per-VM average CPU from metric samples
    vm_cpu_samples: dict[str, list[float]] = defaultdict(list)
    for sample in raw_data.vm_metrics:
        if "CPU" in sample.metric_name and sample.average is not None:
            vm_cpu_samples[sample.resource_id].append(sample.average)

    # Build per-resource cost lookup
    cost_by_resource: dict[str, float] = defaultdict(float)
    for entry in raw_data.cost_entries:
        if entry.resource_id:
            cost_by_resource[entry.resource_id.lower()] += entry.cost_usd

    underutilized = []
    for vm_id, cpu_values in vm_cpu_samples.items():
        avg_cpu = sum(cpu_values) / len(cpu_values)
        if avg_cpu < cpu_threshold_percent:
            monthly_cost = cost_by_resource.get(vm_id.lower(), 0.0)
            # Find resource info
            vm_info = next(
                (r for r in raw_data.resources if r.resource_id.lower() == vm_id.lower()), None
            )
            underutilized.append(
                {
                    "resource_id": vm_id,
                    "name": vm_info.name if vm_info else vm_id.split("/")[-1],
                    "resource_group": vm_info.resource_group if vm_info else "",
                    "vm_size": vm_info.vm_size if vm_info else None,
                    "power_state": vm_info.power_state if vm_info else None,
                    "avg_cpu_percent": round(avg_cpu, 2),
                    "monthly_cost_usd": round(monthly_cost, 2),
                    "recommended_action": "resize" if monthly_cost > 50 else "evaluate",
                }
            )

    return sorted(underutilized, key=lambda x: x["monthly_cost_usd"], reverse=True)


def _identify_orphaned_resources(raw_data: RawAzureData) -> list[dict]:
    orphaned = []
    resource_map = {r.resource_id.lower(): r for r in raw_data.resources}
    cost_by_resource: dict[str, float] = defaultdict(float)
    for entry in raw_data.cost_entries:
        if entry.resource_id:
            cost_by_resource[entry.resource_id.lower()] += entry.cost_usd

    for orphan_id in raw_data.orphaned_resource_ids:
        info = resource_map.get(orphan_id.lower())
        resource_type = info.type if info else orphan_id.split("/")[-2] if "/" in orphan_id else ""
        name = info.name if info else orphan_id.split("/")[-1]
        rg = info.resource_group if info else ""

        reason_map = {
            "microsoft.compute/disks": "unattached_disk",
            "microsoft.network/publicipaddresses": "unused_public_ip",
            "microsoft.network/networksecuritygroups": "empty_nsg",
        }
        reason = reason_map.get(resource_type.lower(), "orphaned_resource")

        orphaned.append(
            {
                "resource_id": orphan_id,
                "name": name,
                "resource_type": resource_type,
                "resource_group": rg,
                "estimated_monthly_cost_usd": round(cost_by_resource.get(orphan_id.lower(), 0.0), 2),
                "reason": reason,
            }
        )
    return orphaned


def _get_advisor_cost_recommendations(
    raw_data: RawAzureData, min_savings_usd: float
) -> list[dict]:
    cost_recs = [
        r for r in raw_data.advisor_recommendations
        if r.category.lower() == "cost"
        and (r.potential_savings_usd or 0) >= min_savings_usd
    ]
    cost_recs.sort(key=lambda r: r.potential_savings_usd or 0, reverse=True)

    return [
        {
            "recommendation_id": r.recommendation_id,
            "category": r.category,
            "impact": r.impact,
            "short_description": r.short_description,
            "long_description": r.long_description,
            "resource_id": r.resource_id,
            "potential_savings_usd": r.potential_savings_usd,
            "savings_currency": r.savings_currency,
            "impacted_resource_type": r.impacted_resource_type,
        }
        for r in cost_recs
    ]


def _get_vm_inventory_summary(raw_data: RawAzureData, include_deallocated: bool) -> list[dict]:
    cost_by_resource: dict[str, float] = defaultdict(float)
    for entry in raw_data.cost_entries:
        if entry.resource_id:
            cost_by_resource[entry.resource_id.lower()] += entry.cost_usd

    vms = [r for r in raw_data.resources if r.type == "Microsoft.Compute/virtualMachines"]
    if not include_deallocated:
        vms = [v for v in vms if v.power_state not in ("deallocated", "stopped")]

    return [
        {
            "resource_id": vm.resource_id,
            "name": vm.name,
            "resource_group": vm.resource_group,
            "location": vm.location,
            "vm_size": vm.vm_size,
            "power_state": vm.power_state,
            "os_type": vm.os_type,
            "os_disk_size_gb": vm.os_disk_size_gb,
            "monthly_cost_usd": round(cost_by_resource.get(vm.resource_id.lower(), 0.0), 2),
            "tags": vm.tags,
        }
        for vm in vms
    ]
