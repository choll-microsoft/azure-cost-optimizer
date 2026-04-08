"""Agent 2: Structures raw Azure data into a normalized report using Azure OpenAI."""

import json
import uuid
from datetime import datetime, timezone

from rich.console import Console

from ..config import settings
from ..models.raw_data import RawAzureData
from ..models.structured import (
    CostByResourceGroup,
    CostByService,
    OrphanedResource,
    StructuredReport,
    UnderutilizedResource,
)
from ..openai_client import get_openai_client
from ..tools.structurer_tools import STRUCTURER_TOOLS, handle_structurer_tool

console = Console()

SYSTEM_PROMPT = """You are an Azure cost analysis specialist. You have access to raw Azure \
billing and resource data. Your job is to normalize and structure this data into a clean \
report by calling the provided functions.

Follow this sequence:
1. Call aggregate_costs_by_service (top 20) to identify top spending services
2. Call aggregate_costs_by_resource_group to understand cost distribution
3. Call identify_underutilized_vms (cpu_threshold_percent: 10) to find optimization targets
4. Call identify_orphaned_resources to find waste
5. Call get_advisor_cost_recommendations (min_savings_usd: 10) for Azure's own recommendations
6. Call get_vm_inventory_summary to compile the VM inventory
7. Call finalize_structured_report with all gathered data assembled into a report dict

Be thorough but concise. Focus on actionable data."""

# Convert Anthropic-style tool defs → OpenAI function-calling format
def _to_openai_tools(tools: list[dict]) -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t["description"],
                "parameters": t["input_schema"],
            },
        }
        for t in tools
    ]


OPENAI_TOOLS = _to_openai_tools(STRUCTURER_TOOLS)


class StructurerAgent:
    """Uses Azure OpenAI GPT-4o with function calling to normalize raw Azure data."""

    def __init__(self):
        self.client = get_openai_client()
        self.deployment = settings.azure_openai_deployment

    def structure(self, raw_data: RawAzureData) -> StructuredReport:
        """Run the function-calling loop until finalize_structured_report is called."""
        console.print(
            f"[cyan]Structurer Agent:[/cyan] Starting analysis with "
            f"Azure OpenAI ({self.deployment})..."
        )

        user_message = (
            f"Please structure the following Azure cost data into a normalized report.\n\n"
            f"Subscription: {raw_data.subscription_id}\n"
            f"Collection period: {raw_data.lookback_days} days\n"
            f"Collected at: {raw_data.collected_at.isoformat()}\n"
            f"Total resources: {raw_data.resource_count}\n"
            f"Total cost entries: {len(raw_data.cost_entries)}\n"
            f"Total cost (USD): ${raw_data.total_cost_usd:,.2f}\n"
            f"Advisor recommendations available: {len(raw_data.advisor_recommendations)}\n"
            f"VM metric samples collected: {len(raw_data.vm_metrics)}\n"
            f"Potentially orphaned resources detected: {len(raw_data.orphaned_resource_ids)}\n\n"
            "Please call the available functions to build the structured report."
        )

        messages: list[dict] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]
        structured_report_data: dict | None = None

        while True:
            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=messages,
                tools=OPENAI_TOOLS,
                tool_choice="auto",
                max_tokens=8096,
                temperature=0.1,
            )

            msg = response.choices[0].message
            messages.append(msg)

            finish_reason = response.choices[0].finish_reason

            if finish_reason == "stop" or not msg.tool_calls:
                console.print("[yellow]Structurer:[/yellow] Model finished without calling finalize.")
                break

            if finish_reason in ("tool_calls", "function_call") or msg.tool_calls:
                for tool_call in msg.tool_calls or []:
                    fn_name = tool_call.function.name
                    fn_args = json.loads(tool_call.function.arguments or "{}")
                    console.print(f"  [dim]→ Function call: {fn_name}[/dim]")

                    if fn_name == "finalize_structured_report":
                        structured_report_data = fn_args.get("report", {})
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps({
                                "status": "saved",
                                "report_id": structured_report_data.get("report_id"),
                            }),
                        })
                        break
                    else:
                        result = handle_structurer_tool(fn_name, fn_args, raw_data)
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": result,
                        })

                if structured_report_data is not None:
                    break

        if structured_report_data is None:
            console.print(
                "[yellow]Warning:[/yellow] Model did not call finalize — building fallback report."
            )
            structured_report_data = {}

        return self._build_structured_report(structured_report_data, raw_data)

    def _build_structured_report(
        self, data: dict, raw_data: RawAzureData
    ) -> StructuredReport:
        """Convert the assembled report dict to a StructuredReport dataclass."""
        report_id = data.get("report_id") or str(uuid.uuid4())[:8]

        cost_by_service = [
            CostByService(
                service_name=s.get("service_name", ""),
                total_cost_usd=s.get("total_cost_usd", 0.0),
                percentage_of_total=s.get("percentage_of_total", 0.0),
                trend=s.get("trend", "stable"),
            )
            for s in data.get("cost_by_service", [])
        ]

        cost_by_rg = [
            CostByResourceGroup(
                resource_group=r.get("resource_group", ""),
                total_cost_usd=r.get("total_cost_usd", 0.0),
                top_services=r.get("top_services", []),
            )
            for r in data.get("cost_by_resource_group", [])
        ]

        underutilized = [
            UnderutilizedResource(
                resource_id=u.get("resource_id", ""),
                name=u.get("name", ""),
                resource_type=u.get("resource_type", "Microsoft.Compute/virtualMachines"),
                resource_group=u.get("resource_group", ""),
                avg_cpu_percent=u.get("avg_cpu_percent"),
                avg_memory_percent=u.get("avg_memory_percent"),
                monthly_cost_usd=u.get("monthly_cost_usd", 0.0),
                recommended_action=u.get("recommended_action", "evaluate"),
            )
            for u in data.get("underutilized_resources", [])
        ]

        orphaned = [
            OrphanedResource(
                resource_id=o.get("resource_id", ""),
                name=o.get("name", ""),
                resource_type=o.get("resource_type", ""),
                resource_group=o.get("resource_group", ""),
                estimated_monthly_cost_usd=o.get("estimated_monthly_cost_usd", 0.0),
                reason=o.get("reason", "orphaned_resource"),
            )
            for o in data.get("orphaned_resources", [])
        ]

        return StructuredReport(
            report_id=report_id,
            generated_at=datetime.now(timezone.utc),
            subscription_id=raw_data.subscription_id,
            period_days=raw_data.lookback_days,
            total_spend_usd=data.get("total_spend_usd", raw_data.total_cost_usd),
            cost_by_service=cost_by_service,
            cost_by_resource_group=cost_by_rg,
            underutilized_resources=underutilized,
            orphaned_resources=orphaned,
            advisor_cost_recommendations=data.get("advisor_cost_recommendations", []),
            vm_inventory=data.get("vm_inventory", []),
        )
