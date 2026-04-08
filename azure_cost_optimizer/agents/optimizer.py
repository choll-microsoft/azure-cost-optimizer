"""Agent 3: Generates cost optimization recommendations using Azure OpenAI GPT-4o."""

import json
import uuid
from datetime import datetime, timezone

from rich.console import Console

from ..config import settings
from ..models.raw_data import RawAzureData
from ..models.structured import OptimizationOutput, OptimizationRecommendation, StructuredReport
from ..openai_client import get_openai_client
from ..tools.optimizer_tools import OPTIMIZER_TOOLS, handle_optimizer_tool

console = Console()

SYSTEM_PROMPT = """You are a senior Azure cloud cost optimization architect. You have access \
to a structured cost and resource report for an Azure subscription. Your goal is to analyze \
the data and produce actionable, prioritized recommendations.

Focus areas (in priority order):
1. Immediate wins: orphaned/idle resources (delete = instant savings, zero risk)
2. Right-sizing: VMs running at <20% CPU for 30+ days
3. Reservations: VMs running 24/7 that are not reserved
4. Azure Savings Plans: broad compute spend commitments
5. AI model costs: GPT-4o → GPT-4o-mini for suitable workloads, over-provisioned deployments
6. Data Factory: idle integration runtimes, compute type optimization
7. Databricks: spot instances, auto-termination, job vs all-purpose clusters
8. Synapse & Fabric: dedicated SQL pool auto-pause, DWU right-sizing, SKU optimization
9. SQL migration: Azure SQL → PostgreSQL or MySQL (50-70% cheaper for eligible workloads)
10. Cost anomalies: unexplained spend spikes

Always call ALL relevant analysis tools before calling finalize_optimization_report.
Even if a service has zero cost today, call the tool — it may reveal architectural recommendations.

For each recommendation:
- Be specific (name the actual resources when possible)
- Quantify savings in USD/month
- Rate implementation effort (low/medium/high)
- Provide concrete implementation steps with Azure CLI commands where possible

Use the available functions to gather supporting data, then call \
finalize_optimization_report with your complete findings.

Priority levels:
- critical: immediate action needed, large savings or risk
- high: significant savings with low-medium effort
- medium: moderate savings or higher effort
- low: nice-to-have improvements"""


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


OPENAI_TOOLS = _to_openai_tools(OPTIMIZER_TOOLS)


class OptimizerAgent:
    """Uses Azure OpenAI GPT-4o with function calling to generate cost optimization recommendations."""

    def __init__(self):
        self.client = get_openai_client()
        self.deployment = settings.azure_openai_deployment

    def optimize(
        self,
        structured_report: StructuredReport,
        raw_data: RawAzureData,
    ) -> OptimizationOutput:
        """Run the function-calling loop to generate optimization recommendations."""
        console.print(
            f"[cyan]Optimizer Agent:[/cyan] Starting optimization with "
            f"Azure OpenAI ({self.deployment})..."
        )

        top_services = structured_report.cost_by_service[:5]
        top_services_str = "\n".join(
            f"  - {s.service_name}: ${s.total_cost_usd:,.2f} ({s.percentage_of_total:.1f}%)"
            for s in top_services
        )

        user_message = (
            f"Please analyze the following Azure cost report and generate optimization recommendations.\n\n"
            f"## Subscription Overview\n"
            f"- Subscription: {structured_report.subscription_id}\n"
            f"- Analysis period: {structured_report.period_days} days\n"
            f"- Total spend: ${structured_report.total_spend_usd:,.2f}\n"
            f"- Report generated: {structured_report.generated_at.isoformat()}\n\n"
            f"## Top 5 Services by Cost\n{top_services_str}\n\n"
            f"## Resource Summary\n"
            f"- VMs in inventory: {len(structured_report.vm_inventory)}\n"
            f"- Underutilized VMs identified: {len(structured_report.underutilized_resources)}\n"
            f"- Orphaned resources: {len(structured_report.orphaned_resources)}\n"
            f"- Azure Advisor cost recommendations: {len(structured_report.advisor_cost_recommendations)}\n\n"
            "Please call the available functions to gather detailed analysis data, then call "
            "finalize_optimization_report with your complete findings and recommendations."
        )

        messages: list[dict] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]
        optimization_data: dict | None = None

        while True:
            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=messages,
                tools=OPENAI_TOOLS,
                tool_choice="auto",
                max_tokens=8096,
                temperature=0.2,
            )

            msg = response.choices[0].message
            messages.append(msg)

            finish_reason = response.choices[0].finish_reason

            if finish_reason == "stop" or not msg.tool_calls:
                console.print("[yellow]Optimizer:[/yellow] Model finished without calling finalize.")
                break

            if finish_reason in ("tool_calls", "function_call") or msg.tool_calls:
                for tool_call in msg.tool_calls or []:
                    fn_name = tool_call.function.name
                    fn_args = json.loads(tool_call.function.arguments or "{}")
                    console.print(f"  [dim]→ Function call: {fn_name}[/dim]")

                    if fn_name == "finalize_optimization_report":
                        optimization_data = fn_args
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps({"status": "report_finalized"}),
                        })
                        break
                    else:
                        result = handle_optimizer_tool(
                            fn_name, fn_args, structured_report, raw_data
                        )
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": result,
                        })

                if optimization_data is not None:
                    break

        if optimization_data is None:
            console.print(
                "[yellow]Warning:[/yellow] Model did not call finalize — building empty output."
            )
            optimization_data = {
                "executive_summary": "Analysis incomplete.",
                "recommendations": [],
                "total_potential_savings_usd": 0.0,
            }

        output = self._build_optimization_output(optimization_data, structured_report)
        output.markdown_report = self._render_markdown_report(output, structured_report)
        return output

    def _build_optimization_output(
        self, data: dict, structured_report: StructuredReport
    ) -> OptimizationOutput:
        report_id = str(uuid.uuid4())[:8]

        recommendations = [
            OptimizationRecommendation(
                priority=r.get("priority", "medium"),
                category=r.get("category", "general"),
                title=r.get("title", ""),
                description=r.get("description", ""),
                affected_resources=r.get("affected_resources", []),
                estimated_monthly_savings_usd=float(
                    r.get("estimated_monthly_savings_usd", 0) or 0
                ),
                implementation_effort=r.get("implementation_effort", "medium"),
                steps=r.get("steps", []),
            )
            for r in data.get("recommendations", [])
        ]

        priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        recommendations.sort(
            key=lambda r: (
                priority_order.get(r.priority, 99),
                -r.estimated_monthly_savings_usd,
            )
        )

        return OptimizationOutput(
            report_id=report_id,
            generated_at=datetime.now(timezone.utc),
            executive_summary=data.get("executive_summary", ""),
            total_potential_savings_usd=float(
                data.get("total_potential_savings_usd", 0) or 0
            ),
            recommendations=recommendations,
        )

    def _render_markdown_report(
        self, output: OptimizationOutput, structured_report: StructuredReport
    ) -> str:
        lines = [
            "# Azure Cost Optimization Report",
            "",
            f"**Report ID:** `{output.report_id}`  ",
            f"**Generated:** {output.generated_at.strftime('%Y-%m-%d %H:%M UTC')}  ",
            f"**Model:** Azure OpenAI {settings.azure_openai_deployment}  ",
            f"**Subscription:** `{structured_report.subscription_id}`  ",
            f"**Analysis Period:** {structured_report.period_days} days  ",
            "",
            "---",
            "",
            "## Executive Summary",
            "",
            output.executive_summary,
            "",
            "---",
            "",
            f"## Total Potential Savings: ${output.total_potential_savings_usd:,.2f}/month",
            "",
            "---",
            "",
            "## Current Cost Breakdown",
            "",
            "### Top Services by Spend",
            "",
            "| Service | Monthly Cost | % of Total |",
            "|---------|-------------|------------|",
        ]

        for svc in structured_report.cost_by_service[:10]:
            lines.append(
                f"| {svc.service_name} | ${svc.total_cost_usd:,.2f} | {svc.percentage_of_total:.1f}% |"
            )

        lines += [
            "",
            "### Cost by Resource Group",
            "",
            "| Resource Group | Monthly Cost | Top Services |",
            "|----------------|-------------|--------------|",
        ]

        for rg in structured_report.cost_by_resource_group[:10]:
            top_svcs = ", ".join(rg.top_services[:3])
            lines.append(f"| {rg.resource_group} | ${rg.total_cost_usd:,.2f} | {top_svcs} |")

        lines += ["", "---", "", "## Priority Recommendations", ""]

        priority_icons = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}

        for i, rec in enumerate(output.recommendations, 1):
            icon = priority_icons.get(rec.priority, "⚪")
            lines += [
                f"### {i}. {icon} [{rec.priority.upper()}] {rec.title}",
                "",
                f"**Category:** {rec.category}  ",
                f"**Estimated Savings:** ${rec.estimated_monthly_savings_usd:,.2f}/month  ",
                f"**Implementation Effort:** {rec.implementation_effort.capitalize()}  ",
                "",
                rec.description,
                "",
            ]

            if rec.affected_resources:
                lines.append("**Affected Resources:**")
                for res in rec.affected_resources[:5]:
                    lines.append(f"- `{res}`")
                if len(rec.affected_resources) > 5:
                    lines.append(f"- *...and {len(rec.affected_resources) - 5} more*")
                lines.append("")

            if rec.steps:
                lines.append("**Implementation Steps:**")
                for step in rec.steps:
                    lines.append(f"1. {step}")
                lines.append("")

        if structured_report.vm_inventory:
            lines += [
                "---", "", "## VM Inventory", "",
                "| VM Name | Size | Power State | Resource Group | Monthly Cost |",
                "|---------|------|-------------|----------------|--------------|",
            ]
            for vm in sorted(
                structured_report.vm_inventory,
                key=lambda v: v.get("monthly_cost_usd", 0),
                reverse=True,
            )[:20]:
                lines.append(
                    f"| {vm.get('name','')} | {vm.get('vm_size','')} | "
                    f"{vm.get('power_state','')} | {vm.get('resource_group','')} | "
                    f"${vm.get('monthly_cost_usd',0):,.2f} |"
                )

        if structured_report.orphaned_resources:
            lines += [
                "", "---", "", "## Orphaned Resources", "",
                "| Resource | Type | Resource Group | Reason | Est. Monthly Cost |",
                "|----------|------|----------------|--------|-------------------|",
            ]
            for r in structured_report.orphaned_resources:
                lines.append(
                    f"| {r.name} | {r.resource_type} | {r.resource_group} | "
                    f"{r.reason} | ${r.estimated_monthly_cost_usd:,.2f} |"
                )

        lines += [
            "", "---", "",
            f"*Generated by Azure Cost Optimizer — powered by Azure OpenAI {settings.azure_openai_deployment}*",
            "",
        ]

        return "\n".join(lines)
