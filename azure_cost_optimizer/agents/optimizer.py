"""Agent 3: Generates cost optimization recommendations using Claude tool_use."""

import json
import uuid
from datetime import datetime, timezone

import anthropic
from rich.console import Console

from ..config import settings
from ..models.raw_data import RawAzureData
from ..models.structured import OptimizationOutput, OptimizationRecommendation, StructuredReport
from ..tools.optimizer_tools import OPTIMIZER_TOOLS, handle_optimizer_tool

console = Console()

SYSTEM_PROMPT = """You are a senior Azure cloud cost optimization architect. You have access \
to a structured cost and resource report for an Azure subscription. Your goal is to analyze \
the data and produce actionable, prioritized recommendations.

Focus areas (in priority order):
1. Immediate wins: orphaned/idle resources (delete = instant savings, zero risk)
2. Right-sizing: VMs running at <20% CPU for 30+ days
3. Reservations: VMs running 24/7 that aren't reserved
4. Azure Savings Plans: broad compute spend commitments
5. Cost anomalies: unexplained spend spikes
6. Performance tracking: identify over-provisioned and at-capacity resources

For each recommendation:
- Be specific (name the actual resources when possible)
- Quantify savings in USD/month
- Rate implementation effort (low/medium/high)
- Provide concrete implementation steps with Azure CLI commands where possible

Use the available tools to gather supporting data, analyze it, then call \
finalize_optimization_report with your complete findings.

Priority levels:
- critical: immediate action needed, large savings or risk
- high: significant savings with low-medium effort
- medium: moderate savings or higher effort
- low: nice-to-have improvements"""


class OptimizerAgent:
    """Uses Claude with tool_use to generate cost optimization recommendations."""

    def __init__(self):
        self.client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        self.model = settings.claude_model

    def optimize(
        self,
        structured_report: StructuredReport,
        raw_data: RawAzureData,
    ) -> OptimizationOutput:
        """Run the Claude tool_use loop to generate optimization recommendations."""
        console.print("[cyan]Optimizer Agent:[/cyan] Starting optimization analysis with Claude...")

        # Give Claude a rich context snapshot to work from
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
            "Please call the available tools to gather detailed analysis data, then call "
            "finalize_optimization_report with your complete findings and recommendations."
        )

        messages: list[dict] = [{"role": "user", "content": user_message}]
        optimization_data: dict | None = None

        while True:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=8096,
                system=SYSTEM_PROMPT,
                tools=OPTIMIZER_TOOLS,
                messages=messages,
            )

            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                console.print("[yellow]Optimizer:[/yellow] Claude finished without calling finalize.")
                break

            if response.stop_reason == "tool_use":
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        console.print(f"  [dim]→ Tool call: {block.name}[/dim]")
                        if block.name == "finalize_optimization_report":
                            optimization_data = block.input
                            tool_results.append(
                                {
                                    "type": "tool_result",
                                    "tool_use_id": block.id,
                                    "content": json.dumps({"status": "report_finalized"}),
                                }
                            )
                        else:
                            result = handle_optimizer_tool(
                                block.name, block.input, structured_report, raw_data
                            )
                            tool_results.append(
                                {
                                    "type": "tool_result",
                                    "tool_use_id": block.id,
                                    "content": result,
                                }
                            )

                messages.append({"role": "user", "content": tool_results})

                if optimization_data is not None:
                    break

        if optimization_data is None:
            console.print("[yellow]Warning:[/yellow] Claude did not call finalize — building empty output.")
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
        """Convert Claude's finalize call data to OptimizationOutput."""
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

        # Sort by priority order then savings
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
            total_potential_savings_usd=float(data.get("total_potential_savings_usd", 0) or 0),
            recommendations=recommendations,
        )

    def _render_markdown_report(
        self, output: OptimizationOutput, structured_report: StructuredReport
    ) -> str:
        """Render a full Markdown optimization report."""
        lines = [
            "# Azure Cost Optimization Report",
            "",
            f"**Report ID:** `{output.report_id}`  ",
            f"**Generated:** {output.generated_at.strftime('%Y-%m-%d %H:%M UTC')}  ",
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
            effort_map = {"low": "Low", "medium": "Medium", "high": "High"}
            lines += [
                f"### {i}. {icon} [{rec.priority.upper()}] {rec.title}",
                "",
                f"**Category:** {rec.category}  ",
                f"**Estimated Savings:** ${rec.estimated_monthly_savings_usd:,.2f}/month  ",
                f"**Implementation Effort:** {effort_map.get(rec.implementation_effort, rec.implementation_effort)}  ",
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

        # VM inventory table
        if structured_report.vm_inventory:
            lines += [
                "---",
                "",
                "## VM Inventory",
                "",
                "| VM Name | Size | Power State | Resource Group | Monthly Cost |",
                "|---------|------|-------------|----------------|--------------|",
            ]
            for vm in sorted(
                structured_report.vm_inventory,
                key=lambda v: v.get("monthly_cost_usd", 0),
                reverse=True,
            )[:20]:
                lines.append(
                    f"| {vm.get('name', '')} | {vm.get('vm_size', '')} | "
                    f"{vm.get('power_state', '')} | {vm.get('resource_group', '')} | "
                    f"${vm.get('monthly_cost_usd', 0):,.2f} |"
                )

        # Orphaned resources
        if structured_report.orphaned_resources:
            lines += [
                "",
                "---",
                "",
                "## Orphaned Resources",
                "",
                "| Resource | Type | Resource Group | Reason | Est. Monthly Cost |",
                "|----------|------|----------------|--------|-------------------|",
            ]
            for r in structured_report.orphaned_resources:
                lines.append(
                    f"| {r.name} | {r.resource_type} | {r.resource_group} | "
                    f"{r.reason} | ${r.estimated_monthly_cost_usd:,.2f} |"
                )

        lines += [
            "",
            "---",
            "",
            "*Generated by Azure Cost Optimizer — powered by Claude*",
            "",
        ]

        return "\n".join(lines)
