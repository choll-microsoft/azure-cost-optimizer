#!/usr/bin/env python3
"""Azure Cost Optimizer — CLI entry point."""

import argparse
import sys

from dotenv import load_dotenv

load_dotenv()

from rich.console import Console
from rich.panel import Panel

console = Console()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Azure Cost Optimizer: collect, structure, and optimize Azure costs using GPT-4o.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline — subscription scope (default from .env)
  python main.py

  # Target a specific subscription
  python main.py --subscription-id <sub-id>

  # Scope to a single resource group
  python main.py --subscription-id <sub-id> --resource-group my-rg

  # Re-run agents on cached data (skip collection)
  python main.py --raw-file outputs/raw/raw_20240101_120000.json

  # Analyze last 14 days
  python main.py --days 14
        """,
    )
    parser.add_argument("--days", type=int, default=None,
                        help="Lookback period in days (default: 30)")
    parser.add_argument("--tenant-id", type=str, default=None,
                        help="Azure tenant ID (uses registered credentials)")
    parser.add_argument("--subscription-id", type=str, default=None,
                        help="Azure subscription ID to analyze")
    parser.add_argument("--resource-group", type=str, default=None,
                        help="Scope analysis to a single resource group")
    parser.add_argument("--raw-file", type=str, default=None,
                        help="Path to existing raw data JSON (skips collection)")
    args = parser.parse_args()

    console.print(
        Panel.fit(
            "[bold blue]Azure Cost Optimizer[/bold blue]\n"
            "Multi-agent cost analysis powered by Azure OpenAI GPT-4o",
            border_style="blue",
        )
    )

    try:
        from azure_cost_optimizer.pipeline import CostOptimizerPipeline

        if args.days is not None:
            import os
            os.environ["LOOKBACK_DAYS"] = str(args.days)

        pipeline = CostOptimizerPipeline(
            tenant_id=args.tenant_id,
            subscription_id=args.subscription_id,
            resource_group=args.resource_group,
        )

        raw_data = None
        if args.raw_file:
            console.print(f"[dim]Loading raw data from: {args.raw_file}[/dim]")
            raw_data = CostOptimizerPipeline.load_raw_data(args.raw_file)

        structured_report, optimization = pipeline.run(raw_data=raw_data)

        console.print()
        console.print(
            Panel(
                f"[bold green]Pipeline Complete![/bold green]\n\n"
                f"Total Azure spend: [bold]${structured_report.total_spend_usd:,.2f}[/bold]/month\n"
                f"Potential savings: [bold green]${optimization.total_potential_savings_usd:,.2f}[/bold green]/month\n"
                f"Recommendations: [bold]{len(optimization.recommendations)}[/bold]\n\n"
                f"[dim]Reports saved to: {pipeline.output_dir}[/dim]",
                title="Results",
                border_style="green",
            )
        )

        if optimization.recommendations:
            console.print("\n[bold]Top 3 Recommendations:[/bold]")
            for i, rec in enumerate(optimization.recommendations[:3], 1):
                priority_colors = {
                    "critical": "red", "high": "orange3",
                    "medium": "yellow", "low": "green",
                }
                color = priority_colors.get(rec.priority, "white")
                console.print(
                    f"  {i}. [{color}][{rec.priority.upper()}][/{color}] {rec.title} "
                    f"— [green]${rec.estimated_monthly_savings_usd:,.2f}/mo[/green]"
                )

        return 0

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user.[/yellow]")
        return 1
    except Exception as e:
        console.print(f"\n[red]Error:[/red] {e}")
        console.print_exception()
        return 1


if __name__ == "__main__":
    sys.exit(main())
