"""Pipeline orchestrator: runs all three agents end-to-end."""

import dataclasses
import json
from datetime import datetime
from pathlib import Path

from rich.console import Console

from .agents.collector import AzureCollector
from .agents.optimizer import OptimizerAgent
from .agents.structurer import StructurerAgent
from .auth import get_credential
from .config import settings
from .models.raw_data import RawAzureData
from .models.structured import OptimizationOutput, StructuredReport

console = Console()


def _to_dict(obj):
    """Recursively convert dataclasses to dicts for JSON serialization."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _to_dict(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, list):
        return [_to_dict(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _to_dict(v) for k, v in obj.items()}
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


class CostOptimizerPipeline:
    """Orchestrates the three-agent Azure cost optimization pipeline."""

    def __init__(
        self,
        subscription_id: str | None = None,
        resource_group: str | None = None,
        tenant_id: str | None = None,
    ):
        self.credential = get_credential(tenant_id=tenant_id)
        self.collector = AzureCollector(
            self.credential,
            settings,
            subscription_id=subscription_id,
            resource_group=resource_group,
        )
        self.structurer = StructurerAgent()
        self.optimizer = OptimizerAgent()
        self.output_dir = Path(settings.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "raw").mkdir(exist_ok=True)
        (self.output_dir / "reports").mkdir(exist_ok=True)
        (self.output_dir / "recommendations").mkdir(exist_ok=True)

    def run(
        self,
        raw_data: RawAzureData | None = None,
    ) -> tuple[StructuredReport, OptimizationOutput]:
        """
        Execute the full pipeline.

        Args:
            raw_data: Pre-loaded raw data (skips collection step if provided).

        Returns:
            Tuple of (StructuredReport, OptimizationOutput).
        """
        # Step 1: Collect
        if raw_data is None:
            console.rule("[bold blue]Step 1: Azure Data Collection")
            raw_data = self.collector.collect_all()
            self._save_json(raw_data, self.output_dir / "raw" / f"raw_{raw_data.collected_at.strftime('%Y%m%d_%H%M%S')}.json")
            console.print(
                f"[green]✓[/green] Collected {raw_data.resource_count} resources, "
                f"{len(raw_data.cost_entries)} cost entries, "
                f"total ${raw_data.total_cost_usd:,.2f}"
            )
        else:
            console.print("[dim]Skipping collection — using provided raw data.[/dim]")

        # Step 2: Structure
        console.rule("[bold blue]Step 2: Data Structuring (Claude)")
        structured_report = self.structurer.structure(raw_data)
        self._save_json(
            structured_report,
            self.output_dir / "reports" / f"report_{structured_report.report_id}.json",
        )
        console.print(
            f"[green]✓[/green] Structured report complete: "
            f"{len(structured_report.cost_by_service)} services, "
            f"{len(structured_report.vm_inventory)} VMs, "
            f"{len(structured_report.orphaned_resources)} orphaned resources"
        )

        # Step 3: Optimize
        console.rule("[bold blue]Step 3: Cost Optimization (Claude)")
        optimization = self.optimizer.optimize(structured_report, raw_data)
        report_path = self.output_dir / "recommendations" / f"optimization_{optimization.report_id}.json"
        md_path = self.output_dir / "recommendations" / f"optimization_{optimization.report_id}.md"
        self._save_json(optimization, report_path)
        md_path.write_text(optimization.markdown_report, encoding="utf-8")
        console.print(
            f"[green]✓[/green] Optimization complete: "
            f"{len(optimization.recommendations)} recommendations, "
            f"${optimization.total_potential_savings_usd:,.2f}/month potential savings"
        )
        console.print(f"  Markdown report: [link]{md_path}[/link]")

        return structured_report, optimization

    def _save_json(self, obj, path: Path) -> None:
        """Serialize a dataclass or dict to JSON."""
        data = _to_dict(obj) if dataclasses.is_dataclass(obj) else obj
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        console.print(f"  [dim]Saved: {path}[/dim]")

    @classmethod
    def load_raw_data(cls, path: str) -> RawAzureData:
        """Load previously-saved raw data from a JSON file."""
        import dataclasses as dc
        from datetime import datetime, timezone

        from .models.raw_data import (
            AdvisorRecommendation,
            CostEntry,
            MetricSample,
            RawAzureData,
            ResourceInfo,
        )

        data = json.loads(Path(path).read_text(encoding="utf-8"))

        return RawAzureData(
            collected_at=datetime.fromisoformat(data["collected_at"]),
            subscription_id=data["subscription_id"],
            lookback_days=data["lookback_days"],
            cost_entries=[CostEntry(**e) for e in data.get("cost_entries", [])],
            resources=[ResourceInfo(**r) for r in data.get("resources", [])],
            advisor_recommendations=[
                AdvisorRecommendation(**a) for a in data.get("advisor_recommendations", [])
            ],
            vm_metrics=[
                MetricSample(
                    **{**m, "timestamp": datetime.fromisoformat(m["timestamp"])}
                )
                for m in data.get("vm_metrics", [])
            ],
            orphaned_resource_ids=data.get("orphaned_resource_ids", []),
            total_cost_usd=data.get("total_cost_usd", 0.0),
            resource_count=data.get("resource_count", 0),
        )
