from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class CostByService:
    service_name: str
    total_cost_usd: float
    percentage_of_total: float
    trend: str = "stable"  # "increasing", "stable", "decreasing"


@dataclass
class CostByResourceGroup:
    resource_group: str
    total_cost_usd: float
    top_services: list[str] = field(default_factory=list)


@dataclass
class UnderutilizedResource:
    resource_id: str
    name: str
    resource_type: str
    resource_group: str
    avg_cpu_percent: float | None
    avg_memory_percent: float | None
    monthly_cost_usd: float
    recommended_action: str  # "resize", "deallocate", "delete"


@dataclass
class OrphanedResource:
    resource_id: str
    name: str
    resource_type: str
    resource_group: str
    estimated_monthly_cost_usd: float
    reason: str  # "unattached_disk", "unused_public_ip", "empty_nsg", etc.


@dataclass
class StructuredReport:
    report_id: str
    generated_at: datetime
    subscription_id: str
    period_days: int
    total_spend_usd: float
    cost_by_service: list[CostByService] = field(default_factory=list)
    cost_by_resource_group: list[CostByResourceGroup] = field(default_factory=list)
    underutilized_resources: list[UnderutilizedResource] = field(default_factory=list)
    orphaned_resources: list[OrphanedResource] = field(default_factory=list)
    advisor_cost_recommendations: list[dict] = field(default_factory=list)
    vm_inventory: list[dict] = field(default_factory=list)


@dataclass
class OptimizationRecommendation:
    priority: str           # "critical", "high", "medium", "low"
    category: str           # "right_sizing", "reservations", "savings_plan", "orphaned", "architectural"
    title: str
    description: str
    affected_resources: list[str] = field(default_factory=list)
    estimated_monthly_savings_usd: float = 0.0
    implementation_effort: str = "low"  # "low", "medium", "high"
    steps: list[str] = field(default_factory=list)


@dataclass
class OptimizationOutput:
    report_id: str
    generated_at: datetime
    executive_summary: str
    total_potential_savings_usd: float
    recommendations: list[OptimizationRecommendation] = field(default_factory=list)
    markdown_report: str = ""
