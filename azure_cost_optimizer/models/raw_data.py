from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class CostEntry:
    date: str
    service_name: str
    resource_group: str
    resource_id: str
    cost_usd: float
    currency: str
    usage_quantity: float
    usage_unit: str
    meter_category: str
    meter_subcategory: str
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class ResourceInfo:
    resource_id: str
    name: str
    type: str
    location: str
    resource_group: str
    tags: dict[str, str] = field(default_factory=dict)
    properties: dict[str, Any] = field(default_factory=dict)
    # VM-specific fields
    vm_size: str | None = None
    power_state: str | None = None  # "running", "deallocated", "stopped"
    os_disk_size_gb: int | None = None
    os_type: str | None = None


@dataclass
class AdvisorRecommendation:
    recommendation_id: str
    category: str           # "Cost", "Performance", "HighAvailability", "Security"
    impact: str             # "High", "Medium", "Low"
    short_description: str
    long_description: str
    resource_id: str
    potential_savings_usd: float | None
    savings_currency: str | None
    impacted_resource_type: str


@dataclass
class MetricSample:
    resource_id: str
    metric_name: str
    timestamp: datetime
    average: float | None
    maximum: float | None
    minimum: float | None
    unit: str


@dataclass
class RawAzureData:
    collected_at: datetime
    subscription_id: str
    lookback_days: int
    cost_entries: list[CostEntry] = field(default_factory=list)
    resources: list[ResourceInfo] = field(default_factory=list)
    advisor_recommendations: list[AdvisorRecommendation] = field(default_factory=list)
    vm_metrics: list[MetricSample] = field(default_factory=list)
    orphaned_resource_ids: list[str] = field(default_factory=list)
    total_cost_usd: float = 0.0
    resource_count: int = 0
