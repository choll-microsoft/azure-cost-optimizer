"""Agent 1: Azure data collector using Azure SDKs (no Claude involvement)."""

import time
from datetime import datetime, timedelta, timezone

from azure.identity import ClientSecretCredential
from azure.mgmt.advisor import AdvisorManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.costmanagement.models import (
    ExportType,
    FunctionType,
    GranularityType,
    QueryAggregation,
    QueryDataset,
    QueryDefinition,
    QueryGrouping,
    QueryTimePeriod,
    TimeframeType,
)
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.monitor.query import MetricsQueryClient
from rich.console import Console

from ..config import Settings
from ..models.raw_data import (
    AdvisorRecommendation,
    CostEntry,
    MetricSample,
    RawAzureData,
    ResourceInfo,
)

console = Console()

# Metrics to collect per VM
VM_METRICS = [
    "Percentage CPU",
    "Available Memory Bytes",
    "Network In Total",
    "Network Out Total",
    "Disk Read Bytes",
    "Disk Write Bytes",
]

# Max VMs to collect metrics for (avoid rate limits on large subscriptions)
MAX_VMS_FOR_METRICS = 50
VM_METRIC_BATCH_SIZE = 10


class AzureCollector:
    """Collects cost, resource, advisor, and metric data from Azure APIs."""

    def __init__(self, credential: ClientSecretCredential, settings: Settings):
        self.credential = credential
        self.subscription_id = settings.azure_subscription_id
        self.lookback_days = settings.lookback_days
        self._scope = f"/subscriptions/{self.subscription_id}"

        self.cost_client = CostManagementClient(credential)
        self.resource_client = ResourceManagementClient(credential, self.subscription_id)
        self.compute_client = ComputeManagementClient(credential, self.subscription_id)
        self.network_client = NetworkManagementClient(credential, self.subscription_id)
        self.advisor_client = AdvisorManagementClient(credential, self.subscription_id)
        self.metrics_client = MetricsQueryClient(credential)

    def collect_all(self) -> RawAzureData:
        """Run all collectors and assemble RawAzureData."""
        now = datetime.now(timezone.utc)
        console.print("[cyan]Step 1/4:[/cyan] Collecting cost data...")
        cost_entries = self._get_cost_data()

        console.print("[cyan]Step 2/4:[/cyan] Collecting resource inventory...")
        resources = self._get_all_resources()

        console.print("[cyan]Step 3/4:[/cyan] Collecting Azure Advisor recommendations...")
        advisor_recs = self._get_advisor_recommendations()

        console.print("[cyan]Step 4/4:[/cyan] Collecting VM performance metrics...")
        vm_resources = [r for r in resources if r.type == "Microsoft.Compute/virtualMachines"]
        vm_metrics = self._get_vm_metrics([r.resource_id for r in vm_resources[:MAX_VMS_FOR_METRICS]])

        orphaned_ids = self._detect_orphaned_resource_ids(resources)

        total_cost = sum(e.cost_usd for e in cost_entries)

        return RawAzureData(
            collected_at=now,
            subscription_id=self.subscription_id,
            lookback_days=self.lookback_days,
            cost_entries=cost_entries,
            resources=resources,
            advisor_recommendations=advisor_recs,
            vm_metrics=vm_metrics,
            orphaned_resource_ids=orphaned_ids,
            total_cost_usd=total_cost,
            resource_count=len(resources),
        )

    # -------------------------------------------------------------------------
    # Cost Management
    # -------------------------------------------------------------------------

    def _get_cost_data(self) -> list[CostEntry]:
        """Query daily costs grouped by service/resource group/resource."""
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=self.lookback_days)

        query = QueryDefinition(
            type=ExportType.ACTUAL_COST,
            timeframe=TimeframeType.CUSTOM,
            time_period=QueryTimePeriod(
                from_property=start_date,
                to=end_date,
            ),
            dataset=QueryDataset(
                granularity=GranularityType.DAILY,
                aggregation={
                    "totalCost": QueryAggregation(name="Cost", function=FunctionType.SUM),
                    "totalUsage": QueryAggregation(name="UsageQuantity", function=FunctionType.SUM),
                },
                grouping=[
                    QueryGrouping(type="Dimension", name="ServiceName"),
                    QueryGrouping(type="Dimension", name="ResourceGroupName"),
                    QueryGrouping(type="Dimension", name="ResourceId"),
                    QueryGrouping(type="Dimension", name="MeterCategory"),
                    QueryGrouping(type="Dimension", name="MeterSubcategory"),
                ],
            ),
        )

        entries: list[CostEntry] = []
        try:
            result = self.cost_client.query.usage(scope=self._scope, parameters=query)
            columns = [col.name for col in result.columns]

            def _col(row: list, name: str, default=None):
                try:
                    return row[columns.index(name)]
                except (ValueError, IndexError):
                    return default

            for row in result.rows:
                entries.append(
                    CostEntry(
                        date=str(_col(row, "UsageDate", "")),
                        service_name=str(_col(row, "ServiceName", "Unknown")),
                        resource_group=str(_col(row, "ResourceGroupName", "")),
                        resource_id=str(_col(row, "ResourceId", "")),
                        cost_usd=float(_col(row, "Cost", 0) or 0),
                        currency="USD",
                        usage_quantity=float(_col(row, "UsageQuantity", 0) or 0),
                        usage_unit="",
                        meter_category=str(_col(row, "MeterCategory", "")),
                        meter_subcategory=str(_col(row, "MeterSubcategory", "")),
                    )
                )
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Could not retrieve cost data: {e}")

        console.print(f"  Collected {len(entries)} cost entries")
        return entries

    # -------------------------------------------------------------------------
    # Resource Inventory
    # -------------------------------------------------------------------------

    def _get_all_resources(self) -> list[ResourceInfo]:
        """List all resources in the subscription and enrich VMs."""
        resources: list[ResourceInfo] = []

        try:
            for item in self.resource_client.resources.list():
                rg = ""
                if item.id:
                    parts = item.id.split("/")
                    try:
                        rg_idx = [p.lower() for p in parts].index("resourcegroups")
                        rg = parts[rg_idx + 1]
                    except (ValueError, IndexError):
                        pass

                resources.append(
                    ResourceInfo(
                        resource_id=item.id or "",
                        name=item.name or "",
                        type=item.type or "",
                        location=item.location or "",
                        resource_group=rg,
                        tags=dict(item.tags or {}),
                    )
                )
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Could not list resources: {e}")

        resources = self._enrich_vm_info(resources)
        console.print(f"  Collected {len(resources)} resources")
        return resources

    def _enrich_vm_info(self, resources: list[ResourceInfo]) -> list[ResourceInfo]:
        """Fetch VM size, power state, and disk info for all VM resources."""
        vm_resources = [r for r in resources if r.type == "Microsoft.Compute/virtualMachines"]
        vm_map = {r.resource_id: r for r in vm_resources}

        for resource in vm_resources:
            try:
                vm = self.compute_client.virtual_machines.get(
                    resource_group_name=resource.resource_group,
                    vm_name=resource.name,
                    expand="instanceView",
                )
                resource.vm_size = vm.hardware_profile.vm_size if vm.hardware_profile else None
                resource.os_type = (
                    vm.storage_profile.os_disk.os_type
                    if vm.storage_profile and vm.storage_profile.os_disk
                    else None
                )
                resource.os_disk_size_gb = (
                    vm.storage_profile.os_disk.disk_size_gb
                    if vm.storage_profile and vm.storage_profile.os_disk
                    else None
                )

                # Extract power state from instance view
                if vm.instance_view and vm.instance_view.statuses:
                    for status in vm.instance_view.statuses:
                        if status.code and status.code.startswith("PowerState/"):
                            resource.power_state = status.code.split("/")[1].lower()
                            break
            except Exception as e:
                console.print(f"[yellow]Warning:[/yellow] Could not enrich VM {resource.name}: {e}")

        _ = vm_map  # suppress unused warning
        return resources

    # -------------------------------------------------------------------------
    # Azure Advisor
    # -------------------------------------------------------------------------

    def _get_advisor_recommendations(self) -> list[AdvisorRecommendation]:
        """Fetch all Advisor recommendations for the subscription."""
        recommendations: list[AdvisorRecommendation] = []

        try:
            for rec in self.advisor_client.recommendations.list():
                savings_usd: float | None = None
                savings_currency: str | None = None

                if rec.extended_properties:
                    savings_raw = rec.extended_properties.get(
                        "savingsAmount", rec.extended_properties.get("annualSavingsAmount")
                    )
                    if savings_raw is not None:
                        try:
                            savings_usd = float(savings_raw)
                        except (TypeError, ValueError):
                            pass
                    savings_currency = rec.extended_properties.get("savingsCurrency", "USD")

                resource_id = ""
                if rec.resource_metadata and rec.resource_metadata.resource_id:
                    resource_id = rec.resource_metadata.resource_id

                recommendations.append(
                    AdvisorRecommendation(
                        recommendation_id=rec.name or "",
                        category=str(rec.category or ""),
                        impact=str(rec.impact or ""),
                        short_description=str(
                            rec.short_description.solution if rec.short_description else ""
                        ),
                        long_description=str(
                            rec.short_description.problem if rec.short_description else ""
                        ),
                        resource_id=resource_id,
                        potential_savings_usd=savings_usd,
                        savings_currency=savings_currency,
                        impacted_resource_type=str(rec.impacted_field or ""),
                    )
                )
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Could not retrieve Advisor recommendations: {e}")

        console.print(f"  Collected {len(recommendations)} Advisor recommendations")
        return recommendations

    # -------------------------------------------------------------------------
    # VM Metrics
    # -------------------------------------------------------------------------

    def _get_vm_metrics(self, vm_resource_ids: list[str]) -> list[MetricSample]:
        """Collect CPU, memory, network, and disk metrics for VMs."""
        samples: list[MetricSample] = []
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=self.lookback_days)
        timespan = (start_time, end_time)

        batches = [
            vm_resource_ids[i : i + VM_METRIC_BATCH_SIZE]
            for i in range(0, len(vm_resource_ids), VM_METRIC_BATCH_SIZE)
        ]

        for batch_idx, batch in enumerate(batches):
            for vm_id in batch:
                try:
                    result = self.metrics_client.query_resource(
                        resource_uri=vm_id,
                        metric_names=VM_METRICS,
                        timespan=timespan,
                        granularity=timedelta(hours=1),
                    )
                    for metric in result.metrics:
                        for ts in metric.timeseries:
                            for dp in ts.data:
                                if dp.timestamp:
                                    samples.append(
                                        MetricSample(
                                            resource_id=vm_id,
                                            metric_name=metric.name,
                                            timestamp=dp.timestamp,
                                            average=dp.average,
                                            maximum=dp.maximum,
                                            minimum=dp.minimum,
                                            unit=metric.unit or "",
                                        )
                                    )
                except Exception as e:
                    console.print(
                        f"[yellow]Warning:[/yellow] Could not get metrics for {vm_id}: {e}"
                    )

            if batch_idx < len(batches) - 1:
                time.sleep(1)  # avoid rate limits between batches

        console.print(f"  Collected {len(samples)} metric samples from {len(vm_resource_ids)} VMs")
        return samples

    # -------------------------------------------------------------------------
    # Orphan Detection
    # -------------------------------------------------------------------------

    def _detect_orphaned_resource_ids(self, resources: list[ResourceInfo]) -> list[str]:
        """Detect likely-orphaned resources via SDK calls."""
        orphaned: list[str] = []

        # Unattached managed disks
        try:
            for disk in self.compute_client.disks.list():
                if disk.managed_by is None and disk.id:
                    orphaned.append(disk.id)
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Could not check disks: {e}")

        # Unused public IP addresses
        try:
            for ip in self.network_client.public_ip_addresses.list_all():
                if ip.ip_configuration is None and ip.id:
                    orphaned.append(ip.id)
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Could not check public IPs: {e}")

        # Empty NSGs (not associated with any subnet or NIC)
        try:
            for nsg in self.network_client.network_security_groups.list_all():
                subnets = nsg.subnets or []
                interfaces = nsg.network_interfaces or []
                if not subnets and not interfaces and nsg.id:
                    orphaned.append(nsg.id)
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Could not check NSGs: {e}")

        console.print(f"  Detected {len(orphaned)} potentially orphaned resources")
        return orphaned
