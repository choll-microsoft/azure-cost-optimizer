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
    AiServiceDetail,
    AnalyticsServiceDetail,
    CostEntry,
    DataFactoryDetail,
    MetricSample,
    RawAzureData,
    ResourceInfo,
    SqlResourceDetail,
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

    def __init__(
        self,
        credential: ClientSecretCredential,
        settings: Settings,
        subscription_id: str | None = None,
        resource_group: str | None = None,
    ):
        self.credential = credential
        self.subscription_id = subscription_id or settings.azure_subscription_id
        self.resource_group = resource_group  # None = subscription-wide
        self.lookback_days = settings.lookback_days

        # Cost Management scope: subscription or resource group level
        if resource_group:
            self._scope = (
                f"/subscriptions/{self.subscription_id}"
                f"/resourceGroups/{resource_group}"
            )
        else:
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
        scope_label = (
            f"resource group '{self.resource_group}'" if self.resource_group
            else f"subscription '{self.subscription_id}'"
        )
        console.print(f"[dim]Scope: {scope_label}[/dim]")

        console.print("[cyan]Step 1/5:[/cyan] Collecting cost data...")
        cost_entries = self._get_cost_data()

        console.print("[cyan]Step 2/5:[/cyan] Collecting resource inventory...")
        resources = self._get_all_resources()

        console.print("[cyan]Step 3/5:[/cyan] Collecting Azure Advisor recommendations...")
        advisor_recs = self._get_advisor_recommendations()

        console.print("[cyan]Step 4/5:[/cyan] Collecting VM performance metrics...")
        vm_resources = [r for r in resources if r.type == "Microsoft.Compute/virtualMachines"]
        vm_metrics = self._get_vm_metrics([r.resource_id for r in vm_resources[:MAX_VMS_FOR_METRICS]])

        console.print("[cyan]Step 5/5:[/cyan] Collecting AI, SQL, ADF, and analytics service details...")
        cost_by_resource = self._build_cost_by_resource(cost_entries)
        ai_services = self._collect_ai_services(resources, cost_by_resource)
        sql_resources = self._collect_sql_resources(resources, cost_by_resource)
        data_factories = self._collect_data_factories(resources, cost_by_resource)
        analytics_services = self._collect_analytics_services(resources, cost_by_resource)

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
            ai_services=ai_services,
            sql_resources=sql_resources,
            data_factories=data_factories,
            analytics_services=analytics_services,
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
            # Scope to a specific resource group when selected
            if self.resource_group:
                iterator = self.resource_client.resources.list_by_resource_group(
                    self.resource_group
                )
            else:
                iterator = self.resource_client.resources.list()

            for item in iterator:
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
        """Fetch Advisor recommendations, filtered to resource group when scoped."""
        recommendations: list[AdvisorRecommendation] = []

        try:
            all_recs = self.advisor_client.recommendations.list()
            # Filter to resource group when scoped
            if self.resource_group:
                rg_lower = self.resource_group.lower()
                all_recs = (
                    r for r in all_recs
                    if r.resource_metadata
                    and r.resource_metadata.resource_id
                    and f"/resourcegroups/{rg_lower}/" in r.resource_metadata.resource_id.lower()
                )
            for rec in all_recs:
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

    # -------------------------------------------------------------------------
    # Helper: cost lookup by resource ID
    # -------------------------------------------------------------------------

    def _build_cost_by_resource(self, cost_entries: list[CostEntry]) -> dict[str, float]:
        totals: dict[str, float] = {}
        for e in cost_entries:
            if e.resource_id:
                key = e.resource_id.lower()
                totals[key] = totals.get(key, 0.0) + e.cost_usd
        return totals

    # -------------------------------------------------------------------------
    # AI / Cognitive Services
    # -------------------------------------------------------------------------

    def _collect_ai_services(
        self,
        resources: list[ResourceInfo],
        cost_by_resource: dict[str, float],
    ) -> list[AiServiceDetail]:
        """Collect details for all AI/Cognitive Services resources."""
        ai_types = {
            "microsoft.cognitiveservices/accounts",
            "microsoft.machinelearningservices/workspaces",
            "microsoft.machinelearningservices/workspaces/onlineendpoints",
        }
        ai_resources = [r for r in resources if r.type.lower() in ai_types]
        details: list[AiServiceDetail] = []

        for r in ai_resources:
            deployments: list[dict] = []

            # Try to get OpenAI deployments
            if r.type.lower() == "microsoft.cognitiveservices/accounts":
                try:
                    from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
                    cs_client = CognitiveServicesManagementClient(
                        self.credential, self.subscription_id
                    )
                    account = cs_client.accounts.get(r.resource_group, r.name)
                    sku_name = account.sku.name if account.sku else "Unknown"
                    kind = account.kind or "CognitiveServices"

                    # Get deployments for OpenAI accounts
                    if kind in ("OpenAI", "AIServices"):
                        try:
                            for dep in cs_client.deployments.list(r.resource_group, r.name):
                                deployments.append({
                                    "name": dep.name,
                                    "model": dep.properties.model.name if dep.properties and dep.properties.model else "unknown",
                                    "version": dep.properties.model.version if dep.properties and dep.properties.model else "",
                                    "capacity_k_tpm": dep.sku.capacity if dep.sku else None,
                                    "sku": dep.sku.name if dep.sku else None,
                                })
                        except Exception:
                            pass
                except Exception:
                    sku_name = "Unknown"
                    kind = r.type.split("/")[-1]
            else:
                sku_name = "Unknown"
                kind = r.type.split("/")[-1]

            details.append(AiServiceDetail(
                resource_id=r.resource_id,
                name=r.name,
                kind=kind,
                sku_name=sku_name,
                resource_group=r.resource_group,
                location=r.location,
                monthly_cost_usd=cost_by_resource.get(r.resource_id.lower(), 0.0),
                deployments=deployments,
                tags=r.tags,
            ))

        console.print(f"  Collected {len(details)} AI/Cognitive Services resources")
        return details

    # -------------------------------------------------------------------------
    # SQL Resources (Azure SQL, MySQL, PostgreSQL)
    # -------------------------------------------------------------------------

    def _collect_sql_resources(
        self,
        resources: list[ResourceInfo],
        cost_by_resource: dict[str, float],
    ) -> list[SqlResourceDetail]:
        """Collect SQL database details — Azure SQL, MySQL, and PostgreSQL."""
        sql_type_map = {
            "microsoft.sql/servers/databases": "AzureSQL",
            "microsoft.sql/servers/elasticpools": "ElasticPool",
            "microsoft.sql/managedinstances": "ManagedInstance",
            "microsoft.dbformysql/servers": "MySQL",
            "microsoft.dbformysql/flexibleservers": "MySQLFlexible",
            "microsoft.dbforpostgresql/servers": "PostgreSQL",
            "microsoft.dbforpostgresql/flexibleservers": "PostgreSQLFlexible",
        }
        sql_resources = [
            r for r in resources if r.type.lower() in sql_type_map
        ]
        details: list[SqlResourceDetail] = []

        for r in sql_resources:
            resource_type = sql_type_map[r.type.lower()]
            sku_name = None
            tier = None
            dtu_or_vcores = None
            storage_gb = None

            try:
                from azure.mgmt.sql import SqlManagementClient
                sql_client = SqlManagementClient(self.credential, self.subscription_id)

                if resource_type == "AzureSQL":
                    parts = r.name.split("/")
                    if len(parts) == 2:
                        db = sql_client.databases.get(r.resource_group, parts[0], parts[1])
                        if db.sku:
                            sku_name = db.sku.name
                            tier = db.sku.tier
                            dtu_or_vcores = db.sku.capacity
                        storage_gb = int((db.max_size_bytes or 0) / (1024 ** 3))

                elif resource_type == "ElasticPool":
                    parts = r.name.split("/")
                    if len(parts) == 2:
                        pool = sql_client.elastic_pools.get(r.resource_group, parts[0], parts[1])
                        if pool.sku:
                            sku_name = pool.sku.name
                            tier = pool.sku.tier
                            dtu_or_vcores = pool.sku.capacity

                elif resource_type == "ManagedInstance":
                    mi = sql_client.managed_instances.get(r.resource_group, r.name)
                    sku_name = mi.sku.name if mi.sku else None
                    tier = mi.sku.tier if mi.sku else None
                    dtu_or_vcores = mi.v_cores
                    storage_gb = mi.storage_size_in_gb

            except Exception:
                pass  # SDK not installed or access denied — use resource info only

            details.append(SqlResourceDetail(
                resource_id=r.resource_id,
                name=r.name,
                resource_type=resource_type,
                resource_group=r.resource_group,
                location=r.location,
                sku_name=sku_name,
                tier=tier,
                dtu_or_vcores=dtu_or_vcores,
                storage_gb=storage_gb,
                monthly_cost_usd=cost_by_resource.get(r.resource_id.lower(), 0.0),
                tags=r.tags,
            ))

        console.print(f"  Collected {len(details)} SQL/database resources")
        return details

    # -------------------------------------------------------------------------
    # Azure Data Factory
    # -------------------------------------------------------------------------

    def _collect_data_factories(
        self,
        resources: list[ResourceInfo],
        cost_by_resource: dict[str, float],
    ) -> list[DataFactoryDetail]:
        """Collect ADF factory details including integration runtimes."""
        adf_resources = [
            r for r in resources
            if r.type.lower() == "microsoft.datafactory/factories"
        ]
        details: list[DataFactoryDetail] = []

        for r in adf_resources:
            integration_runtimes: list[dict] = []
            try:
                from azure.mgmt.datafactory import DataFactoryManagementClient
                adf_client = DataFactoryManagementClient(self.credential, self.subscription_id)
                for ir in adf_client.integration_runtimes.list_by_factory(
                    r.resource_group, r.name
                ):
                    ir_props = ir.properties
                    integration_runtimes.append({
                        "name": ir.name,
                        "type": ir_props.type if ir_props else "Unknown",
                        "state": ir_props.state if hasattr(ir_props, "state") else None,
                        "compute_type": (
                            ir_props.compute_properties.data_flow_properties.compute_type
                            if hasattr(ir_props, "compute_properties")
                            and ir_props.compute_properties
                            and hasattr(ir_props.compute_properties, "data_flow_properties")
                            and ir_props.compute_properties.data_flow_properties
                            else None
                        ),
                        "core_count": (
                            ir_props.compute_properties.data_flow_properties.core_count
                            if hasattr(ir_props, "compute_properties")
                            and ir_props.compute_properties
                            and hasattr(ir_props.compute_properties, "data_flow_properties")
                            and ir_props.compute_properties.data_flow_properties
                            else None
                        ),
                    })
            except Exception:
                pass  # SDK not installed or no access

            details.append(DataFactoryDetail(
                resource_id=r.resource_id,
                name=r.name,
                resource_group=r.resource_group,
                location=r.location,
                monthly_cost_usd=cost_by_resource.get(r.resource_id.lower(), 0.0),
                integration_runtimes=integration_runtimes,
                tags=r.tags,
            ))

        console.print(f"  Collected {len(details)} Data Factory resources")
        return details

    # -------------------------------------------------------------------------
    # Analytics Services (Databricks, Synapse, Fabric, HDInsight)
    # -------------------------------------------------------------------------

    def _collect_analytics_services(
        self,
        resources: list[ResourceInfo],
        cost_by_resource: dict[str, float],
    ) -> list[AnalyticsServiceDetail]:
        """Collect Databricks, Synapse, Fabric, and HDInsight details."""
        analytics_type_map = {
            "microsoft.databricks/workspaces": "Databricks",
            "microsoft.synapse/workspaces": "Synapse",
            "microsoft.synapse/workspaces/sqlpools": "SynapseSQLPool",
            "microsoft.synapse/workspaces/bigdatapools": "SynapseSparkPool",
            "microsoft.fabric/capacities": "Fabric",
            "microsoft.hdinsight/clusters": "HDInsight",
        }
        analytics_resources = [
            r for r in resources if r.type.lower() in analytics_type_map
        ]
        details: list[AnalyticsServiceDetail] = []

        for r in analytics_resources:
            resource_type = analytics_type_map[r.type.lower()]
            sku_name = None
            props: dict = {}

            # Try Synapse SQL pool details
            if resource_type == "SynapseSQLPool":
                try:
                    from azure.mgmt.synapse import SynapseManagementClient
                    syn_client = SynapseManagementClient(self.credential, self.subscription_id)
                    parts = r.name.split("/")
                    if len(parts) == 2:
                        pool = syn_client.sql_pools.get(r.resource_group, parts[0], parts[1])
                        sku_name = pool.sku.name if pool.sku else None
                        props = {
                            "status": pool.status,
                            "dw_units": pool.sku.capacity if pool.sku else None,
                        }
                except Exception:
                    pass

            # Try Fabric capacity SKU
            elif resource_type == "Fabric":
                try:
                    # Fabric uses the resource properties from resource manager
                    props = {"sku": r.properties.get("sku", {}).get("name")}
                    sku_name = props.get("sku")
                except Exception:
                    pass

            details.append(AnalyticsServiceDetail(
                resource_id=r.resource_id,
                name=r.name,
                resource_type=resource_type,
                resource_group=r.resource_group,
                location=r.location,
                sku_name=sku_name,
                monthly_cost_usd=cost_by_resource.get(r.resource_id.lower(), 0.0),
                properties=props,
                tags=r.tags,
            ))

        console.print(f"  Collected {len(details)} analytics service resources")
        return details
