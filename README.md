# Azure Cost Optimizer

A multi-agent system that collects Azure cost and resource data, structures it using Claude, and generates prioritized cost optimization recommendations.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Pipeline Orchestrator                    │
└──────────────────────────┬──────────────────────────────────┘
                           │
          ┌────────────────▼────────────────┐
          │       Agent 1: Collector        │
          │  (Azure SDKs — no Claude)        │
          │                                 │
          │  • Cost Management API          │
          │  • Resource Manager (inventory) │
          │  • Compute (VM details)         │
          │  • Azure Advisor                │
          │  • Azure Monitor (VM metrics)   │
          │  • Network (orphan detection)   │
          └────────────────┬────────────────┘
                           │ RawAzureData
          ┌────────────────▼────────────────┐
          │      Agent 2: Structurer        │
          │   (Claude + tool_use)           │
          │                                 │
          │  Tools:                         │
          │  • aggregate_costs_by_service   │
          │  • aggregate_costs_by_rg        │
          │  • identify_underutilized_vms   │
          │  • identify_orphaned_resources  │
          │  • get_advisor_recommendations  │
          │  • get_vm_inventory_summary     │
          └────────────────┬────────────────┘
                           │ StructuredReport
          ┌────────────────▼────────────────┐
          │      Agent 3: Optimizer         │
          │   (Claude + tool_use)           │
          │                                 │
          │  Tools:                         │
          │  • get_right_sizing_candidates  │
          │  • calculate_reservation_savings│
          │  • calculate_savings_plan       │
          │  • get_orphaned_cleanup_plan    │
          │  • analyze_cost_anomalies       │
          │  • get_performance_tracking     │
          └────────────────┬────────────────┘
                           │ OptimizationOutput
                    ┌──────▼──────┐
                    │   Outputs   │
                    │  JSON + MD  │
                    └─────────────┘
```

## Optimization Rules Applied

| Category | What It Checks |
|----------|---------------|
| **Right-sizing** | VMs with avg CPU < 20% over 30 days → recommends smaller VM size within same family |
| **Reservations** | Consistently-running VMs not covered by RIs → estimates 30-45% savings by family |
| **Azure Savings Plans** | Total compute spend → estimates 17% (1yr) or 33% (3yr) savings |
| **Orphaned resources** | Unattached disks, unused public IPs, empty NSGs → instant cleanup wins |
| **Cost anomalies** | Services with >50% spike in last 7 days vs prior baseline |
| **Performance tracking** | Full CPU/memory/network/disk metrics per VM for capacity planning |
| **Azure Advisor** | Native Azure recommendations with savings estimates |

## Prerequisites

- Python 3.11+
- Azure subscription
- Azure service principal with these roles:
  - `Cost Management Reader`
  - `Reader` (subscription scope)
  - `Monitoring Reader`
- Anthropic API key

### Create a service principal

```bash
az ad sp create-for-rbac --name "azure-cost-optimizer" \
  --role "Cost Management Reader" \
  --scopes /subscriptions/<subscription-id>

az role assignment create \
  --assignee <client-id> \
  --role "Reader" \
  --scope /subscriptions/<subscription-id>

az role assignment create \
  --assignee <client-id> \
  --role "Monitoring Reader" \
  --scope /subscriptions/<subscription-id>
```

## Installation

```bash
git clone https://github.com/choll/azure-cost-optimizer.git
cd azure-cost-optimizer

python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

pip install -e .
```

## Configuration

```bash
cp .env.example .env
# Edit .env with your credentials
```

Required environment variables:

| Variable | Description |
|----------|-------------|
| `AZURE_TENANT_ID` | Azure AD tenant ID |
| `AZURE_CLIENT_ID` | Service principal client ID |
| `AZURE_CLIENT_SECRET` | Service principal secret |
| `AZURE_SUBSCRIPTION_ID` | Target subscription ID |
| `ANTHROPIC_API_KEY` | Anthropic API key |
| `LOOKBACK_DAYS` | Days of cost history to analyze (default: 30) |
| `CLAUDE_MODEL` | Claude model to use (default: claude-sonnet-4-6) |

## Usage

```bash
# Full pipeline (collect → structure → optimize)
python main.py

# Analyze last 14 days
python main.py --days 14

# Use cached raw data (skips Azure API calls — faster for iteration)
python main.py --raw-file outputs/raw/raw_20240101_120000.json
```

## Output Files

All outputs are saved to the `outputs/` directory:

```
outputs/
├── raw/
│   └── raw_YYYYMMDD_HHMMSS.json      # Raw Azure data (reusable with --raw-file)
├── reports/
│   └── report_<id>.json              # Structured cost report
└── recommendations/
    ├── optimization_<id>.json        # Full optimization data (JSON)
    └── optimization_<id>.md          # Human-readable Markdown report
```

### Sample Markdown Report Structure

```markdown
# Azure Cost Optimization Report

## Executive Summary
...

## Total Potential Savings: $X,XXX.00/month

## Current Cost Breakdown
| Service | Monthly Cost | % of Total |
...

## Priority Recommendations
### 1. 🔴 [CRITICAL] Delete 12 unattached managed disks
**Estimated Savings:** $340.00/month
**Implementation Effort:** Low
...

## VM Inventory
...

## Orphaned Resources
...
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Lint
ruff check .

# Test with cached data (no Azure/Claude API calls needed)
python main.py --raw-file tests/fixtures/sample_raw_data.json
```

## Cost Considerations

- **Azure API calls**: Cost Management queries count against your Azure API quota. The collector runs once and caches results.
- **Claude API**: Each full pipeline run makes ~2 Claude API calls (one per agent). Use `--raw-file` to re-run Agents 2 & 3 without re-collecting.
- **Rate limits**: VM metrics are collected in batches of 10 with 1-second delays to avoid Azure Monitor rate limits.
