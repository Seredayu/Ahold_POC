# Infrastructure

> Last updated: 2026-05-08 | Sources: [ahold_delhaize_inventory_optimization_architecture.md, Ahold Delhaize - 1.md]

## Summary

The platform runs on **Databricks Lakehouse on Azure** (existing Ahold Delhaize standard). Two regional Databricks workspaces serve different ERP regions. Unity Catalog provides cross-workspace governance throughout.

## Cloud Platform

- **Cloud**: Microsoft Azure (Ahold Delhaize corporate standard)
- **Data Platform**: Databricks Lakehouse
- **Storage**: Azure Data Lake Storage Gen2 (ADLS)
- **Networking**: Azure ExpressRoute (SAP connectivity)
- **Compute**: Databricks Clusters (autoscaling)

## Databricks Architecture

### Workspaces
- **West Europe workspace**: Serves SAP ECC countries (NL, BE, LU)
- **Central Europe workspace**: Serves Symphony countries (CZ, RO, etc.)
- **Unity Catalog**: Cross-workspace data sharing with governance; centralized model training with regional fine-tuning

### Core Components
- **DLT (Delta Live Tables)**: Silver layer pipelines; declarative, self-healing, incremental
- **Feature Store**: Centralized signal repository for consistent train/serve
- **MLflow**: Experiment tracking, Model Registry (Staging → Production), model lineage
- **Mosaic AI Agent Framework**: Sweeper Engine tooling
- **Databricks Workflows**: Job orchestration
- **Databricks SQL Warehouse**: Query engine for BI and analytics
- **Databricks Model Serving**: REST API endpoints for model inference
- **Databricks Lakehouse Monitoring**: Model drift detection, pipeline health, SLA monitoring

## Azure Services

| Service | Role |
|---|---|
| Azure Data Lake Storage Gen2 | Raw + processed data storage |
| Azure ExpressRoute | Dedicated SAP connectivity |
| Azure Container Apps | FastAPI backend hosting |
| Azure Static Web Apps | React frontend hosting |
| Azure Logic Apps | EDI 850 file pickup + SFTP delivery |
| Azure Monitor / App Insights | Observability, alerting |
| Azure AD / Entra ID | Authentication + SSO |
| Azure DevOps / GitHub | Version control + CI/CD |
| Azure Pipelines | CI/CD for code and Databricks Asset Bundles |

## EDI Transport

- EDI 850 files written to `gold.edi.outbound/` (Azure Blob Storage)
- Azure Logic App triggered by Blob Storage event
- Logic App delivers to supplier SFTP
- **Never** route EDI directly from Databricks to external SFTP

## Security & Governance

- **Authentication**: Azure AD / Entra ID
- **Authorization**: Role-based access control (RBAC) via Unity Catalog
- **Data protection**: Row/column-level security, data masking for PII
- **Compliance**: GDPR via Unity Catalog (end-to-end lineage, retention policies)
- **Audit**: Full audit logging; encryption at rest and in transit

## Monitoring & Observability

- Databricks Lakehouse Monitoring — data quality checks, pipeline health
- Azure Monitor / Application Insights — infrastructure and app observability
- MLflow experiment tracking — model performance, forecast accuracy
- Cost tracking and optimization dashboards
- SLA monitoring for the 08:15 AM EDI deadline

## DevOps / MLOps

- **Version Control**: Azure DevOps / GitHub
- **CI/CD**: Azure Pipelines + Databricks Asset Bundles
- **Data Quality**: Great Expectations
- **Code Testing**: pytest
- **Model Retraining**: Weekly, triggered by performance metrics

## Implementation Roadmap

| Phase | Timeline | Scope |
|---|---|---|
| Phase 1: Foundation | Months 1–3 | Databricks workspaces, connectivity, Bronze→Gold DLT, Unity Catalog |
| Phase 2: Pilot (Fresh Produce) | Months 4–6 | AH NL 50 stores, demand forecasting, waste prediction, baseline KPIs |
| Phase 3: Scale (All Perishables) | Months 7–9 | All perishables, ~1000 AH stores, dynamic pricing, 80% automation |
| Phase 4: Regional Expansion | Months 10–12 | Belgium (Delhaize), Central/Eastern Europe (Symphony), 85% automation |
| Phase 5: Full Rollout | Months 13–18 | All banners, all categories, cross-banner optimization, sustainability scoring |

## Related

[[Index]] [[Architecture-Overview]] [[Data-Integration]] [[Frontend]]
