# Frontend

> Last updated: 2026-05-08 | Sources: [ahold_delhaize_inventory_optimization_architecture.md, Ahold Delhaize - 1.md]

## Summary

Layer 8 of the architecture includes three user interface surfaces. The React Field App is the primary store-manager tool for "Management by Exception" — reviewing the ~10% of orders not auto-approved by the Sweeper.

## React Field App (Store Managers / Associates)

- **Stack**: React 18 + TypeScript + Vite
- **Backend**: FastAPI on Azure Container Apps
- **Hosting**: Azure Static Web Apps with Azure AD SSO (Entra ID)
- **Design**: Mobile-first, offline-capable
- **Users**: Store associates, warehouse staff, delivery drivers

**Features**:
- Management by Exception — review and approve/override replenishment recommendations
- SHAP Waterfall explainability for every exception in the queue
- Barcode scanning
- Real-time stock check
- Markdown approval workflow
- Photo upload for quality issues
- Task management

## SHAP Waterfall (Critical Component)

`ShapWaterfall.tsx` is the **primary store-manager trust mechanism**. It renders a SHAP waterfall chart showing which features drove each AI recommendation.

**Load-bearing constraint**: Must render for every exception in the queue. Removing or degrading this component undermines user adoption. Do not refactor it away.

## Power BI (Operational Dashboards)

- **Users**: Store managers, category managers, supply chain teams, buyers
- **Refresh**: Real-time
- **Dashboards**:
  - Daily inventory positions
  - Stockout alerts (by SKU, store, time-of-day)
  - Waste tracking (YoY comparison)
  - Forecast vs. actuals
  - Replenishment queues (automation rate by store)

## SAP Analytics Cloud (Strategic / Executive)

- **Users**: C-suite, Category VPs, Finance, Strategy
- **Refresh**: Daily
- **Analytics**:
  - Waste trends
  - Forecast accuracy
  - Margin analysis
  - Supplier scorecard
  - Regional performance
  - ROI metrics / €350M+ EBITDA tracker
  - ESG/Sustainability targets

## Technology Stack

| Component | Technology |
|---|---|
| Frontend framework | React 18 + TypeScript + Vite |
| Backend API | FastAPI (Python) |
| Hosting | Azure Static Web Apps |
| Backend hosting | Azure Container Apps |
| Auth | Azure AD SSO (Entra ID) |
| SQL engine | Databricks SQL Warehouse |
| Operational BI | Power BI |
| Strategic BI | SAP Analytics Cloud |

## Related

[[Index]] [[Architecture-Overview]] [[ML-Models]] [[Replenishment-Engine]] [[Infrastructure]]
