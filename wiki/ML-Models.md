# ML Models

> Last updated: 2026-05-08 | Sources: [ahold_delhaize_inventory_optimization_architecture.md, Ahold Delhaize - 1.md]

## Summary

Layer 5 of the architecture contains five specialized ML engines managed via **MLflow Model Registry**. They run in sequence each morning, generating probabilistic insights that feed the Freshness and Sweeper engines. All models are trained on Databricks ML Runtime and served via Databricks Model Serving (REST APIs).

## Model 1: Demand Forecasting

- **Algorithm**: Prophet (seasonal baseline) → LightGBM (production); LSTM available as deep-learning option
- **Granularity**: Store-SKU-Day, DC-SKU-Day, Category-Week
- **Outputs**: P10/P50/P90 demand forecasts, forecast accuracy scores, confidence intervals
- **Base**: Databricks Demand Forecasting Solution Accelerator (hierarchical forecasting)
- **Feature inputs**: 7/14/30-day sales moving averages, day-of-week/month patterns, seasonality decomposition, promotion lift, weather, foot traffic, local events

## Model 2: Waste Prediction / Phantom Stock Detection

- **Algorithm**: XGBoost Classifier (primary), Random Forest, Logistic Regression
- **Targets**: Waste probability, expected waste $, optimal markdown %; also invisible stockout (phantom stock) detection
- **Triggers**: Markdown alerts, donation triggers, reorder prevention
- **Write-back**: Phantom stock corrections via `BAPI_GOODSMVT_CREATE`
- **Code**: `src/engines/phantom_stock/`

## Model 3: Dynamic Pricing

- **Algorithm**: Reinforcement Learning (PPO/DQN), Price Elasticity Model
- **Optimization objectives**: Margin maximization, inventory turnover, freshness preservation
- **Outputs**: Optimal price points, promotion timing recommendations, discount schedules

## Model 4: Replenishment Optimization (MILP)

- **Algorithm**: Multi-Echelon Inventory Optimization (MEIO); POC uses MILP via PuLP with CBC solver
- **Code**: `src/engines/freshness/replenishment_quantity_optimizer.py`
- **Solver interface**: Abstracted behind `SolverInterface` base class — OR-Tools/Gurobi can be swapped without touching engine logic
- **Key constraint**: Transit-to-Life (shelf-life must exceed transit time)
- **Hard constraints**: truck capacity, warehouse space, shelf-life limits, minimum order quantities (MOQs)
- **Output**: `gold.replenishment.order_recommendations`

## Model 5: AI-Driven Supplier Selection

- **Algorithm**: Multi-criteria decision model
- **Scoring dimensions**: Quality, lead time reliability, price competitiveness, sustainability score
- **Outputs**: Preferred supplier ranking, split-buy recommendations, new supplier suggestions

## Feature Store

**Databricks Feature Store** centralizes all signals for consistent use between training and real-time inference:

| Feature Group | Examples |
|---|---|
| Time-Series | 7/14/30-day moving averages, day-of-week patterns, seasonality, trend |
| Freshness | Days to expiry, shelf-life remaining %, temperature compliance, QM quality score |
| Promotion | Active flag, discount depth, historical lift, cross-promotion effects |
| External | Weather (temp/precip), events/holidays, competitor pricing, foot traffic |
| Supply Chain | Lead time variance, supplier reliability score, transit time predictions, warehouse capacity |
| Categorical | Store cluster (A/B/C), product category, brand, supplier region |

## MLOps

- **Experiment tracking**: MLflow
- **Model Registry**: Staging → Production promotion workflow with A/B testing
- **Model lineage & reproducibility**: Full provenance via Unity Catalog + MLflow
- **Retraining**: Weekly model updates based on forecast accuracy feedback
- **Drift detection**: Databricks Lakehouse Monitoring + Azure Monitor

## SHAP Explainability

The React app's `ShapWaterfall.tsx` component is the **primary store-manager trust mechanism**. It must render a SHAP waterfall chart for every exception in the queue. This is load-bearing for user adoption — do not remove or degrade.

## Related

[[Index]] [[Architecture-Overview]] [[Replenishment-Engine]] [[Data-Integration]] [[Frontend]]
