from setuptools import setup, find_packages

setup(
    name="ahold_freshness_poc",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={
        "console_scripts": [
            # Phase 1 — Gold aggregates
            "gold_daily_positions=medallion.gold.aggregates:_entry_daily_positions",
            "gold_sales_velocity=medallion.gold.aggregates:_entry_sales_velocity",
            "gold_open_orders=medallion.gold.aggregates:_entry_open_orders",
            # Phase 2A — Feature Store
            "fs_velocity_features=medallion.feature_store.velocity_features:_entry_velocity_features",
            "fs_stock_features=medallion.feature_store.stock_features:_entry_stock_features",
            "fs_collapse_signals=medallion.feature_store.collapse_signals:_entry_collapse_signals",
            # Phase 2B — Phantom Stock Detector
            "phantom_generate_labels=engines.phantom_stock.label_generator:_entry_generate_labels",
            "phantom_train_model=engines.phantom_stock.classifier:_entry_train_model",
            "phantom_score_batch=engines.phantom_stock.feature_pipeline:_entry_score_batch",
            "phantom_write_alerts=engines.phantom_stock.feature_pipeline:_entry_write_alerts",
            "phantom_bapi_writeback=engines.phantom_stock.feature_pipeline:_entry_bapi_writeback",
            # Phase 3A — Demand Models (M1/M2/M4)
            "demand_train_m1=engines.demand.m1_model:_entry_train_m1",
            "demand_score_m1=engines.demand.m1_model:_entry_score_m1",
            "demand_ingest_weather=engines.demand.weather_ingest:_entry_ingest_weather",
            "demand_train_m2=engines.demand.m2_model:_entry_train_m2",
            "demand_score_m2=engines.demand.m2_model:_entry_score_m2",
            "demand_train_m4=engines.demand.m4_model:_entry_train_m4",
            "demand_score_m4=engines.demand.m4_model:_entry_score_m4",
            # Phase 3B — Freshness Orchestrator
            "freshness_milp_solve=engines.freshness.freshness_pipeline:_entry_freshness_milp_solve",
            "freshness_write_recommendations=engines.freshness.freshness_pipeline:_entry_write_recommendations",
            "freshness_bapi_po_create=engines.freshness.freshness_pipeline:_entry_bapi_po_create",
            # Phase 4A — Sweeper + EDI
            "sweeper_load_exceptions=engines.sweeper.sweeper_pipeline:_entry_load_exceptions",
            "sweeper_finalize=engines.sweeper.sweeper_pipeline:_entry_finalize",
        ],
    },
    python_requires=">=3.8",
    install_requires=[],
)
