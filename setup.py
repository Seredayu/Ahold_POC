from setuptools import setup, find_packages

setup(
    name="ahold_freshness_poc",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={
        "console_scripts": [
            "gold_daily_positions=medallion.gold.aggregates:_entry_daily_positions",
            "gold_sales_velocity=medallion.gold.aggregates:_entry_sales_velocity",
            "gold_open_orders=medallion.gold.aggregates:_entry_open_orders",
            "fs_velocity_features=medallion.feature_store.velocity_features:_entry_velocity_features",
            "fs_stock_features=medallion.feature_store.stock_features:_entry_stock_features",
            "fs_collapse_signals=medallion.feature_store.collapse_signals:_entry_collapse_signals",
        ],
    },
    python_requires=">=3.8",
    install_requires=[],
)
