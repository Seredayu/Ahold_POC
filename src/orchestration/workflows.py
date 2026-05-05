"""Databricks Workflow definitions for the 05:00–08:15 AM replenishment window."""

FRESHNESS_WORKFLOW = {
    "name": "freshness-replenishment-daily",
    "schedule": {
        "quartz_cron_expression": "0 0 5 * * ?",  # 05:00 AM daily
        "timezone_id": "Europe/Amsterdam",
    },
    "tasks": [
        {
            "task_key": "engine1_phantom_stock",
            "description": "XGBoost phantom stock detection → BAPI_GOODSMVT_CREATE",
            "notebook_task": {"notebook_path": "/Shared/freshness-poc/engine1_phantom_stock"},
            "timeout_seconds": 1800,
        },
        {
            "task_key": "engine2_freshness_milp",
            "description": "MILP replenishment optimizer with Transit-to-Life constraint",
            "notebook_task": {"notebook_path": "/Shared/freshness-poc/engine2_freshness_milp"},
            "depends_on": [{"task_key": "engine1_phantom_stock"}],
            "timeout_seconds": 2700,
        },
        {
            "task_key": "engine3_sweeper",
            "description": "Deterministic sweeper → BAPI_PO_CREATE1 + EDI 850 by 08:15",
            "notebook_task": {"notebook_path": "/Shared/freshness-poc/engine3_sweeper"},
            "depends_on": [{"task_key": "engine2_freshness_milp"}],
            "timeout_seconds": 7200,  # runs until 08:15 AM
        },
    ],
    "max_concurrent_runs": 1,
    "email_notifications": {
        "on_failure": ["replenishment-oncall@ah.nl"],
    },
}
