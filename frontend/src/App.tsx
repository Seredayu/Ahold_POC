import React, { useEffect, useState } from "react";
import { ExceptionQueue } from "./components/ExceptionQueue";
import { FreshnessMap } from "./components/FreshnessMap";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";
const MANAGER_ID = import.meta.env.VITE_MANAGER_ID ?? "dev-manager-01";

interface Exception {
  exception_id: string;
  sku_id: string;
  site_id: string;
  exception_type: string;
  recommended_qty: number;
  deviation_pct: number;
  shap_values: Record<string, number>;
  status: string;
}

export default function App() {
  const [exceptions, setExceptions] = useState<Exception[]>([]);
  const [tab, setTab] = useState<"queue" | "map">("queue");

  useEffect(() => {
    const fetchExceptions = () => {
      fetch(`${API_BASE}/exceptions/`)
        .then((r) => r.json())
        .then((data: Exception[]) => {
          const sorted = [...data].sort((a, b) => b.deviation_pct - a.deviation_pct);
          setExceptions(sorted);
        })
        .catch(() => {});
    };

    fetchExceptions();
    const interval = setInterval(fetchExceptions, parseInt(import.meta.env.VITE_POLLING_INTERVAL_MS ?? "15000"));
    return () => clearInterval(interval);
  }, []);

  const approve = (id: string, overrideQty?: number, note?: string) =>
    fetch(`${API_BASE}/exceptions/${id}/approve`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Manager-Id": MANAGER_ID,
      },
      body: JSON.stringify({ override_qty: overrideQty ?? null, reviewer_note: note ?? null }),
    }).then(() =>
      setExceptions((prev) => prev.filter((e) => e.exception_id !== id))
    );

  const reject = (id: string) =>
    fetch(`${API_BASE}/exceptions/${id}/reject`, {
      method: "POST",
      headers: {
        "X-Manager-Id": MANAGER_ID,
      },
    }).then(() =>
      setExceptions((prev) => prev.filter((e) => e.exception_id !== id))
    );

  return (
    <div className="app">
      <header>
        <h1>Albert Heijn — Freshness Replenishment</h1>
        <nav>
          <button onClick={() => setTab("queue")} className={tab === "queue" ? "active" : ""}>
            Exception Queue
          </button>
          <button onClick={() => setTab("map")} className={tab === "map" ? "active" : ""}>
            Store Map
          </button>
        </nav>
      </header>
      <main>
        {tab === "queue" && (
          <ExceptionQueue exceptions={exceptions} onApprove={approve} onReject={reject} />
        )}
        {tab === "map" && <FreshnessMap stores={[]} />}
      </main>
    </div>
  );
}
