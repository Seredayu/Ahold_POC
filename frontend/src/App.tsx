import React, { useEffect, useState } from "react";
import { ExceptionQueue } from "./components/ExceptionQueue";
import { FreshnessMap } from "./components/FreshnessMap";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";
const MANAGER_ID = import.meta.env.VITE_MANAGER_ID ?? "dev-manager-01";
// TODO Production: replace VITE_MANAGER_ID with Azure AD claim from X-MS-CLIENT-PRINCIPAL
// token parsed by the Static Web Apps auth middleware. This env-var approach is POC-only.

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
  const [error, setError] = useState<string | null>(null);

  const loadExceptions = (isInitial: boolean) => {
    fetch(`${API_BASE}/exceptions/?status=PENDING`, {
      headers: { "X-Manager-Id": MANAGER_ID },
    })
      .then((r) => r.json())
      .then((data: Exception[]) => {
        if (isInitial) {
          // First load: sort by deviation_pct desc
          setExceptions(data.sort((a, b) => b.deviation_pct - a.deviation_pct));
        } else {
          // Subsequent polls: add new exceptions at end, don't re-sort
          setExceptions(prev => {
            const existingIds = new Set(prev.map(e => e.exception_id));
            const newExcs = data.filter(e => !existingIds.has(e.exception_id));
            return [...prev, ...newExcs];
          });
        }
      })
      .catch(() => {});
  };

  useEffect(() => {
    loadExceptions(true);
    const id = setInterval(() => loadExceptions(false), parseInt(import.meta.env.VITE_POLLING_INTERVAL_MS ?? "15000") || 15000);
    return () => clearInterval(id);
  }, []);

  const approve = (id: string, overrideQty?: number, note?: string) =>
    fetch(`${API_BASE}/exceptions/${id}/approve`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Manager-Id": MANAGER_ID },
      body: JSON.stringify({ override_qty: overrideQty ?? null, reviewer_note: note ?? null }),
    })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        setExceptions((prev) => prev.filter((e) => e.exception_id !== id));
      })
      .catch((err) => {
        console.error("Approve failed:", err);
        setError(`Failed to approve exception. Please try again.`);
      });

  const reject = (id: string, note?: string) =>
    fetch(`${API_BASE}/exceptions/${id}/reject`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Manager-Id": MANAGER_ID },
      body: JSON.stringify({ reviewer_note: note ?? null }),
    })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        setExceptions((prev) => prev.filter((e) => e.exception_id !== id));
      })
      .catch((err) => {
        console.error("Reject failed:", err);
        setError(`Failed to reject exception. Please try again.`);
      });

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
      {error && <div className="error-banner" role="alert">{error}<button onClick={() => setError(null)}>×</button></div>}
      <main>
        {tab === "queue" && (
          <ExceptionQueue exceptions={exceptions} onApprove={approve} onReject={reject} />
        )}
        {tab === "map" && <FreshnessMap stores={[]} />}
      </main>
    </div>
  );
}
