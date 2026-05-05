import React, { useEffect, useState } from "react";
import { ExceptionQueue } from "./components/ExceptionQueue";
import { FreshnessMap } from "./components/FreshnessMap";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";

export default function App() {
  const [exceptions, setExceptions] = useState([]);
  const [tab, setTab] = useState<"queue" | "map">("queue");

  useEffect(() => {
    fetch(`${API_BASE}/exceptions/`)
      .then((r) => r.json())
      .then(setExceptions);
  }, []);

  const approve = (id: string) =>
    fetch(`${API_BASE}/exceptions/${id}/approve`, { method: "POST" }).then(() =>
      setExceptions((prev: any[]) =>
        prev.map((e) => (e.exception_id === id ? { ...e, status: "APPROVED" } : e))
      )
    );

  const reject = (id: string) =>
    fetch(`${API_BASE}/exceptions/${id}/reject`, { method: "POST" }).then(() =>
      setExceptions((prev: any[]) =>
        prev.map((e) => (e.exception_id === id ? { ...e, status: "BLOCKED" } : e))
      )
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
