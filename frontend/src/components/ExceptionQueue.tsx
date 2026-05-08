import React, { useEffect, useState } from "react";
import { ShapWaterfall } from "./ShapWaterfall";
import "./ExceptionQueue.css";

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

interface ExceptionQueueProps {
  exceptions: Exception[];
  onApprove: (id: string, overrideQty?: number, note?: string) => void;
  onReject: (id: string, note?: string) => void;
}

export const ExceptionQueue: React.FC<ExceptionQueueProps> = ({
  exceptions,
  onApprove,
  onReject,
}) => {
  const [selected, setSelected] = useState<string | null>(null);
  const [overrideInputs, setOverrideInputs] = useState<Map<string, { qty?: number; note?: string }>>(new Map());

  useEffect(() => {
    setOverrideInputs(prev => {
      const currentIds = new Set(exceptions.map(e => e.exception_id));
      const next = new Map(prev);
      for (const key of next.keys()) {
        if (!currentIds.has(key)) next.delete(key);
      }
      return next;
    });
  }, [exceptions]);

  const setNote = (id: string, note: string) => {
    setOverrideInputs((prev) => {
      const next = new Map(prev);
      const existing = next.get(id) ?? {};
      next.set(id, { ...existing, note: note || undefined });
      return next;
    });
  };

  return (
    <div className="exception-queue">
      <h2>Exception Queue ({exceptions.filter((e) => e.status === "PENDING").length} pending)</h2>
      <table className="responsive-table">
        <thead>
          <tr>
            <th>SKU</th>
            <th>Store</th>
            <th>Type</th>
            <th>Qty</th>
            <th>Deviation</th>
            <th>Status</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {exceptions.map((exc) => (
            <React.Fragment key={exc.exception_id}>
              <tr onClick={() => setSelected(selected === exc.exception_id ? null : exc.exception_id)}>
                <td>{exc.sku_id}</td>
                <td>{exc.site_id}</td>
                <td>{exc.exception_type}</td>
                <td>{exc.recommended_qty}</td>
                <td>{(exc.deviation_pct * 100).toFixed(1)}%</td>
                <td>
                  <span className={`status-badge status-${exc.status.toLowerCase()}`}>{exc.status}</span>
                </td>
                <td>
                  <button onClick={(e) => { e.stopPropagation(); onApprove(exc.exception_id, overrideInputs.get(exc.exception_id)?.qty, overrideInputs.get(exc.exception_id)?.note); }}>
                    Approve
                  </button>
                  <button onClick={(e) => { e.stopPropagation(); onReject(exc.exception_id, overrideInputs.get(exc.exception_id)?.note); }}>
                    Reject
                  </button>
                </td>
              </tr>
              {selected === exc.exception_id && (
                <tr>
                  <td colSpan={7}>
                    <ShapWaterfall
                      shapValues={exc.shap_values}
                      baseValue={0}
                      predictedValue={exc.recommended_qty}
                      skuId={exc.sku_id}
                      siteId={exc.site_id}
                    />
                    <div style={{ marginTop: 8 }}>
                      <input
                        type="number"
                        placeholder="Override qty (optional)"
                        value={overrideInputs.get(exc.exception_id)?.qty ?? ""}
                        onChange={(e) => {
                          const v = e.target.valueAsNumber;
                          setOverrideInputs(prev => {
                            const next = new Map(prev);
                            const existing = next.get(exc.exception_id) ?? {};
                            next.set(exc.exception_id, { ...existing, qty: Number.isFinite(v) && v > 0 ? Math.round(v) : undefined });
                            return next;
                          });
                        }}
                        style={{ marginRight: 8 }}
                      />
                      <textarea
                        placeholder="Note (optional)"
                        value={overrideInputs.get(exc.exception_id)?.note ?? ""}
                        onChange={(e) => setNote(exc.exception_id, e.target.value)}
                        rows={2}
                      />
                    </div>
                  </td>
                </tr>
              )}
            </React.Fragment>
          ))}
        </tbody>
      </table>
    </div>
  );
};
