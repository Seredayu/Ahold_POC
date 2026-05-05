import React, { useState } from "react";
import { ShapWaterfall } from "./ShapWaterfall";

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
  onApprove: (id: string, overrideQty?: number) => void;
  onReject: (id: string) => void;
}

export const ExceptionQueue: React.FC<ExceptionQueueProps> = ({
  exceptions,
  onApprove,
  onReject,
}) => {
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <div className="exception-queue">
      <h2>Exception Queue ({exceptions.filter((e) => e.status === "PENDING").length} pending)</h2>
      <table>
        <thead>
          <tr>
            <th>SKU</th>
            <th>Store</th>
            <th>Type</th>
            <th>Qty</th>
            <th>Deviation</th>
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
                  <button onClick={(e) => { e.stopPropagation(); onApprove(exc.exception_id); }}>
                    Approve
                  </button>
                  <button onClick={(e) => { e.stopPropagation(); onReject(exc.exception_id); }}>
                    Reject
                  </button>
                </td>
              </tr>
              {selected === exc.exception_id && (
                <tr>
                  <td colSpan={6}>
                    <ShapWaterfall
                      shapValues={exc.shap_values}
                      baseValue={0}
                      predictedValue={exc.recommended_qty}
                      skuId={exc.sku_id}
                      siteId={exc.site_id}
                    />
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
