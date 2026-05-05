import React from "react";

interface StoreStatus {
  site_id: string;
  store_name: string;
  no_touch_rate: number;
  edi_released: boolean;
  exceptions_pending: number;
  waste_pct: number;
}

interface FreshnessMapProps {
  stores: StoreStatus[];
}

const statusColor = (store: StoreStatus): string => {
  if (!store.edi_released) return "#ef4444";
  if (store.exceptions_pending > 0) return "#f59e0b";
  return "#22c55e";
};

export const FreshnessMap: React.FC<FreshnessMapProps> = ({ stores }) => {
  return (
    <div className="freshness-map">
      <h2>Store Status — {new Date().toLocaleTimeString()}</h2>
      <div className="store-grid">
        {stores.map((store) => (
          <div
            key={store.site_id}
            className="store-tile"
            style={{ borderLeft: `4px solid ${statusColor(store)}` }}
          >
            <div className="store-name">{store.store_name}</div>
            <div className="store-metrics">
              <span>No-Touch: {(store.no_touch_rate * 100).toFixed(0)}%</span>
              <span>Waste: {store.waste_pct.toFixed(1)}%</span>
              <span>{store.exceptions_pending} exceptions</span>
              <span>{store.edi_released ? "EDI ✓" : "EDI PENDING"}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};
