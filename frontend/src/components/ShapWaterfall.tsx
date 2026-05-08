import React from "react";

interface ShapWaterfallProps {
  shapValues: Record<string, number>;
  baseValue: number;
  predictedValue: number;
  skuId: string;
  siteId: string;
}

export const ShapWaterfall: React.FC<ShapWaterfallProps> = ({
  shapValues,
  baseValue,
  predictedValue,
  skuId,
  siteId,
}) => {
  if (Object.keys(shapValues).length === 0) {
    return <div className="shap-waterfall"><p>No SHAP data available for this exception.</p></div>;
  }

  const sorted = Object.entries(shapValues).sort(
    ([, a], [, b]) => Math.abs(b) - Math.abs(a)
  );

  return (
    <div className="shap-waterfall">
      <h3>
        Why this order? — SKU {skuId} @ Store {siteId}
      </h3>
      <div className="shap-base">Base value: {baseValue.toFixed(2)}</div>
      <div className="shap-features">
        {sorted.map(([feature, value]) => (
          <div
            key={feature}
            className={`shap-bar ${value >= 0 ? "positive" : "negative"}`}
            style={{ "--bar-width": `${Math.abs(value) * 100}px` } as React.CSSProperties}
          >
            <span className="feature-name">{feature}</span>
            <span className="feature-value">
              {value >= 0 ? "+" : ""}
              {value.toFixed(3)}
            </span>
          </div>
        ))}
      </div>
      <div className="shap-prediction">
        Predicted: {Math.round(predictedValue)} units
      </div>
    </div>
  );
};
