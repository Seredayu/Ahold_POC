from dataclasses import dataclass
from typing import Optional


@dataclass
class UnifiedSKU:
    sap_material_number: str
    symphony_gold_item_code: Optional[str]
    ean_barcode: Optional[str]
    unified_sku_id: str
    match_confidence: float
    source_banner: str  # "AH_NL" | "MEGA_IMAGE_RO" etc.


class RosettaStone:
    """Maps SAP ECC material numbers ↔ Symphony Gold item codes ↔ EAN barcodes.

    Outputs to silver.master.unified_sku_registry.
    Match rate must be ≥95% before model training can proceed (Week 5 gate).
    Uses Databricks Entity Resolution Solution Accelerator under the hood.
    """

    TARGET_MATCH_RATE = 0.95

    def __init__(self, spark, catalog: str = "silver", schema: str = "master"):
        self._spark = spark
        self._table = f"{catalog}.{schema}.unified_sku_registry"

    def get_match_rate(self) -> float:
        df = self._spark.table(self._table)
        total = df.count()
        matched = df.filter(
            df.symphony_gold_item_code.isNotNull() & df.ean_barcode.isNotNull()
        ).count()
        return matched / total if total > 0 else 0.0

    def is_ready(self) -> bool:
        return self.get_match_rate() >= self.TARGET_MATCH_RATE

    def lookup_by_sap(self, sap_material_number: str) -> Optional[UnifiedSKU]:
        df = self._spark.table(self._table).filter(
            f"sap_material_number = '{sap_material_number}'"
        )
        rows = df.collect()
        if not rows:
            return None
        r = rows[0]
        return UnifiedSKU(
            sap_material_number=r.sap_material_number,
            symphony_gold_item_code=r.symphony_gold_item_code,
            ean_barcode=r.ean_barcode,
            unified_sku_id=r.unified_sku_id,
            match_confidence=r.match_confidence,
            source_banner=r.source_banner,
        )
