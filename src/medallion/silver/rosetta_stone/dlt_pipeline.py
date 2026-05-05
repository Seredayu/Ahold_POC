import dlt
from src.medallion.silver.rosetta_stone.entity_resolution import build_registry


@dlt.table(
    name="unified_sku_registry",
    comment="Rosetta Stone: SAP MATNR → EAN11 → unified_sku_id",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_matnr", "sap_material_number IS NOT NULL")
@dlt.expect("match_rate_informational", "match_confidence >= 0.0")
def unified_sku_registry():
    bronze = spark.read.table("bronze.sap.materials")  # noqa: F821 — spark injected by DLT
    return build_registry(bronze)
