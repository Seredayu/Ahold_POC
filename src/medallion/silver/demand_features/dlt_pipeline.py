import dlt
from src.medallion.silver.demand_features.transformations import (
    build_enriched_movements,
    clean_inventory,
    clean_materials,
    clean_open_orders,
)


@dlt.table(
    name="materials_clean",
    comment="Cleaned SAP materials — deduped, filtered to FERT/HALB/ROH",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_matnr", "MATNR IS NOT NULL")
def materials_clean():
    bronze = spark.read.table("bronze.sap.materials")  # noqa: F821
    return clean_materials(bronze)


@dlt.table(
    name="inventory_clean",
    comment="Cleaned SAP inventory movements — deduped, valid BWART only",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_document", "MBLNR IS NOT NULL AND ZEILE IS NOT NULL")
def inventory_clean():
    bronze = spark.read.table("bronze.sap.inventory")  # noqa: F821
    return clean_inventory(bronze)


@dlt.table(
    name="open_orders_clean",
    comment="Cleaned SAP open PO lines — deduped, MENGE > 0",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_po_line", "EBELN IS NOT NULL AND EBELP IS NOT NULL")
def open_orders_clean():
    bronze = spark.read.table("bronze.sap.open_orders")  # noqa: F821
    return clean_open_orders(bronze)


@dlt.table(
    name="enriched_movements",
    comment="Inventory movements enriched with material attributes and unified_sku_id",
    table_properties={"quality": "silver"},
)
def enriched_movements():
    inv = dlt.read("inventory_clean")
    mat = dlt.read("materials_clean")
    sku = spark.read.table("silver.master.unified_sku_registry")  # noqa: F821
    return build_enriched_movements(inv, mat, sku)
