from src.medallion.bronze.sap_ecc_inventory import SAPECCInventoryLoader


def test_loader_sets_correct_target_table(spark):
    loader = SAPECCInventoryLoader(spark=spark)
    assert loader.target_table == "bronze.sap.inventory"


def test_target_schema_contains_key_columns(spark):
    loader = SAPECCInventoryLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "MBLNR" in field_names
    assert "ZEILE" in field_names


def test_target_schema_contains_movement_type(spark):
    loader = SAPECCInventoryLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "BWART" in field_names


def test_target_schema_contains_quantity(spark):
    loader = SAPECCInventoryLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "MENGE" in field_names
