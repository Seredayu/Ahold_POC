from src.medallion.bronze.sap_ecc_open_orders import SAPECCOpenOrdersLoader


def test_loader_sets_correct_target_table(spark):
    loader = SAPECCOpenOrdersLoader(spark=spark)
    assert loader.target_table == "bronze.sap.open_orders"


def test_target_schema_contains_key_columns(spark):
    loader = SAPECCOpenOrdersLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "EBELN" in field_names
    assert "EBELP" in field_names


def test_target_schema_contains_vendor(spark):
    loader = SAPECCOpenOrdersLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "LIFNR" in field_names


def test_target_schema_contains_delivery_date(spark):
    loader = SAPECCOpenOrdersLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "EINDT" in field_names


def test_target_schema_contains_quantity_and_value(spark):
    loader = SAPECCOpenOrdersLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "MENGE" in field_names
    assert "NETPR" in field_names
