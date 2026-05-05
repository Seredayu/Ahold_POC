from src.medallion.bronze.sap_ecc_materials import SAPECCMaterialsLoader


def test_loader_sets_correct_target_table(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    assert loader.target_table == "bronze.sap.materials"


def test_target_schema_contains_matnr(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "MATNR" in field_names


def test_target_schema_contains_werks(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "WERKS" in field_names


def test_target_schema_contains_shelf_life_fields(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "MHDRZ" in field_names
    assert "IPRKZ" in field_names


def test_add_audit_columns_present_after_transform(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    df = spark.createDataFrame([("000000000000001234", "NL01")], ["MATNR", "WERKS"])
    result = loader.add_audit_columns(df)
    assert "_ingested_at" in result.columns
