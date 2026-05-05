terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.workspace]
    }
  }
}

resource "databricks_connection" "sap_ecc" {
  provider        = databricks.workspace
  name            = "sap-ecc-connection"
  connection_type = "SAP_ERP"
  comment         = "SAP ECC 6.0 RFC connection for Lakeflow Connect CDC"

  options = {
    host         = "{{secrets/${var.secret_scope_name}/sap-ecc-host}}"
    systemNumber = "{{secrets/${var.secret_scope_name}/sap-ecc-sysnr}}"
    clientId     = "{{secrets/${var.secret_scope_name}/sap-ecc-client}}"
    user         = "{{secrets/${var.secret_scope_name}/sap-ecc-username}}"
    password     = "{{secrets/${var.secret_scope_name}/sap-ecc-password}}"
  }
}

resource "databricks_pipeline" "lakeflow_sap_bronze" {
  provider = databricks.workspace
  name     = "${var.prefix}-lakeflow-sap-bronze"
  target   = var.bronze_catalog
  channel  = "PREVIEW"

  cluster {
    policy_id = var.cluster_policy_id
    label     = "default"
  }

  ingestion_definition {
    connection_name = databricks_connection.sap_ecc.name

    table {
      source_schema       = "SAP_ECC"
      source_table        = "MARA"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "materials_mara"
    }

    table {
      source_schema       = "SAP_ECC"
      source_table        = "MARC"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "materials_marc"
    }

    table {
      source_schema       = "SAP_ECC"
      source_table        = "MSEG"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "inventory_mseg"
    }

    table {
      source_schema       = "SAP_ECC"
      source_table        = "MKPF"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "inventory_mkpf"
    }

    table {
      source_schema       = "SAP_ECC"
      source_table        = "EKKO"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "open_orders_ekko"
    }

    table {
      source_schema       = "SAP_ECC"
      source_table        = "EKPO"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "open_orders_ekpo"
    }
  }
}
