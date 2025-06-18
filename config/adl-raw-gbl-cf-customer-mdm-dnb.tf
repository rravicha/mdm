variable "rw_buckets" {
  type = list(string)
  default = [
    "adl-salesforce-etl/adl-enriched-gbl-cf-salesforce",
    "adl-salesforce-etl/adl-enriched-gbl-cf-salesforce-archive",
    "adl-reference-etl/adl-enriched-gbl-cf-reference",
    "adl-ebx-etl/adl-enriched-gbl-cf-ebx"
  ]

}

variable "tags" {
  type = map(string)
  default = {
    AppId = "APM0002069",
    AppName = "MDM"
  }
}

locals {
  name             = "adl-raw-gbl-cf-customer-mdm-dnb"
  description      = "adl-raw-gbl-cf-customer-mdm"
  dynamodb_enabled = false
  crawler_enabled  = false
  redshift_enabled = false
  create_database  = false


    glue_extra_permissions = [
    {
      effect = "Allow"
      actions = [
        "glue:GetTable"
      ]
      resources = [
        "arn:aws:glue:us-east-1:${data.aws_caller_identity.this.account_id}:database/global_temp",
        "arn:aws:glue:us-east-1:${data.aws_caller_identity.this.account_id}:database/default",
        "arn:aws:glue:us-east-1:${data.aws_caller_identity.this.account_id}:catalog"
      ]
    }
  ]

  helpers = {
      class = {
      dev  = "FLEX"
      test = "FLEX"
      prod = "STANDARD"
    }
  }


  workflow_sm = [

    {
      state = "rawmdm-dnb"
      type = "classic"
      next = "proceed_further"
      flow = [
        {
          name            = "DNB_raw" // Job name
          type            = "glueetl"      // [ glueetl || pythonshell || python3.7 ]
          glue_version    = "4.0"
          script_location = "adl-customer-mdm-etl/releases/${var.artifacts_version}/raw_dnb_hco.py"
          handler         = ""      // Uses only in lambda function
          timeout         = 180     // timeout. For lambda in sec and in minutes for glue
          max_capacity    = 2       // memory size for lambda and dpu for glue
          connection      = ""      // connection for glue etl jobs and vpc endpoint for lambda
          execution_class = local.helpers.class[var.environment]
          // map of args for glue etl job and environment variables for lambda
          args = {
            // special glue args
            "--enable-metrics"            = "true"
            "--enable-auto-scaling"       = "true"
            "--job-language"              = "python"
            "--enable-glue-datacatalog"   = ""
            "--TempDir"                   = "s3://${var.access_point.alias}/trusted/__tmp/${local.name}/"
            "--python_version"            = 3
            "--additional-python-modules" = "spacy"
            "--job-bookmark-option"       = "job-bookmark-disable"
            "--extra-py-files"            = "s3://${var.artifact_bucket}/adl-customer-mdm-etl/releases/${var.artifacts_version}/adl_Customer_MDM_ETL-1.0.0-py3.6.egg"
            //"--datalake-formats"          = "iceberg"
            //"--conf"                      = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.iceberg_data_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.iceberg_data_catalog.warehouse=\"${local.tfstate_dp["adl-reference-etl/adl-enriched-gbl-cf-reference"].bucket.id}\" --conf spark.sql.catalog.iceberg_data_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.iceberg_data_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
          }
          // map of args to pass through SFN (Payload)
          sfn_args = {
        //   "--TARGET_DATABASE" = aws_glue_catalog_database.this[0].name
          }
          // map of args to pass through SFN (Parameters)
          sfn_params = {
            WorkerType      = "G.1X"
            NumberOfWorkers = 2
          }
        }
      ]
    }
  ]
}

