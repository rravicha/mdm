variable "rw_buckets" {
  type = list(string)
  default = [
    "adl-customer-mdm-etl/adl-trusted-gbl-cf-customer-mdm","adl-customer-mdm-etl/adl-enriched-gbl-cf-customer-mdm"
  ]
}

variable "tags" {
  type = map(string)
  default = {
    AppId   = "APM0002069"
    AppName = "MDM"
  }
}

locals {
  name = "adl-enriched-gbl-cf-customer-mdm-out-hco-api" 
  dynamodb_enabled  = true
  crawler_enabled   = false
  redshift_enabled  = false
  create_database   = false

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
    },
    {
      effect = "Allow"
      actions = [
        "ec2:*"
      ]
      resources = [
        "*"
      ]
    },
    {
    effect    = "Allow"
    actions   = [
      "s3:ListBucket",
      "s3:GetObject"
    ]
    resources = [
      "arn:aws:s3:::adl-spark-event-logs-${var.environment}",
      "arn:aws:s3:::adl-spark-event-logs-${var.environment}/*"
    ]
    },
    {
      effect    = "Allow"
      actions   = [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject"
      ]
      resources = [
        "arn:aws:s3:::adl-spark-event-logs-${var.environment}/adl-customer-mdm-etl/*"
      ]
    }
  ]

  workflow_static_input = <<EOF
{
  "table_list": null, "drop_tables": null, "exclude_tables": null
}
EOF

  helpers = {
    class = {
      dev  = "STANDARD"
      test = "FLEX"
      prod = "STANDARD"
    }
  }

      workflow_sm = [

    {
      state = "enrichedmdm-sap"
      type = "classic"
      next = "proceed_further"
      flow = [
        {
          name            = "trust_to_enrich_out_hco_api" // Job name
          type            = "glueetl"      // [ glueetl || pythonshell || python3.7 ]
          glue_version    = "4.0"
          script_location = "adl-customer-mdm-etl/releases/${var.artifacts_version}/out_api_hco.py"
          handler         = ""      // Uses only in lambda function
          timeout         = 60     // timeout. For lambda in sec and in minutes for glue
          max_capacity    = 2       // memory size for lambda and dpu for glue
          connection      = ""      // connection for glue etl jobs and vpc endpoint for lambda
          execution_class = local.helpers.class[var.environment]
          // map of args for glue etl job and environment variables for lambda
          args = {
            // special glue args
            "--enable-spark-ui"                  = "true"
            "--enable-observability-metrics"     = "true"
            "--enable-continuous-cloudwatch-log" = "true"
            "--spark-event-logs-path"            = "s3://adl-spark-event-logs-${var.environment}/adl-customer-mdm-etl/"
            "--conf"                             = "spark.eventLog.rolling.enabled=true"
            "--enable-metrics"            = "true"
            "--enable-auto-scaling"       = "true"
            "--job-language"              = "python"
            "--enable-glue-datacatalog"   = ""
            "--TempDir"                   = "s3://${local.bucket.onlyid}/trusted/__tmp/${local.name}/"
            "--python_version"            = 3
            "--job-bookmark-option"       = "job-bookmark-disable"
            "--extra-py-files"            = "s3://${var.artifact_bucket}/adl-customer-mdm-etl/releases/${var.artifacts_version}/adl_Customer_MDM_ETL-1.0.0-py3.6.egg"
          }
          // map of args to pass through SFN (Payload)
          sfn_args = {
           
            "--SOURCE_PATH"      = "${local.tfstate_dp["adl-customer-mdm-etl/adl-trusted-gbl-cf-customer-mdm"].bucket.id}"
            "--TARGET_PATH"      = "${local.bucket.onlyid}/enriched"
            "--CONFIG_PATH"      = "${var.artifact_bucket}/adl-customer-mdm-etl/releases/${var.artifacts_version}/objects.json"
            "--TARGET_DATABASE" =  "${local.tfstate_dp["adl-customer-mdm-etl/adl-enriched-gbl-cf-customer-mdm"].databases[0].name}"
           #"--TABLE_LIST.$"     = "$.input.static_parameters.table_list"
           #"--DROP_TABLES.$"     = "$.input.static_parameters.drop_tables"
           #"--EXCLUDE_TABLES.$" = "$.input.static_parameters.exclude_tables"
          }
          // map of args to pass through SFN (Parameters)
          sfn_params = {
            WorkerType      = "G.2X"
            NumberOfWorkers = 2
          }
        }
      ]
    }
  ]

}
