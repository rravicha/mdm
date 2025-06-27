from src.ingestion.out_api_hco_address import parse_s3_uri
import pytest

def test_parse_s3_uri():
    # Test cases with expected inputs and outputs
    test_cases = [
        (
            "s3://${var.artifact_bucket}/adl-customer-mdm-etl/releases/${var.artifacts_version}/adl_Customer_MDM_ETL-1.0.0-py3.6.egg",
            ("${var.artifact_bucket}", "adl-customer-mdm-etl/releases/${var.artifacts_version}/adl_Customer_MDM_ETL-1.0.0-py3.6.egg")
        ),
        (
            "s3://my-bucket/path/to/file.txt",
            ("my-bucket", "path/to/file.txt")
        ),
        (
            "s3://my-bucket/",
            ("my-bucket", "")
        ),
        (
            "s3://my-bucket",
            ("my-bucket", "")
        )
    ]

    for input_uri, expected_output in test_cases:
        bucket, prefix = parse_s3_uri(input_uri)
        assert (bucket, prefix) == expected_output, f"Failed for input: {input_uri}"

