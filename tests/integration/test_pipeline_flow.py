import os
import boto3
import time

def test_clickstream_data_written_to_s3():
    """
    Integration test:
    Verifies that clickstream data is written to S3 as Parquet files.
    Assumes docker-compose services are running.
    """

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=boto3.session.Config(s3={"addressing_style": "path"})
    )

    bucket = os.getenv("S3_BUCKET", "clickstream-bucket")

    # Give the pipeline some time to write data
    time.sleep(10)

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix="clickstream/"
    )

    assert "Contents" in response, "No data found in S3 bucket"
    assert len(response["Contents"]) > 0, "No Parquet files written to S3"
