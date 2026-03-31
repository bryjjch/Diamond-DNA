import boto3
import moto

from src.pipeline.listing import list_processed_statcast_player_year_keys


@moto.mock_aws
def test_list_processed_statcast_player_year_keys_filters_years():
    bucket = "test-bucket"
    conn = boto3.client("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    keys = [
        "processed/statcast/batter/batter_id=1/year=2021/statcast_pitches.parquet",
        "processed/statcast/batter/batter_id=2/year=2023/statcast_pitches.parquet",
        "processed/statcast/batter/batter_id=3/year=2025/statcast_pitches.parquet",
    ]
    for k in keys:
        conn.put_object(Bucket=bucket, Key=k, Body=b"not-real-parquet")

    out = list_processed_statcast_player_year_keys(
        bucket=bucket,
        processed_prefix="processed/statcast",
        role="batter",
        start_year=2022,
        end_year=2024,
    )
    assert len(out) == 1
    assert out[0][0] == 2
    assert out[0][1] == 2023
    assert out[0][2].endswith("batter_id=2/year=2023/statcast_pitches.parquet")
