from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from creds import GCP_PROJECT_ID 


@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"./{gcs_path}")


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id=GCP_PROJECT_ID,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(color, year, month):
    """ETL flow to load dataset into Big Query"""
    
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    print(f"rows: {len(df)}") # rows in dataset
    write_bq(df)

@flow()
def main_etl_flow(datasets: list) -> None:
    """Main ETL flow to load datasets into Big Query"""

    for ds in datasets:
        etl_gcs_to_bq(*ds)


if __name__ == "__main__":
    datasets = [
        ("yellow", 2019, 2),
        ("yellow", 2019, 3)
    ]
    main_etl_flow(datasets)