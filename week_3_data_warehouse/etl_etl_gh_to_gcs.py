from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket



@task()
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read FHV data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url, low_memory=False)
    return df


@task()
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean data"""

    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    
    path = Path(f"data/fhv/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(dataset_url: str, dataset_file: str) -> None:
    """The main ETL function"""
    

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    path = Path(f"data/fhv/{dataset_file}.parquet")
    write_gcs(path)

if __name__ == "__main__":
    
    DATASET_YEAR = 2019
    MOTHS_IN_YEAR = 12
    for month in range(1, MOTHS_IN_YEAR + 1):
        dataset_file = f"fhv_tripdata_{DATASET_YEAR}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

        etl_web_to_gcs(dataset_url, dataset_file)