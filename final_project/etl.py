from datetime import date
from typing import List
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.bigquery import bigquery_load_cloud_storage
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
#from google.cloud import bigquery


@task(log_prints=True, retries=2)
def extract_energy_dataset(url_dataset: str) -> pd.DataFrame:
    """Downloads a file with data with solar and wind energy generation from 
    a data provider website to the local PC and extracts energy dataset.
    
    Args:
        url_dataset: A string containing the URL of the dataset to be downloaded.

    Returns:
        energy_dataframe: A pandas DataFrame containing the energy dataset extracted from the downloaded file.

    """
    energy_dataframe = pd.DataFrame()
    
    print(f"Start extract from {url_dataset}")
    energy_dataframe = pd.read_csv(url_dataset,
                           delimiter=";",
                           encoding="utf_16_le",
                           skiprows=4,
                           na_values="-")
    return energy_dataframe

@task(log_prints=True)
def drop_unused_columns(energy_dataframe: pd.DataFrame, unused_columns: List[str]):
    """
    Drops the unused columns from the energy dataset DataFrame.

    Args:
        energy_dataframe: A pandas DataFrame containing the energy dataset to be cleaned.
        unused_columns: A list of strings containing the names of unused columns to be dropped.

    Returns:
        None

    """
    
    print(f"Start Drop Columns {unused_columns}")
    energy_dataframe.drop(columns=unused_columns, inplace=True)
    print("Drop Columns complete")

@task(log_prints=True)
def generate_timestamp(energy_dataframe: pd.DataFrame, date_time_columns: List[str]):
    """
    Generates a timestamp column from two existing columns 
    (one for date and one for time) in the provided energy dataset.

    Args:
        energy_dataframe: A pandas DataFrame containing the energy dataset.
        date_time_columns: A list of two strings containing the column names for date and time in the energy dataset.
        This list must be in the order [date_column, time_column].

    Returns:
        None

    """
    print("Start Generate Timestamp")
    energy_dataframe["timestamp"] = pd.to_datetime(energy_dataframe[date_time_columns]
                                                .agg(" ".join, axis=1), format="%d.%m.%Y %H:%M")
    print("Generate Timestamp Complete")

@task(log_prints=True)
def rename_columns(energy_dataframe: pd.DataFrame, new_columns: List[str]):
    """
    Renames the columns in the energy dataset based on the provided list of new column names.

    Args:
        energy_dataframe: A pandas DataFrame containing the energy dataset.
        new_columns: A list of strings containing names of columns, in the same order as the original columns in the dataset.

    Returns:
        None

    """
    print(f"Start Rename Columns {new_columns}")
    energy_dataframe.columns = new_columns
    print(f"Rename Columns Complete")

@task(log_prints=True)
def drop_nulls_rows(energy_dataframe: pd.DataFrame, chek_columns: List[str]):
    """
    Drops rows with null values in the specified columns of the energy dataset.

    Args: 
        energy_dataframe: A pandas DataFrame containing the energy dataset.
        check_columns: A list of strings containing names of columns to check for null values.

    Returns:
        None

    """
    
    print(f"Start Drop NULLs Columns {chek_columns}")
    for col in chek_columns:
        energy_dataframe = energy_dataframe[energy_dataframe[col].notnull()]
    print("Drop NULLs Columns Complete")

@task(log_prints=True)
def convert_type_columns(energy_dataframe: pd.DataFrame, change_columns: List[str]) -> pd.DataFrame:
    """
    Convert the datatype of specified columns in the energy dataset to float64.

    Args:
        energy_dataframe: A pandas DataFrame containing the energy dataset to be modified.
        change_columns: A list of strings containing names of columns to modify.

    Returns:
        pandas DataFrame: A modified pandas DataFrame with specified columns' 
        datatype converted to float64.

    """
    
    print(f"Start Change Type of Columns {change_columns}")
    data_types = dict(energy_dataframe.dtypes)
    for col in change_columns:
        if data_types[col] == object:
            energy_dataframe[col] = energy_dataframe[col].str.replace(',','.')
        energy_dataframe = energy_dataframe.astype({col: "float64"})
    print(f"Change Type of Columns Complete")
    return energy_dataframe

@task(log_prints=True)
def reorder_columns(energy_dataframe: pd.DataFrame, reorder_columns: List[str]) -> pd.DataFrame:
    """
    Reorders the columns in the energy dataset according to the specified list of column names.

    Args:
        energy_dataframe: A Pandas DataFrame containing the energy dataset to be modified.
        reorder_columns: A list of strings containing names of columns in the order they 
        should appear in the modified DataFrame.

    Returns:
        pd.DataFrame: A modified pandas DataFrame with the columns in the specified order.

    """
    
    print(f"Start Reorder Columns {reorder_columns}")
    energy_dataframe = energy_dataframe[reorder_columns]
    print("Reorder Columns Complete")
    return energy_dataframe

@task(log_prints=True)
def save_to_parquet(energy_dataframe: pd.DataFrame, type_energy: str, current_year: int) -> Path:
    """
    Saves a given pandas DataFrame of energy dataset to on path ./data_solar_wind 
    as a Parquet file with gzip compression.

    Args:
        energy_dataframe: A pandas DataFrame containing energy dataset to be saved.
        type_energy: A string specifying the type of energy to be saved 'solar' or 'wind.
        current_year: An integer specifying the year in which the energy data was collected.

    Returns:
        A pathlib Path object representing the file path to the saved Parquet file.

    """
    print(f"Start Save to Parquet {type_energy}_{current_year}.parquet")
    path = Path(f"data_solar_wind/{type_energy}_{current_year}.parquet")
    energy_dataframe.to_parquet(path, index=False, compression="gzip")
    print("Save to Parquet Complete")
    return path

@task(log_prints=True)
def save_to_csv(energy_dataframe: pd.DataFrame, type_energy: str, current_year: int) -> Path:
    """
    Saves a given pandas DataFrame of energy dataset to on path ./data_solar_wind 
    as a CSV file.

    Args:
        energy_dataframe: A pandas DataFrame containing energy dataset to be saved.
        type_energy: A string specifying the type of energy to be saved 'solar' or 'wind.
        current_year: An integer specifying the year in which the energy data was collected.

    Returns:
        A pathlib Path object representing the file path to the saved CSV file.

    """
    
    print(f"Start Save to CSV {type_energy}_{current_year}.csv")
    path = Path(f"data_solar_wind/{type_energy}_{current_year}.csv")
    energy_dataframe.to_csv(path, index=False)
    print("Save to CSV Complete")
    return path

@task(log_prints=True)
def delete_file(file_path: Path):
    try:
        file_path.unlink()
    except FileNotFoundError:
        print("File not found {file_path}")

@task
def write_gcs(path: Path):
    """
    Uploads a local Parquet file to a Google Cloud Storage (GCS) bucket.

    Args:
        path: pathlib.Path, a Path object representing the file path to the local Parquet file.

    Returns:
        None
        
    """
    gcs_bucket_block = GcsBucket.load("solar-wind-bucket")
    gcs_bucket_block.upload_from_path(from_path=path)

@flow(log_prints=True)
def bq_load_cs(bq_table: str):
    """
    Loads Parquet files from a Google Cloud Storage bucket to a BigQuery 
    'solar_energy'/ 'wind_energy' table.
    The old dataset in BQ is deleted and a new one created from the current version 
    of the Parquet files
    
    Args:
        bq_table (str): The name of the destination table in BigQuery.

    Returns:
        Any: The result of the load job.

     """
    
    gcp_credentials_block = GcpCredentials.load("solar-wind-cred")

    result = bigquery_load_cloud_storage(
        dataset="solar_wind",
        table=f"{bq_table}_energy",
        uri=f"gs://data_solar_wind/{bq_table}_*.parquet",
        gcp_credentials=gcp_credentials_block,
        job_config={
                "source_format": "PARQUET",
                "write_disposition": "WRITE_TRUNCATE",
            },
        location='US'
        )
    return result

@flow()
def solar_energy_flow(curent_year: int):
    """
    Flow for processing the Solar Energy Dataset for a given year.

    Args:
        curent_year (int): The year for which the data has to be processed.

    Returns:
        None. The results are loaded into a BigQuery table.

    """
    url_solar_energy = f"https://ds.50hertz.com/api/PhotovoltaicActual/DownloadFile?fileName={curent_year}.csv"
    
    solar_energy = extract_energy_dataset(url_solar_energy)
    date_time_columns = ["Datum", "von"]
    generate_timestamp(solar_energy, date_time_columns)
    
    unused_columns = ["bis", "Datum", "von"]
    drop_unused_columns(solar_energy, unused_columns)
    
    new_columns = ["solar_energy_mw", "timestamp"]
    rename_columns(solar_energy, new_columns)
    
    chek_columns = ["solar_energy_mw"]
    drop_nulls_rows(solar_energy, chek_columns)
    
    change_columns = ["solar_energy_mw"]
    solar_energy = convert_type_columns(solar_energy, change_columns)
    
    change_columns = ["timestamp", "solar_energy_mw"]
    solar_energy = reorder_columns(solar_energy, change_columns)
    
    solar_path = save_to_parquet(solar_energy, "solar", curent_year)
    save_to_csv(solar_energy, "solar", curent_year)
    
    write_gcs(solar_path)
    
    _ = bq_load_cs("solar")
    
    # delete_file(solar_path)

@flow()
def wind_energy_flow(curent_year: int):
    """
    Flow for processing the Solar Energy Dataset for a given year.

    Args:
        curent_year (int): The year for which the data has to be processed.

    Returns:
        None. The results are loaded into a BigQuery table.

    """
    
    url_wind_energy = f"https://ds.50hertz.com/api/WindPowerActual/DownloadFile?fileName={curent_year}.csv"
    
    wind_energy = extract_energy_dataset(url_wind_energy)
    
    date_time_columns = ["Datum", "Von"]
    generate_timestamp(wind_energy, date_time_columns)
    
    unused_columns = ["bis", "Unnamed: 6", "Datum", "Von"]
    drop_unused_columns(wind_energy, unused_columns)
    
    new_columns = ["wind_energy_total_mw", "wind_energy_onshore_mw", "wind_energy_offshore_mw", "timestamp"]
    rename_columns(wind_energy, new_columns)
    
    chek_columns = ["wind_energy_total_mw", "wind_energy_onshore_mw", "wind_energy_offshore_mw"]
    drop_nulls_rows(wind_energy, chek_columns)
    
    change_columns = ["wind_energy_offshore_mw", "wind_energy_onshore_mw", "wind_energy_total_mw"]
    wind_energy = convert_type_columns(wind_energy, change_columns)
    
    change_columns = ["timestamp", "wind_energy_onshore_mw", "wind_energy_offshore_mw","wind_energy_total_mw"]
    wind_energy = reorder_columns(wind_energy, change_columns)
    
    wind_path = save_to_parquet(wind_energy, "wind", curent_year)
    save_to_csv(wind_energy, "wind", curent_year)
    
    write_gcs(wind_path)
    
    _ = bq_load_cs("wind")
    
    # delete_file(wind_path)

@flow()
def main_flow(curent_year: int):
    """Main Flow"""
    solar_energy_flow(curent_year)
    wind_energy_flow(curent_year)
    

if __name__ == "__main__":
    curent_year = date.today().year
    main_flow(curent_year)