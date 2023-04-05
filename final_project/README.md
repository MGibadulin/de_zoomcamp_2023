# Data Engineering Zoomcamp Project
This folder contains my project for the Data Engineering Zoomcamp.
# Project Description
This is a simple project that takes historical data on solar and wind energy production in Germany in the "50 hertz zone" from 2011 to the present.  And transform them for visualisation and use by data scientists to predict how much energy will be produced in the next period of time.
# Data Pipeline
![Data Pipeline](https://github.com/MGibadulin/de_zoomcamp_2023/blob/main/final_project/DataPipeline.png)
## Source
Source files are public and has CSV format. Files updated daily, with a delay of one day. 
- [Dataset Wind Energy](https://ds.50hertz.com/api/WindPowerActual/DownloadFile?fileName=2023.csv)
- [Dataset Solar Energy](https://ds.50hertz.com/api/PhotovoltaicActual/DownloadFile?fileName=2023.csv)
## Ingestion
The orchestration framework Prefect with Python script ingest data once a day and saves the files localy.
## Pre-processing
Prefect with Python processing files localy and load the parquet files to Cloud Storage:
- Drop Unused Columns
- Rename Columns
- Check NULL
- Fillout NULL
- Change Types
- Convert to Parquet
- Load to GCS
## Data Lake
Prefect with Python creates external tables from data from GCS in DWH BigQuery.
## Data Warehouse
Data Build Tool transformes, summarises, checks data consistency and calculates metrics for further visualization. The result is the table that is partitioned by month.
![Lineage](https://github.com/MGibadulin/de_zoomcamp_2023/blob/main/final_project/lineage.jpg)
## Analytics
Looker Studio get data direct from BigQuery and presents on the [Dashboard](.). 


# Used Technologies and Tools
    Cloud: GCP
    Data Lake: Google Cloud Storage
    Data Wareshouse: BigQuery
    Infrastructure as code (IaC): Terraform
    Workflow orchestration: Prefect
    Batch processing: DBT

# Reproduce the project

## GCP

1. Create an account on GCP
2. Setup a new project and write down the Project ID
3. Setup a service account for this project and download the JSON authentication key files and write down the path. You will need to create a Service Account with the following roles:
    - Storage Admin
    - Storage Object Admin
    - BigQuery Admin
    - Viewer
4. Download the GCP SDK for local setup. Follow the instructions to install and connect to your account and project.
5. Set the environment variable to point to the auth keys:

    ```bash
    $ export GOOGLE_APPLICATION_CREDENTIALS="<path/to/authkeys>.json"
    ```
6. Refresh the token and verify the authentication with the GCP SDK:
    ```bash
    $ gcloud auth application-default login
    ```
7. Enable APIs for the project:
    - https://console.cloud.google.com/apis/library/iam.googleapis.com
    - https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com

## Terraform

8. Install Terraform
    - https://developer.hashicorp.com/terraform/downloads?product_intent=terraform
9. Initialize the directory. When you create a new configuration — or check out an existing configuration from version control — you need to initialize the directory with terraform init. This step downloads the providers defined in the configuration.
    ```bash
    $ cd terraform
    $ terraform init
    ```
10. Create infrastructure. Apply the configuration now with the terraform apply command.
    ```bash
    $ terraform apply
    ```
## Prefect
11. Create venv
    ```bash
    $ python3 -m venv venv
    ```
12. Activate venv
    ```bash
    $ source venv/bin/activate
    ```
13. Setup Prefect
    ```bash
    $(venv) pip install -U prefect
    ```
14. Start the Prefect Orion API server locally
    ```bash
    $(venv) prefect server start
    ```
15. Open your web-browser and navigate to http://127.0.0.1:4200. Create a GCP blocks in Prefect UI
16. Create a GCP Credentials block in the Prefect UI. Paste your service account information from your JSON file into the Service Account Info block's field.
17. Create and apply your deployments.
```bash
    $ prefect deployment build ./prefect:main_flow -n main_flow -q main_deployment  --apply --cron "0 0 * * *"
```
18. Use the Prefect CLI to create a flow run for this deployment and run it with an agent that pulls work from the 'main_deployment ' work pool:
```bash
    $ prefect deployment run ‘main_flow/‘main_flow’
    $ prefect agent start -q ‘main_work’
```
## DBT Cloud
19. Please refer this [tutorial](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/1bfbc36c90d5e15c6aa18efd56420d0c4f7eeb4e/week_4_analytics_engineering/dbt_cloud_setup.md) to setup the DBT Cloud. It requires the json file from step 3.
20. In DBT Cloud, configure the deployment and scheduler to run the transformation once a day.

## Looker Studio
21. Please refer this [article](https://support.google.com/looker-studio/answer/6370296#zippy=%2Cin-this-article) to connect Looker Studio and BigQuery.
