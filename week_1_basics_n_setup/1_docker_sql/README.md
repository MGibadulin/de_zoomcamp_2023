## Running Postgres and pgAdmin together, Docker-compose
Run it:
```
docker-compose up
```

## Data ingestion
Building image
```
docker build -t data_ingest:v001 .
```

Run the script with Docker

```
URL = <link to file.csv>
docker run -it \
  --network=postgres \
  data_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg_db \
    --port=5432 \
    --db=ny_taxi \
    --table_name_1=taxi_trips \
    --url=${URL}
```
