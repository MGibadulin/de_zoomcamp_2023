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
docker run -it \
  --network=postgres \
  data_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name_1=taxi_trips \
```
