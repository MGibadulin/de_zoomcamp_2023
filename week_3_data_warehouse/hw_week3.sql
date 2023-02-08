-- SETUP

-- Create an external table using the fhv 2019 data. 
CREATE OR REPLACE EXTERNAL TABLE `evident-syntax-375104.week3.fhv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://de_zoomcamp_w2/data/fhv/fhv_tripdata_2019-*.csv.gz']
);
-- Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). 
CREATE OR REPLACE TABLE `evident-syntax-375104.week3.fhv_nonpartitioned`
AS SELECT * FROM `evident-syntax-375104.week3.fhv`;


-- Question 1
SELECT COUNT(*) FROM `evident-syntax-375104.week3.fhv`; -- 43244696


-- Question 2
-- on External Table
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `evident-syntax-375104.week3.fhv`; -- 0B
-- on Materialized Table
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `evident-syntax-375104.week3.fhv_nonpartitioned`; -- 317.94MB


-- Question 3
SELECT COUNT(*) FROM `evident-syntax-375104.week3.fhv`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL; -- 717748

-- Question 4
-- Partition by pickup_datetime Cluster on affiliated_base_number


-- Question 5
-- Create partitioned and clustered table 
CREATE OR REPLACE TABLE `evident-syntax-375104.week3.fhv_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
SELECT * FROM evident-syntax-375104.week3.fhv);

-- query on non partitioned and non clustered table
SELECT DISTINCT affiliated_base_number
FROM `evident-syntax-375104.week3.fhv_nonpartitioned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'; -- 647.87MB

-- query on partitioned and clustered table 
SELECT DISTINCT affiliated_base_number
FROM `evident-syntax-375104.week3.fhv_partitoned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'; -- 23.05MB



