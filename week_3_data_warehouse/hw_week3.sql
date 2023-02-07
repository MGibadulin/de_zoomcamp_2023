-- SETUP

-- Create an external table using the fhv 2019 data. 
CREATE OR REPLACE EXTERNAL TABLE `week3.fhv`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_w2/data/fhv/fhv_tripdata_2019-*.parquet']
);
-- Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). 
CREATE OR REPLACE TABLE `week3.fhv_bq`
AS SELECT * FROM `week3.fhv`;


-- Question 1
SELECT COUNT(*) FROM `week3.fhv`; -- 43244696

-- 