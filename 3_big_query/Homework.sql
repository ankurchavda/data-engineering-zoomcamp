-- Create External table for FHV
CREATE OR REPLACE EXTERNAL TABLE `dtc-ny-taxi.trips_data_all.external_fhv`
OPTIONS (
    format='parquet',
    uris=['gs://dtc_data_lake_dtc-ny-taxi/parquet/fhv_tripdata/fhv_tripdata_2019-*.parquet']
);

-- Create table in BQ
CREATE OR REPLACE TABLE `dtc-ny-taxi.trips_data_all.fhv_non_partitioned`
AS 
SELECT * FROM `dtc-ny-taxi.trips_data_all.external_fhv`;

-- Record Count
SELECT COUNT(*) FROM `dtc-ny-taxi.trips_data_all.fhv_non_partitioned`;
-- 42084899

-- dispatching_base_num distinct count
SELECT 
    COUNT(DISTINCT dispatching_base_num)
FROM `dtc-ny-taxi.trips_data_all.fhv_non_partitioned`;
-- 792

-- Create partition on dropoff_datetime and cluster on dispatching_base_num
CREATE OR REPLACE TABLE `dtc-ny-taxi.trips_data_all.fhv_partitioned`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num 
    AS
SELECT * FROM `dtc-ny-taxi.trips_data_all.fhv_non_partitioned`;

-- Count trips and estimated vs actual data size
SELECT COUNT(*) FROM 
`dtc-ny-taxi.trips_data_all.fhv_partitioned`
WHERE DATE(dropoff_datetime) BETWEEN DATE '2019-01-01' AND DATE '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');