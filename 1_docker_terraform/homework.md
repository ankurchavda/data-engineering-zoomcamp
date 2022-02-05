Question 3 -
```sql
SELECT  COUNT(*)
FROM yellow_taxi_trips
WHERE cast(tpep_pickup_datetime AS date) = date '2021-01-15';
```
Question 4 -
```sql
SELECT  cast(tpep_pickup_datetime AS date) AS date
       ,MAX(tip_amount)                    AS tip
FROM yellow_taxi_trips
WHERE date_part('month', cast(tpep_pickup_datetime AS date)) = 1
AND date_part('year', cast(tpep_pickup_datetime AS date)) = 2021
GROUP BY  date
ORDER BY tip desc
```

Question 5 - 
```sql
SELECT  "Zone"
       ,COUNT(*) AS cnt
FROM yellow_taxi_trips y
JOIN zones zdo
ON y."DOLocationID" = zdo."LocationID"
WHERE "PULocationID" = 43
AND tpep_pickup_datetime::date = '2021-01-14'::date
GROUP BY  "Zone"
ORDER BY cnt desc
LIMIT 1
```
Question 6 - 
```sql
SELECT  concat(coalesce(zpu."Zone",'Unknown'),' / ',coalesce(zdo."Zone",'Unknown'))
       ,AVG(total_amount) AS total_amount
FROM yellow_taxi_trips y
JOIN zones zpu
ON y."PULocationID" = zpu."LocationID"
JOIN zones zdo
ON y."DOLocationID" = zdo."LocationID"
GROUP BY  zpu."Zone"
         ,zdo."Zone"
ORDER BY total_amount desc
LIMIT 1
```