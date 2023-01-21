-- Question 3. Count records
-- How many taxi trips were totally made on January 15?
SELECT COUNT(1)
FROM public.trip
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-01-15' AND
	  CAST(lpep_dropoff_datetime AS DATE) = '2019-01-15';

-- Question 4. Largest trip for each day
-- Which was the day with the largest trip distance. Use the pick up time for your calculations.
SELECT CAST(lpep_pickup_datetime AS DATE) as date_pu
FROM public.trip
ORDER BY trip_distance DESC
LIMIT 1;

-- Question 5. The number of passengers
-- In 2019-01-01 how many trips had 2 and 3 passengers?
WITH pc AS(
	SELECT passenger_count
	FROM public.trip
	WHERE CAST(lpep_pickup_datetime AS DATE)  = '2019-01-01')
SELECT SUM(CASE WHEN passenger_count = 2 THEN 1 ELSE 0 END) AS trips_2pass,
	   SUM(CASE WHEN passenger_count = 3 THEN 1 ELSE 0 END) AS trips_3pass
FROM pc;

-- Question 6. Largest tip
-- For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? 
-- We want the name of the zone, not the id.
-- Note: it's not a typo, it's tip , not trip
SELECT zpu.zone, zdo.zone, tip_amount
FROM public.trip AS t
LEFT JOIN public.zones AS zpu ON pulocationid = zpu.locationid
LEFT JOIN public.zones AS zdo ON dolocationid = zdo.locationid
WHERE zpu.zone LIKE '%Astoria%'
ORDER BY 3 DESC
LIMIT 1;