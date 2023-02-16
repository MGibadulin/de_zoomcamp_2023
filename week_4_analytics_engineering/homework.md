# Question 1
'''
SELECT count(1) FROM `evident-syntax-375104.dbt_week4.fact_trips`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2020-12-31';
'''
Answer 61635151

# Question 2
Answer 89.9/10.1

# Question 3
'''
SELECT COUNT(1) FROM `evident-syntax-375104.dbt_week4.stg_fhv_tripdata`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31';
'''
Answer 42084899

# Question 4
'''
SELECT count(*) FROM `evident-syntax-375104.dbt_week4.fact_fhv_trips`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31';
'''
Answer 22676253

# Question 5
Answer January
