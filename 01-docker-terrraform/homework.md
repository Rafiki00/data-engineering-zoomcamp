
# Week 1: Docker & Terraform

## Question 1. Understanding docker first run

**Question:**

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- 24.3.1
- 24.2.1
- 23.3.1
- 23.2.1

**Answer:**
- `24.3.1`

To find the pip version in the image, I ran a Python container in interactive mode and checked the pip version:
```bash
docker run -it python:3.12.8 bash
pip --version
```


## Question 2. Understanding Docker networking and docker-compose

**Question:**

Given the following `docker-compose.yaml`, what is the hostname and port that pgadmin should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

**Answer:**

- `db:5432`

When working with container-to-container communication within the same Docker Compose network, we use the service name as the hostname. While the port mapping `5433:5432` means port 5433 on the host machine maps to 5432 in the container, pgAdmin needs to use the internal Docker network address and port, which is `db:5432`.

## Question 3. Trip Segmentation Count

**Question:**

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles

**Answer:**

- `104,802; 198,924; 109,603; 27,678; 35,189`

Ran the following query in pgadmin:

```sql
SELECT TripSegment, COUNT(*)
FROM (
SELECT CASE WHEN trip_distance <= 1 THEN 'Up to 1 mile'
			WHEN trip_distance > 1 AND trip_distance <= 3 THEN 'In between 1 (exclusive) and 3 miles (inclusive)'
			WHEN trip_distance > 3 AND trip_distance <= 7 THEN 'In between 3 (exclusive) and 7 miles (inclusive)'
			WHEN trip_distance > 7 AND trip_distance <= 10 THEN 'In between 7 (exclusive) and 10 miles (inclusive)'		
			WHEN trip_distance > 10 THEN 'Over 10 miles' END AS TripSegment
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01'
	  AND lpep_dropoff_datetime < '2019-11-01')
GROUP BY TripSegment;
```


## Question 4. Longest trip for each day

**Question:**

Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.

- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31

**Answer:**

- `2019-10-11`


Ran the following query in pgadmin:

```sql
SELECT CAST(lpep_pickup_datetime AS DATE) AS TripDate, SUM(trip_distance) AS TotalDistance
FROM green_taxi_trips
WHERE (CAST(lpep_pickup_datetime AS DATE) = '2019-10-11' AND CAST(lpep_dropoff_datetime AS DATE) = '2019-10-11') OR
	  (CAST(lpep_pickup_datetime AS DATE) = '2019-10-24' AND CAST(lpep_dropoff_datetime AS DATE) = '2019-10-24') OR
	  (CAST(lpep_pickup_datetime AS DATE) = '2019-10-26' AND CAST(lpep_dropoff_datetime AS DATE) = '2019-10-26') OR
	  (CAST(lpep_pickup_datetime AS DATE) = '2019-10-31' AND CAST(lpep_dropoff_datetime AS DATE) = '2019-10-31')
GROUP BY CAST(lpep_pickup_datetime AS DATE)
ORDER BY TotalDistance DESC;
```

## Question 5. Three biggest pickup zones

**Question:**

Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?

Consider only lpep_pickup_datetime when filtering by date.

- East Harlem North, East Harlem South, Morningside Heights
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park

**Answer:**

- `East Harlem North, East Harlem South, Morningside Heights`

Ran the following query in pgadmin:

```sql
SELECT tz."Zone", SUM(tr.total_amount) AS SumAmount
FROM green_taxi_trips AS tr
JOIN taxi_zones AS tz
	ON tr."PULocationID" = tz."LocationID"
	AND CAST(tr.lpep_pickup_datetime AS DATE) = '2019-10-18'
GROUP BY tz."Zone"
HAVING SUM(tr.total_amount) > 13000
ORDER BY SumAmount DESC;
```

## Question 6. Largest tip

**Question:**

For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?

Note: it's tip , not trip

We need the name of the zone, not the ID.

- Yorkville West
- JFK Airport
- East Harlem North
- East Harlem South

**Answer:**

- `JFK Airport`

Ran the following query in pgadmin:

```sql
SELECT tz."Zone", MAX(tr.tip_amount) AS MaxTipAmount
FROM green_taxi_trips AS tr
JOIN taxi_zones AS tz
    ON tr."DOLocationID" = tz."LocationID"
JOIN taxi_zones AS tz2
    ON tr."PULocationID" = tz2."LocationID"
	AND tz2."Zone" = 'East Harlem North'
    AND date_part('month', CAST(lpep_pickup_datetime AS DATE)) = 10
    AND date_part('year', CAST(lpep_pickup_datetime AS DATE)) = 2019
GROUP BY tz."Zone"
ORDER BY MaxTipAmount DESC;
```


## Question 7. Terraform Workflow

**Question:**

Which of the following sequences, respectively, describes the workflow for:

1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform

**Answer:**

- `terraform init, terraform apply -auto-approve, terraform destroy`