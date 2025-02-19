
# Week 2: WOrkflow Orchestration

## Question 1

**Question:**

Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?

- 128.3 MB
- 134.5 MB
- 364.7 MB
- 692.6 MB

**Answer:**
- `128.3 MB`

As shown in the GCP bucket onto which the file was loaded:
![alt text](image.png)


## Question 2

**Question:**

What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?

- {{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
- green_tripdata_2020-04.csv
- green_tripdata_04_2020.csv
- green_tripdata_2020.csv

**Answer:**
- `green_tripdata_2020-04.csv`

In the Kestra execution:
![alt text](image-1.png)


## Question 3

**Question:**

How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

- 13,537.299
- 24,648,499
- 18,324,219
- 29,430,127

**Answer:**
- `24,648,499`

Queried in BigQuery: 
```
SELECT COUNT(*)
FROM `my-project.my-dataset.yellow_tripdata`
WHERE filename LIKE 'yellow_tripdata_2020%'
```

## Question 4

**Question:**

How many rows are there for the Green Taxi data for all CSV files in the year 2020?

- 5,327,301
- 936,199
- 1,734,051
- 1,342,034

**Answer:**
- `1,734,051`

Queried in BigQuery: 
```
SELECT COUNT(*)
FROM `my-project.my-dataset.green_tripdata`
WHERE filename LIKE 'green_tripdata_2020%'
```

## Question 5

**Question:**

How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

- 1,428,092
- 706,911
- 1,925,152
- 2,561,031

**Answer:**
- `1,925,152`

Queried in BigQuery: 
```
SELECT COUNT(*)
FROM `my-project.my-dataset.yellow_tripdata`
WHERE filename = 'yellow_tripdata_2021-03.csv';
```


## Question 6

**Question:**

How would you configure the timezone to New York in a Schedule trigger?

- Add a timezone property set to EST in the Schedule trigger configuration
- Add a timezone property set to America/New_York in the Schedule trigger configuration
- Add a timezone property set to UTC-5 in the Schedule trigger configuration
- Add a location property set to New_York in the Schedule trigger configuration

**Answer:**
- `Add a timezone property set to America/New_York in the Schedule trigger configuration`

Read in Kestra's documentation. NYC is EST:

![alt text](image-2.png)

