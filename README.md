# Data Pipeline Assignment

A comprehensive data pipeline that scrapes BLS data, fetches population information via API, and performs analytics using Apache Spark in Jupyter notebooks.

## Overview

This project consists of three main components:
1. **AWS S3 & Sourcing Datasets**: Automated scraping of files from HTTP sources and uploading to S3
2. **API Integration**: Fetching population data from external APIs and storing in S3
3. **Data Analytics**: Jupyter notebooks for data analysis using Apache Spark

## Project Structure

```
├── bls_sync.py              # BLS data scraping and S3 upload script
├── fetch_population_info.py # Population API data fetching script
├── notebooks/               # Jupyter notebooks for data analytics
│   └── spark_analytics.ipynb
└── README.md               # This file
```

## Scripts

### 1. bls_sync.py

Scrapes files from HTTP sources and uploads them to Amazon S3.

**Usage:**
```bash
python bls_sync.py
```

### 2. fetch_population_info.py

Calls external APIs to fetch population information and stores the data in S3.


**Usage:**
```bash
python fetch_population_info.py
```

### 3. Data Analytics Notebooks

Jupyter notebooks that perform data analytics on the collected data using Apache Spark.




## Usage

### Running the Data Pipeline

1. **Scrape BLS Data:**
```bash
python bls_sync.py
```

2. **Fetch Population Data:**
```bash
python fetch_population_info.py
```

3. **Run Analytics:**
```bash
jupyter notebook notebooks/spark_analytics.ipynb
```

https://bls-data-assgn.s3.ap-south-1.amazonaws.com/data/pr.data.0.Current
https://bls-data-assgn.s3.ap-south-1.amazonaws.com/data/pr.data.1.AllData
https://bls-data-assgn.s3.ap-south-1.amazonaws.com/data/pr.duration
https://bls-data-assgn.s3.ap-south-1.amazonaws.com/data/pr.footnote
https://bls-data-assgn.s3.ap-south-1.amazonaws.com/data/pr.measure
https://bls-data-assgn.s3.ap-south-1.amazonaws.com/data/pr.period
https://bls-data-assgn.s3.ap-south-1.amazonaws.com/data/pr.seasonal
https://bls-data-assgn.s3.ap-south-1.amazonaws.com/data/pr.sector
https://bls-data-assgn.s3.ap-south-1.amazonaws.com/data/pr.series
https://bls-data-assgn.s3.ap-south-1.amazonaws.com/data/pr.txt


https://bls-data-assgn.s3.ap-south-1.amazonaws.com/population_data/data.json
