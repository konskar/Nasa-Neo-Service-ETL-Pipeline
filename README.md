# A Python ELT Pipeline for NASA's Near Earth Object Web Service
[![Continous-Integration-Pipeline](https://github.com/konskar/Nasa-Neo-Service-ETL-DAG/actions/workflows/github-actions.yml/badge.svg?branch=main&event=push)](https://github.com/konskar/Nasa-Neo-Service-ETL-DAG/actions/workflows/github-actions.yml)

An end-to-end Big Data Engineering solution that consumes satellite data from NASA's Near Earth Object Web Service regarding asteroids in near orbit to earth such as volume, estimated diameter, potentially hazardous indicator, velocity and lunar distance, stores them in analytical data store and expose the data through BI dashboards.

**Solution Overview**:
1. Ingest Web Service data from REST API with Python client and store them as Json file for further downstream processing.
2. With PySpark load Json file to dataframe, create 'velocity_in_miles_per_hour' calculated metric and store the produced dataframe as Parquet file.
3. Write Parquet file to MongoDB staging collection with PySpark.
4. Populate MongoDB production collection from staging collection with MongoDB Query Language (MQL) queries.
5. Visualize the preprocessed data on Metabase dashboards, an open source BI tool.

Development tools are setup on on-premises Ubuntu instance through Windows Subsystem for Linux (WSL).

The pipeline is defined and scheduled as an Airflow dag. By default dag runs daily and loads last 3 days (from day-1 to day-3) but can run it for custom date range  through Airflow arguments.

## How to use it

If `MB_DB_SAVE_TO_SQL_FILE` is set and the container will get stop/restart signal then the database will be saved 
to SQL-file after Metabase has been stopped.

, e.g `{"start_date": "2022-07-27", "end_date":"2022-07-29"}`

## Architecture

<kbd>
  <img src="https://user-images.githubusercontent.com/12799365/193200674-a634d19a-cc83-4889-954d-6ed3cd56099b.png" />
</kbd>

### Technologies Used
- `Python3`
- `PySpark`
- `Parquet`
- `MongoDB`
- `Airflow`
- `Metabase`
- `Github Actions` 
- `Linux`

### Concepts Practised

- Versioning
- Configurable Pipelines
- Observability
- Logging
- Unit & Integration Testing
- CI Pipeline

### Airflow DAG
<kbd>
  <img src="https://user-images.githubusercontent.com/12799365/193214744-8f7aaba1-21b3-43e8-bb39-b8ad442d20d5.png" />
</kbd>

<br></br>

### Near Earth Asteroids Monitoring Dashboard
<kbd>
  <img src="https://user-images.githubusercontent.com/12799365/193201434-ac7c925f-7cd7-4c02-91e7-6c666f245a0a.png" />
</kbd>

<br></br>

## Links
- NASA APIs: https://api.nasa.gov/
- NeoWs endpoint example: https://api.nasa.gov/neo/rest/v1/feed?start_date=2022-09-28&end_date=2022-09-29&api_key=DEMO_KEY
