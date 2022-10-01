# A Python ELT Pipeline for NASA's Near Earth Object Web Service

[![Continous-Integration-Pipeline](https://github.com/konskar/Nasa-Neo-Service-ETL-Pipeline/actions/workflows/github-actions.yml/badge.svg?branch=main&event=push)](https://github.com/konskar/Nasa-Neo-Service-ETL-Pipeline/actions/workflows/github-actions.yml)

## Description
An end-to-end Big Data Engineering solution that consumes satellite data from NASA's Near Earth Object Web Service regarding asteroids in near orbit to earth such as volume, estimated diameter, potentially hazardous indicator, velocity and lunar distance, stores them in analytical data store and expose the data through BI dashboards.

**Solution Overview**:
1. Ingest Web Service data from REST API with Python client and store them as Json file for further downstream processing.
2. With PySpark load Json file to dataframe, create 'velocity_in_miles_per_hour' calculated metric and store the produced dataframe as Parquet file.
3. Write Parquet file to MongoDB staging collection with PySpark.
4. Populate MongoDB production collection from staging collection with MongoDB Query Language (MQL) queries.
5. Visualize the preprocessed data on Metabase dashboards, an open source BI tool.

Development tools are setup on on-premises Ubuntu instance through Windows Subsystem for Linux (WSL).

The pipeline is defined and scheduled as an Airflow dag. By default dag runs daily and loads last 3 days (from day-1 to day-3) but can run it for custom date range  through Airflow arguments.

## Table of Contents

- [Architecture](#architecture)
  - [Technologies](#technologies)
  - [Concepts Practised](#concepts-practised)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Usage](#usage)
- [Backlog](#backlog)
- [Contributing](#contributing)
- [Links](#links)

## Architecture

<kbd>
  <img src="https://user-images.githubusercontent.com/12799365/193200674-a634d19a-cc83-4889-954d-6ed3cd56099b.png" />
</kbd>

#### Technologies
`Python3`, `PySpark`, `Parquet`, `Airflow`, `MongoDB`, `Metabase`, `Github Actions`, `Linux`

#### Concepts Practised

- Consuming REST API data
- Data Wrangling
- Versioning
- Configurable Pipelines
- Observability
- Logging
- Unit & Integration Testing
- CI Pipeline
- Reporting

## Project Structure

The basic project structure is as follows:
```bash
.
├── configs
│   └── elt_config.py
├── input_output_files
│   ├── http_cache.sqlite
│   ├── nasa_neo_api_landing.parquet
│   └── nasa_neo_api_response.json
├── jobs
│   ├── functions.py
│   └── nasa_neo_service_dag.py
├── tests
    ├── produced_datasets
    │   ├── nasa_neo_api_landing.parquet
    │   └── nasa_neo_api_response.json
    ├── test_datasets
    │   └── nasa_neo_api_response_27.07.2022-31.07.2022.json
    └── test_elt_job.py
├── requirements.txt
├── resources
│   ├── architecture_diagram.drawio
│   ├── metabase.db.mv.db
│   └── mongo
│       ├── mongodb_queries.txt
│       ├── nasa_neo_service_production.json
│       ├── nasa_neo_service_staging.json
│       ├── test_nasa_neo_service_production.json
│       └── test_nasa_neo_service_staging.json
├── README.md
```

The main Python module containing the ELT job (which will be sent to the Spark cluster) is `jobs/functions.py` and it's dag is `nasa_neo_service_dag.py`. Any external configuration parameters required by the job are stored in `configs/elt_config.py`. Unit & Integration tests are on file `tests/test_elt_job.py` and it can run either  manually or automatically via CI pipeline, see `.github/workflows/github-actions.yml`.

## Setup

To run this project need to:

- Setup Linux environment either with [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) or Docker image or standalone distro. 
- Setup [Python3](https://www.python.org/downloads/)
- Setup [PySpark](https://spark.apache.org/downloads.html)
- Setup [Airflow](https://betterdatascience.com/apache-airflow-install/)
- Setup [MongoDB](https://www.mongodb.com/try/download/community)
- Setup [Studio 3T](https://studio3t.com/download/)
- Setup [Metabase](https://www.metabase.com/docs/latest/installation-and-operation/installing-metabase)
- Setup [Self-hosted github actions runner](https://docs.github.com/en/actions/hosting-your-own-runners/adding-self-hosted-runners)

- Create `.env` file with your secrets/paths (used by configs/elt_config.py), see `resources/.env_sample` for reference.

- Install python dependencies (requirements.txt)
  ```bash
  pip install virtualenv
  virtualenv venv # create virtual environment to isolate dependancies
  source venv/bin/activate # activate environment 
  pip install requirements.txt
  ```

- Create mongo databases (nasa_gov, test_nasa_gov) and import json collections, see `resources/mongo` for queries and files to import.
 <img src="https://user-images.githubusercontent.com/12799365/193393185-d7e645d5-7a5f-41da-8cb5-d646437f1f07.png" width=20% height=20% />


- Start mongodb service
  ```
  sudo service mongodb start
  ```

- Start Airflow webserver & scheduler processes
  ```bash
  airflow webserver
  airflow scheduler
  ```
  
- Restore metabase db: Copy `metabase.db.mv.db` to metabase installation folder overriding existing db.
  
- Start metabase process
  ```bash
  java -jar metabase/metabase.jar
  ```

-  Start github actions runner process
    ```bash
    ./actions-runner/run.sh
    ```

## Usage

- Trigger pipeline from Airflow UI without arguments to run with default date range
  
- Trigger pipeline from Airflow UI providing Json arguments to run for custom date range e.g. `{"start_date": "2022-07-27", "end_date":"2022-07-29"}`
<img src="https://user-images.githubusercontent.com/12799365/193385770-cc5ffdc4-8a32-410d-8ed4-0f932a42a5dc.png" width=50% height=50% />
<br />
<img src="https://user-images.githubusercontent.com/12799365/193385775-a4429d8b-2ad2-4726-a645-d627760ce03b.png" width=50% height=50% />

- Access Metabase dashboard: http://localhost:3000/
<img src="https://user-images.githubusercontent.com/12799365/193413280-c30fceaa-35cd-4234-8e03-1866a0b833ea.png" width=50% height=50% />

## Backlog
- [x] Airflow alerting on success & failure
- [x] Airflow pipeline accept dynamic date arguments
- [x] Add Tests
- [x] Implement CI Pipeline
- [x] Create architecture diagram
- [ ] Add License
- [ ] Dockerize the solution so dev environment can be easily reproduced
- [ ] Improve README so someone can better understand the project and how to contribute 
- [ ] Implement Continuos Delivery so code changes are deployed to test environment

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
Please make sure to update tests as appropriate.

## Links
- NASA APIs: https://api.nasa.gov/
- NeoWs endpoint example: https://api.nasa.gov/neo/rest/v1/feed?start_date=2022-09-28&end_date=2022-09-29&api_key=DEMO_KEY
- MongoDB import data: https://www.mongodb.com/docs/compass/current/import-export/
