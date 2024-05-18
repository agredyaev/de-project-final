## Data Loading Pipeline
----
This project provides a data loading pipeline that allows you to load data from a PostgreSQL database to a Vertica database. It includes classes for loading data into the staging layer and populating the common data marts layer. The pipeline utilizes SQL files for executing the necessary queries.

The project is designed to run in the cloud, where the entire environment is set up and configured.

## Technologies

![Python](https://img.shields.io/badge/Python-3.9-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.0.2-blue)
![Vertica](https://img.shields.io/badge/Vertica-10.0.1-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue)
![Metabase](https://img.shields.io/badge/Metabase-0.39.4-blue)
![MIT License](https://img.shields.io/badge/License-MIT-green)(https://opensource.org/licenses/MIT)

## Project Overview
----
The data engineering project consists of the following components:

1. Staging Layer: The staging layer is responsible for extracting data from the source systems and performing initial transformations. It includes tasks for loading transaction data and currency data from the source PostgreSQL database into the staging layer.

2. Common Data Mart (CDM): The common data mart is a centralized repository for storing pre-aggregated and transformed data. It includes tasks for loading global metrics data into the CDM layer.

3. Data Pipeline: The data pipeline is implemented using Apache Airflow, an open-source platform for orchestrating workflows. The DAGs (Directed Acyclic Graphs) in Airflow define the sequence of tasks and dependencies for data extraction, transformation, and loading.

4. SQL Templates: SQL templates are used for generating dynamic SQL queries. These templates can be customized to include specific date ranges, filters, and transformations as per the project requirements.

5. Data Storage: The data is stored in the Vertica database, which provides a scalable and high-performance analytics platform for data analysis and reporting.

## Project Structure

The project structure is organized as follows:

- `img/`: Images for documentation and architecture.
- `src/`: Contains the source code and modules for the project.
  - `src/sql/`: Includes the SQL templates for data extraction and transformation.
  - `src/utils/`: Contains utility functions and common modules used in the project.
  - `src/dags/`: Includes the DAG (Directed Acyclic Graph) files that define the data pipelines.


## Archetecture
---
![archetecture.png](/img/archetecture.png)

## Output mart
---- 
NEYBYANDEXRU__DWH.global_metrics

## Dashboard
-----
![dasboard.png](/img/dashboard.png)