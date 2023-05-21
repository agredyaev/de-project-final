
## Data Loading Pipeline
----
This project provides a data loading pipeline that allows you to load data from a PostgreSQL database to a Vertica database. It includes classes for loading data into the staging layer and populating the common data marts layer. The pipeline utilizes SQL files for executing the necessary queries.

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

- `src/`: Contains the source code and modules for the project.
- `src/sql/`: Includes the SQL templates for data extraction and transformation.
- `src/py/`: Contains utility functions and common modules used in the project.
- `dags/`: Includes the DAG (Directed Acyclic Graph) files that define the data pipelines.

## Archetecture
---
![archetecture.png](/src/img/archetecture.png)

## Output mart
---- 
NEYBYANDEXRU__DWH.global_metrics

## Dashboard
-----
![dasboard.png](/src/img/dashboard.png)