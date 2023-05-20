
## Data Loading Pipeline
----
This project provides a data loading pipeline that allows you to load data from a PostgreSQL database to a Vertica database. It includes classes for loading data into the staging layer and populating the common data marts layer. The pipeline utilizes SQL files for executing the necessary queries.

## Archetecture
---
![archetecture.png](/src/img/archetecture.png)

## Usage
---
1. Configure the database connections by updating the connection information in the `config.py` file.

2. Use the `LoadStaging` class to load data from the source PostgreSQL database to the Vertica staging layer. Provide the PostgreSQL and Vertica connection details when instantiating the class.

```python
from data_loading_pipeline import LoadStaging

# Instantiate the LoadStaging class with the connection information
load_staging = LoadStaging(postgres_conn_info, vertica_conn_info)
load_staging.load_data()
```
3. Use the `LoadCDM` class to populate the common data marts layer from the staging layer. Provide the Vertica connection details when instantiating the class.


```python
from data_loading_pipeline import LoadCDM

# Instantiate the LoadCDM class with the connection information
load_cdm = LoadCDM(vertica_conn_info)
load_cdm.load_data()
```

4. Customize the SQL queries by modifying the SQL files (`staging_query.sql` and `cdm_query.sql`) according to your specific requirements.

5. Execute the DAG file (`data_loading_dag.py`) to schedule and run the data loading pipeline.