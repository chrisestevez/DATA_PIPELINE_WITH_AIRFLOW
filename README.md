# DATA_PIPELINE_WITH_AIRFLOW

## Intro

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Schema

<img src="./imgs/dag.png"  weight="600" height="500"/>

## Files

- **airflow** - Folder containing project
    - **dags** - Folder
        - **udac_example_dag.py** - Script containig execution steps.
    - **plugins** - Folder
       - **helpers** - Folder
           - **sql_queries.py** - Files containing insert queries.
       - **operators** - Folder
            - **data_quality.py** - Script to check db tables
            - **load_dimension.py** - Script that loads dimension tables
            - **load_fact.py** - Script that loads fact table
            - **stage_redshift.py** - Script tha moves s3 data to redshift
    - **create_tables.sql** - Code to create tables
- **imgs** - Picture for the project



## Project Execution
### Step 1
- Run below steps within [create_cluster.ipynb](https://github.com/chrisestevez/AMAZON_DATA_WAREHOUSE/blob/main/create_cluster.ipynb)
- Create AWS user with programmatic access

- Populate KEY & SECRET to **dwh.cfg** configuration file

- Install python 3.8.3

- Install libraries 
    - psycopg2
    - boto3

- Open **create_cluster.ipynb**    
    - Execute SECTION 1 within **create_cluster.ipynb**
    - Execute SECTION 2 & extract ARN & HOST save values to **dwh.cfg**
    - Execute SECTION 3 only run **create_tables.py**

### Step 2

- Install [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start/index.html) & missing dependencies
- Setup credentials within Apache Airflow via Admin -> Connections
    - aws_credentials

    <img src="./imgs/aws.png"  weight="600" height="500"/>
    
    - redshift
    
    <img src="./imgs/redshift.png"  weight="600" height="500"/>

- Load dags, helpers & operator files to Apache Airflow
- Run Dag
<img src="./imgs/run.png"  weight="600" height="500"/>



