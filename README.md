# <u>Data Engineer nanodegree / Udacity project : Automate a data pipeline with Airflow</u>
## Table of Contents
1. [Project info](#project-info)
2. [Repository files info](#repository-files-info)
3. [Environment and application versions](#versions)
4. [Prerequisite to scripts run](#pre-requisite)
5. [Database modelling](#database-modelling)
6. [Airflow Dag and operators settings](#airflow-dag-and-operators-settings)
***

<a id='project-info'></a>
### Project info

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is **Apache Airflow**.

The purpose of this project is to create a data pipeline that is dynamic and built from reusable tasks, can be monitored, and allows easy backfills. Data quality also plays a big part when analyses are executed on top the data warehouse. Consequently, the pipeline also runs tests against the datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

>Log data: `s3://udacity-dend/log_data`

>Song data: `s3://udacity-dend/song_data`


The steps of the Airflow dag are as follows:

![ERD](https://github.com/Datapyaddict/udacity-project-automate-data-pipeline-with-airflow/blob/main/airflow_dag_steps.png?raw=true)

- The JSON files are loaded into staging tables (`staging_events`, `staging_songs`) after a truncate,
- Then from the staging tables, we fill the fact table  `songplays` via an append (though, we can use a switch `truncate` that allows to truncate the table beforehand).
- The dimensional tables are then populated after truncation (via the switch `truncate`).
- Then we run some data quality controls :
> -  For each table of the dimensional model, I expect at least one row.
> 
> - There must be at least one row that matches the fact table against all the dimension tables.

***
<a id='versions'></a>
### Environment and application versions

I have installed Airflow 2.2.3 locally in a Unix environment, via Ubuntu.
The python version installed locally is 3.7.6.


***
<a id='repository-files-info'></a>
### Repository files info

The files can be  found in 2 folders:
- `/airflow` which contains the dag and operators
- `/initialization` which contains the scripts used to create the Redshift cluster and tables.

![ERD](https://github.com/Datapyaddict/udacity-project-automate-data-pipeline-with-airflow/blob/main/repository_structure.png?raw=true)



***
<a id='pre-requisite'></a>
### Prerequisite to run the Airflow Dag

Prerequisites before running the Airflow pipeline are as follows :

- (1) Get AWS credentials
- (2) Create an IAM User in AWS with programmatic access and the following policies:
> AdministratorAccess 
>
> AmazonRedshiftFullAccess
> 
> AmazonS3FullAccess.
- (3) Create a Redshift cluster in the us-west-2 region. This is important as the s3-buckets are in this region. Define the credentials to the postgres db.
- (4) Add the AWS credentials into Airflow/Admin/Connections config panel. Connection type is : `Amazon Web Service`.
- (5) Add the Redshift Postgres database credentials into Airflow/Admin/Connections config panel. Connection type has to be : `Postgres`.

**The steps (2) and (3) can be done programmatically. This is what I did and is described below**.

Once you have the AWS credentials, store them in `dwh.cfg` configuration file under the tags `key` and `secret`.

Fill the other settings in the config file as per your liking.
The settings will be used to create the cluster and the database.
The below values for the cluster settings are suggestions:

>[CLUSTER]
> 
> dwh_cluster_type = multi-node
> 
> dwh_num_nodes = 4
>
> dwh_node_type = dc2.large
>
> dwh_cluster_identifier = dwhCluster
>
> dwh_region = us-west-2
> 
> dwh_iam_role_name = dwhRole
>
> dwh_db = dwh
>
> dwh_db_user = dwhuser
>
> dwh_db_password = Password
>
> dwh_port = 5439

> [S3_BUCKET]
>
> log_data = 's3://udacity-dend/log-data'
>
> log_jsonpath = 's3://udacity-dend/log_json_path.json'
> 
> song_data = 's3://udacity-dend/song-data'



Then, open a terminal and run the script `initialization.py` to create the cluster and the database. 
>python initialization.py

The script will also create an incoming TCP rule to access the cluster. It then updates the configuration file with the following information :
`dwh_endpoint` and `dwh_role_arn` that will be used in any connection to the database. 

**The log will show the `dwh_endpoint` that you need to update the Redshift connection in Airflow/Admin/Connections config panel**. 

Then, run the script `create_tables.py` to create the staging, and dimensional and fact tables in th e postgres db.
>python create_tables.py

Afterwards, you can run the Airflow dag.

If you don't need anymore the redshift cluster, you can delete it with the script `aws_objects_delete.py`:
> python aws_objects_delete.py


***
<a id='database-modelling'></a>
## data model ERD


![ERD](https://github.com/Datapyaddict/udacity-project-automate-data-pipeline-with-airflow/blob/main/erd.png?raw=true)


The database model consists of :
* 2 staging tables : `staging_events` and `staging_songs`.
* 1 fact table : `songplay`,
* 4 dimension tables : `users`, `songs`, `artists` , `time`. 

The dimensional data model is a simple star schema.
The tables were created with constraints.




***
<a id='airflow-dag-and-operators-settings'></a>
## Airflow Dag and operators settings

The default parameters of the dag are as follows:
- `start_date`: 01-11-2018 as the json log files are timestamped in November 2018.
- `depends_on_past`: False,
- `retries` : 3,
- `retry_delay` : 5 minutes,
- `catchup` : False,
- `email_on_retry` : False.

The dag runs every hour.




