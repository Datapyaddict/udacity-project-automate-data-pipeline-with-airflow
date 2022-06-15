from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers.sql_queries import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 12, 1),
    "depends_on_past": False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry' : False,
}

dag = DAG('udac_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',       
#           schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/",
    region= "us-west-2",
    json_path= "s3://udacity-dend/log_json_path.json",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region= "us-west-2",
)




load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    destination_table="public.songplays",
    truncate=False,    
    insert_statement = SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.users",
    truncate=True,
    insert_statement = SqlQueries.user_table_insert,
    dag=dag
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.songs",
    truncate=True,
    insert_statement = SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.artists",
    truncate=True,
    insert_statement = SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.time",
    truncate=True,
    insert_statement = SqlQueries.time_table_insert,
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table_list = SqlQueries.table_list,
    check_analytics_list = SqlQueries.check_analytics_list,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]\
>> load_songplays_table\
>> [load_user_dimension_table,\
                         load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]\
>> run_quality_checks\
>> end_operator
