from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    """
    THis operator runs some data quality checks on dimensional and fact tables. It takes as parameters:
    - a list of tables which are expected to contain data.
    - a list of queries which are expected to provide a count of rows >1.    
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list=[],
                 check_analytics_list = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_list = table_list
        self.check_analytics_list = check_analytics_list
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator starting')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if len(self.table_list)>0:
            for table in self.table_list:
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {table} returned no results")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
         
        if len(self.check_analytics_list)>0:
            for query in self.check_analytics_list:
                records = redshift_hook.get_records(self.check_analytics_list[query])
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {query} returned no results")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {query} contained 0 rows")
                logging.info(f"Data quality on query {query} check passed with {records[0][0]} records")            