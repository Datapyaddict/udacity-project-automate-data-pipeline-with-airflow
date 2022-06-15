from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This operator takes as parameter a table, an insert statement and a flag called truncate.
    It truncates it and fills it with the result of the sql statement. 
    If the truncate flag is True, the table is first truncated before data feed.
    """
    ui_color = '#80BD9E'
    
    truncate_sql = "truncate table {destination_table}"
    insert_sql = "insert into {destination_table} {insert_statement}"
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 truncate=False,
                 insert_statement = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.truncate = truncate
        self.insert_statement = insert_statement

    def execute(self, context):
        self.log.info('LoadDimensionOperator starting')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        rendered_truncate_sql = LoadDimensionOperator.truncate_sql.format(
            destination_table=self.destination_table
        )
        
        if self.truncate:
            redshift.run(rendered_truncate_sql)
            self.log.info('table {destination_table} truncated'.format(destination_table=self.destination_table))        

        rendered_insert_sql = LoadDimensionOperator.insert_sql.format(destination_table=self.destination_table,\
                                                                 insert_statement=self.insert_statement)
        redshift.run(rendered_insert_sql)
        self.log.info('table {destination_table} populated'.format(destination_table=self.destination_table))   