from airflow.contrib.hooks.aws_hook import AwsHook 
from airflow.hooks.S3_hook import S3Hook 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    This operator takes as parameters: 
        - AWS credentials for connection to S3,
        - the S3 bucket and key,
        - the region of the S3 bucket,
        - the path to the json format used to copy json log files,
        - the destination table of the COPY from S3 buckets. 
    The parameters are used to copy the files from s3 to the destination table.        
    """
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        COMPUPDATE OFF REGION '{}'
        FORMAT  JSON '{}'
        TIMEFORMAT  'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    """
    
    truncate_sql = "truncate table {table}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region= "",
                 json_path= "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path
        self.aws_credentials_id = aws_credentials_id
        
        
    def execute(self, context):
        self.log.info('StageToRedshiftOperator starting')
        #aws_hook = S3Hook(self.aws_credentials_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        rendered_truncate_sql = StageToRedshiftOperator.truncate_sql.format(table = self.table)
        redshift.run(rendered_truncate_sql)

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        redshift.run(formatted_sql)
        self.log.info('StageToRedshiftOperator Ended')





