from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id='',
                 redshift_conn_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id=aws_credentials_id
        self.redshift_conn_d=redshift_conn_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_folder=s3_key

    def execute(self, context):
        redshift= PostgresHook(redshit_conn_id)

        aws_hook=AwsHook(aws_credentials_id)
        aws_credentials=aws_hook.get_credentials()

        self.log.info('Delete existing data from {}'.format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('insert data into redshift from s3 folder')
        s3_file_path="s3://{}/{}".format(self.s3_bucket,self.s3_folder)

        sql_text="COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_KEY_ID '{}'  format as json 'auto' DELIMITER ',' DELIMITER ','".format(self.table,s3_file_path,credentials.access_key,credentials.secret_key)

        self.log.info('Executing...{sql_text} ...')
        redshift.run(sql_text)










