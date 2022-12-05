from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
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
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_folder=s3_key

    def execute(self, context):
        redshift= PostgresHook(self.redshift_conn_id)

        aws_hook=AwsHook(self.aws_credentials_id)
        aws_credentials=aws_hook.get_credentials()

        self.log.info('Delete existing data from {}'.format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('insert data into redshift from s3 folder')
        s3_file_path="s3://{}/{}".format(self.s3_bucket,self.s3_folder)

        sql_text="COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' format as json 'auto'"\
        .format(self.table,s3_file_path,aws_credentials.access_key,aws_credentials.secret_key)

        self.log.info('Executing...{{sql_text}} ...')
        redshift.run(sql_text)





