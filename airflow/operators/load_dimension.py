from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_source,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_d = redshift_conn_id
        self.table = table
        self.sql_source=sql_source

    def execute(self, context):
        redshift = PostgresHook(redshit_conn_id)

        aws_hook = AwsHook(aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()

        self.log.info('Delete existing data from {}'.format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('insert data into redshift from staging_tables')

        sql_text = self.sql_source

        self.log.info('Executing...{sql_text} ...')
        redshift.run(sql_text)
