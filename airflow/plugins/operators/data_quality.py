from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',dq_checks='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dq_checks=dq_checks


    def execute(self, context):
        
        redshift_hook=PostgresHook(self.redshift_conn_id)
        for dq_check in self.dq_checks:
            num_rows=redshift_hook.get_records(dq_check['check_sql'])
            self.log.info(dq_check['check_sql'])
            if num_rows[0][0] != dq_check['expected_result']:
                raise ValueError(f"Data quality check failed. Table {dq_check['check_sql']} has null rows")
            
        self.log.info(f"Data quality Check passed ")
             