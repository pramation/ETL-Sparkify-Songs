from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,dq_checks
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dq_checks=dq_checks
        

    def execute(self, context):
        redshift_hook=PostgresOperator(self.redshift_conn_id)
        for dq_check in dq_checks:
            
            num_rows=redshift_hook.get_records(f"SELECT count(*) FROM {{self.table}}")
            if (num_rows <1  and len(records[0])<1):
                raise ValueError(f"Data quality check failed. Table {self.table} returned no result")
            if num_rows[0][0]<1:
                raise ValueError(f"Data quality check failed. Table {self.table} has zero rows")
            
         logging.info(f"Data quality on table {table} check passed with {num_rows[0][0]} records")
             