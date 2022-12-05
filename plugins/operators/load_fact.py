from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_source,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_source=sql_source
        

    def execute(self, context):
        redshift_hook=PostgresHook(self.redshift_conn_id)
        
        self.log.info("Remove data from the fact table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        sql_text="INSERT INTO {} {};".format(self.table,self.sql_source)
        
        self.log.info(f"Executing...{sql_text}")
        redshift.run(sql_text)
                      
                      
        
        
   