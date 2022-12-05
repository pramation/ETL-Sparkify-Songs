from datetime import datetime, timedelta
import os
from airflow import DAG

from helpers import cr_tbls_script
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 5, 24),
    'depends_on_past': False,
    #'retries': 3,
    #'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('DP_cr_table6',
          default_args=default_args,
          description='Create tables in Redshift with Airflow',
          #schedule_interval='@hourly'
        )

class CreateTableOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_source='',
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_source=sql_source


    def execute(self, context):
        redshift= PostgresHook(self.redshift_conn_id)
        self.log.info("Executing...{self.sql_source}")
        redshift.run(self.sql_source)


        
staging_events_table_drop = CreateTableOperator(
    task_id='staging_events_table_drop',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.staging_events_table_drop
)
staging_songs_table_drop = CreateTableOperator(
    task_id='staging_events_table_drop',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.staging_songs_table_drop
)
songplay_table_drop = CreateTableOperator(
    task_id='songplay_table_drop',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.songplay_table_drop
)
user_table_drop = CreateTableOperator(
    task_id='user_table_drop',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.user_table_drop
)
song_table_drop = CreateTableOperator(
    task_id='song_table_drop',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.song_table_drop
)
artist_table_drop = CreateTableOperator(
    task_id='artist_table_drop',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.artist_table_drop
)
time_table_drop = CreateTableOperator(
    task_id='time_table_drop',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.time_table_drop
)
##
staging_events_table_create = CreateTableOperator(
    task_id='staging_events_table_create',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.staging_events_table_create
)
staging_songs_table_create = CreateTableOperator(
    task_id='staging_songs_table_create',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.staging_songs_table_create
)
songplay_table_create = CreateTableOperator(
    task_id='songplay_table_create',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.songplay_table_create
)
user_table_create  = CreateTableOperator(
    task_id='user_table_create',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.user_table_create 
)
song_table_create = CreateTableOperator(
    task_id='song_table_create',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.song_table_create
)
artist_table_create = CreateTableOperator(
    task_id='artist_table_create',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.artist_table_create
)
time_table_create = CreateTableOperator(
    task_id='time_table_create',
    dag=dag,
    redshift_conn_id="redshift",
    sql_source=cr_tbls_script.time_table_create
)

staging_events_table_drop
staging_songs_table_drop
staging_songs_table_drop>>songplay_table_drop
songplay_table_drop>>user_table_drop
user_table_drop>>song_table_drop
song_table_drop>>artist_table_drop
artist_table_drop>>time_table_drop

time_table_drop>>staging_events_table_create
staging_events_table_create>>staging_songs_table_create
staging_songs_table_create>>user_table_create 
user_table_create>>song_table_create 
song_table_create>>artist_table_create 
artist_table_create>>time_table_create 
time_table_create>>songplay_table_create

