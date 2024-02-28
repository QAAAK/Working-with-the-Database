from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'santalov', # если не отработает, то 'owner': 'admin'
    'start_date': days_ago(0),
    'depends_on_past': False
}


with DAG (
    dag_id = "LOAD_VRB_AGG_CUSTOMER",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:
    
    t1 = PostgresOperator(
    task_id ="LOAD",
    postgres_conn_id = "drp", # из вебинтерфейса airflow > admin > connections
    sql = """
        INSERT INTO gl."cdwh$budm_vrb$vrb_agg_customer"
        (id_customer, dt, category, significant_group, "segment", id_system, id_process, dttm_insert, dttm_update, is_pb, id_customer_cdi, "w$loadid", "w$ldate")
        (SELECT id_customer, dt, category, significant_group, "segment", id_system, id_process, 
        dttm_insert, dttm_update, is_pb, id_customer_cdi, 'HELLO_UBAD', CURRENT_TIMESTAMP FROM gl.budm_vrb_vrb_agg_customer
        where id_process>(select max(id_process) from gl."cdwh$budm_vrb$vrb_agg_customer"));
        """
    )


t1
