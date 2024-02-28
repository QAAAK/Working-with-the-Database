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
    dag_id = "LOAD_REP_URZK_DEP_PORTF",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:


    t1 = PostgresOperator(
    task_id ="LOAD",
    postgres_conn_id = "drp", # из вебинтерфейса airflow > admin > connections
    sql = """
            TRUNCATE TABLE gl."cdwh$budm_vrb$rep_urzk_dep_portf";
            INSERT INTO gl."cdwh$budm_vrb$rep_urzk_dep_portf"
            (dt, name_depositdealtype, name_depositdealmaintype, name_depositdealsegment, cl_segment, is_dbo, count_deposit, val_maindebt_nat, 
            val_maindebt, currency, id_currency, id_process, dttm_insert, "w$loadid", "w$ldate")
            (SELECT 
            dt, name_depositdealtype, name_depositdealmaintype, name_depositdealsegment, cl_segment, is_dbo, count_deposit, val_maindebt_nat, 
            val_maindebt, currency, id_currency, id_process, dttm_insert, 'Hello Ubad', CURRENT_TIMESTAMP FROM gl.budm_vrb_rep_urzk_dep_portf);
        """
    )

t1
