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
    dag_id = "LOAD_VRB_AGG_DEPOSITPORTFOLIO",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:


    t1 = PostgresOperator(
    task_id ="LOAD",
    postgres_conn_id = "drp", # из вебинтерфейса airflow > admin > connections
    sql = """
        INSERT INTO gl."cdwh$budm_vrb$vrb_agg_depositportfolio"
        (dt, id_deal, val_maindebt_nat, val_mainperc_nat, cnt_totalprolongation, dt_lastprolongation, sum_nat_init, sum_nat_out, id_product, id_system, id_process, dttm_insert, 
        dttm_update, val_maindebt, val_mainperc, sum_init, sum_out, is_changed, "w$loadid", "w$ldate")
        (SELECT dt, id_deal, val_maindebt_nat, val_mainperc_nat, cnt_totalprolongation, dt_lastprolongation, 
        sum_nat_init, sum_nat_out, id_product, id_system, id_process, dttm_insert, dttm_update, val_maindebt, val_mainperc, 
        sum_init, sum_out, is_changed, 'HELLO_UBAD', CURRENT_TIMESTAMP FROM gl.budm_vrb_vrb_agg_depositportfolio 
        where id_process>(select max(id_process) from gl."cdwh$budm_vrb$vrb_agg_depositportfolio"));
        """
    )



t1
