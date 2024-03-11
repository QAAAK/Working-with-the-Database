from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'kirsanov-nig', # если не отработает, то 'owner': 'admin'
    'start_date': days_ago(0),
    'depends_on_past': False
}


with DAG (
    dag_id = "FULL_VRB_DET_DEPOSITDEAL",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:


    t1 = PostgresOperator(
    task_id ="LOAD",
    postgres_conn_id = "drp", # из вебинтерфейса airflow > admin > connections
    sql = """
        INSERT INTO gl."cdwh$budm_vrb$vrb_det_depositdeal"
        (id_depositdeal, id_subject, account_number, name_depositdealtype, code_depositdealtype, dt_open, dt_close, rate_scheme,
         rate, dt_plan_close, name_depositdealsegment, is_capitalization, breaksum, is_withdrawal, is_replenishment, periodicity, 
        min_first_pay, min_add_first_pay, currency, is_dbo, code_department, manager, depositsum_nat_init, dt_from, dt_to, name_dealstatus, depositsum_init, id_system, id_process, dttm_insert, dttm_update, name_branch, id_account, id_subject_cdi, code_branch, id_manager, "w$loadid", "w$ldate" )
        (SELECT id_depositdeal, id_subject, account_number, name_depositdealtype, code_depositdealtype, dt_open, dt_close, 
        rate_scheme, rate, dt_plan_close, name_depositdealsegment, is_capitalization, breaksum, 
        is_withdrawal, is_replenishment, periodicity, min_first_pay, min_add_first_pay, currency, is_dbo, code_department, 
        manager, depositsum_nat_init, dt_from, dt_to, name_dealstatus, depositsum_init, id_system, id_process, dttm_insert, 
        dttm_update, name_branch, id_account, id_subject_cdi, code_branch, id_manager, 
        'HELLO_UBAD', CURRENT_TIMESTAMP FROM gl.budm_vrb_vrb_det_depositdeal);
        """
    )

t1
