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
    dag_id = "FULL_VRB_FCT_DBOEVENT",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:


    t1 = PostgresOperator(
    task_id ="LOAD",
    postgres_conn_id = "drp", # из вебинтерфейса airflow > admin > connections
    sql = """
        INSERT INTO gl."cdwh$budm_vrb$vrb_fct_dboevent"
        (id_dboevent, code_dboevent, code_dboeventtype, name_dboeventtype, id_dboeventpayer, name_dboeventpayer, dt_dboevent, dboevent_amt, 
        dboevent_comission_amt, name_dboevent_service, code_dboeventrecipient, name_dboeventrecipient, account_dboeventrecipient, is_regular, 
        code_dboeventsendsys, code_dboeventstatus, name_dboeventstatus, dboevent_comment, is_template, currency, id_dboactivity, id_transaction, 
        dboevent_purpose, recipient_bic, recipient_name, recipient_inn, id_system, id_process, dttm_insert, dttm_update, "w$loadid", "w$ldate" )
        (SELECT id_dboevent, code_dboevent, code_dboeventtype, name_dboeventtype, id_dboeventpayer, name_dboeventpayer, 
        dt_dboevent, dboevent_amt, dboevent_comission_amt, name_dboevent_service, code_dboeventrecipient, 
        name_dboeventrecipient, account_dboeventrecipient, is_regular, code_dboeventsendsys, 
        code_dboeventstatus, name_dboeventstatus, dboevent_comment, is_template, currency, 
        id_dboactivity, id_transaction, dboevent_purpose, recipient_bic, recipient_name, recipient_inn, id_system, id_process, dttm_insert, dttm_update,
        'HELLO_UBAD', CURRENT_TIMESTAMP
        from gl."cdwh$budm_vrb$vrb_fct_dboevent");
        """
    )

t1
