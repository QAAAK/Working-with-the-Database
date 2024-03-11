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
    dag_id = "FULL_REP_SALES_FUNNEL",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:

    t1 = PostgresOperator( 
    task_id ="LOAD",
    postgres_conn_id = "drp", # из вебинтерфейса airflow > admin > connections
    sql = """
            INSERT INTO gl."cdwh$budm_vrb$rep_sales_funnel"
            (application_number, id_customer, application_date, issue_date, document_number,
            product_code, subproduct_code, kkr_result, kkr_reason, applicationstage, declinereason, 
            longapp, order_status, is_crm_app, is_cc_app, attract_channel, app_status, branch_code, branch_name,
            office_code, employee_name, employee_number, application_source, product_type, product_name, application_way, 
            sale_channel, client_segment, client_significance, requested_sum, accepted_sum, requested_sum_short, accepted_sum_short, 
            issue_sum, sale_flag, agg_channel, k_accept_full, k_accept_short, id_process, dttm_insert, "w$loadid", "w$ldate")
            (SELECT application_number, id_customer, application_date, issue_date, document_number,
            product_code, subproduct_code, kkr_result, kkr_reason, applicationstage, declinereason, 
            longapp, order_status, is_crm_app, is_cc_app, attract_channel, app_status, branch_code, branch_name,
            office_code, employee_name, employee_number, application_source, product_type, product_name, application_way, 
            sale_channel, client_segment, client_significance, requested_sum, accepted_sum, requested_sum_short, accepted_sum_short, 
            issue_sum, sale_flag, agg_channel, k_accept_full, k_accept_short, id_process, dttm_insert, 'Hello ubad', CURRENT_TIMESTAMP
            FROM gl.budm_vrb_rep_sales_funnel);
        """
    )

t1
