from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator

# Define default_args, schedule_interval, and other DAG configurations
default_args = {
    'owner': 'Rohit Sharma',
    'start_date': datetime(2023, 7, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['rohitsharma301378@gmail.com'],  # Replace with your email address
}

dag = DAG(
    'dag_with_email',
    default_args=default_args,
    schedule_interval='@daily',
    on_failure_callback=email_failure,
    on_success_callback=email_success,
)

def email_failure(context):
    email_task = EmailOperator(
        task_id='send_failure_email',
        to='rohitsharma301378@gmail.com',  # Replace with the recipient's email address
        subject='Airflow DAG Execution Failed',
        html_content=f'The DAG has failed. Context: {context}',
        dag=dag,
    )
    email_task.execute(context=context)

def email_success(context):
    email_task = EmailOperator(
        task_id='send_success_email',
        to='rohitsharma301378@gmail.com',  # Replace with the recipient's email address
        subject='Airflow DAG Execution Successful',
        html_content=f'The DAG has run successfully. Context: {context}',
        dag=dag,
    )
    email_task.execute(context=context)

# Define tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

run_script1_task = BashOperator(
    task_id='run_script1',
    bash_command='python C:/Users/rohit.sharma4/data-project1/data-project1/gcp_landing_bronze_script.py',  # Replace with the actual path to script1.py
    dag=dag,
)

run_script2_task = BashOperator(
    task_id='run_script2',
    bash_command='python C:/Users/rohit.sharma4/data-project1/data-project1/gcp_bronze_silver_script.py',  # Replace with the actual path to script2.py
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set up task dependencies
start_task >> run_script1_task
start_task >> run_script2_task

run_script1_task >> end_task
run_script2_task >> end_task



# Define an EmailOperator for sending an email on successful completion
# email_task = EmailOperator(
#     task_id='send_email',
#     to='rohitsharma301378@gmail.com',  # Replace with the recipient's email address
#     subject='Airflow DAG Execution Successful',
#     html_content='The DAG has run successfully!',
#     dag=dag,
# )

# Set up task dependencies
# end_task >> email_task
