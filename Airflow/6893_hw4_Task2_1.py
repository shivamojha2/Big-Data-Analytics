from datetime import datetime, timedelta
from textwrap import dedent
import time

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Shivam',
    'depends_on_past': False,
    'email': ['123@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}


count = 0

def sleeping_function():
    """This is a function that will run within the DAG execution"""
    time.sleep(2)

def count_function():
    global count
    count += 1
    print('count_increase output: {}'.format(count))
    time.sleep(1)

def print_function():
    print('This task is a python operator')
    time.sleep(1)


with DAG(
    'Big_Data_HW4_Task2_1',
    default_args=default_args,
    description='DAG for task 2 of HW4',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2021, 12, 3, 3, 30, 00),
    catchup=False,
    tags=['Task2'],
) as dag:

    # t* examples of tasks created by instantiating operators
    
    t1 = BashOperator(
        task_id='t1',
        bash_command= 'echo "This task is part of Question 2"',
    )

    t2 = BashOperator(
        task_id='t2',
        bash_command= 'python3 /home/airflow/dags/py_script.py',
        retries=2,
    )

    t3 = BashOperator(
        task_id='t3',
        bash_command= 'date',
        retries=3,
    )

    t4 = PythonOperator(
        task_id='t4',
        python_callable=sleeping_function,
    )

    t5 = BashOperator(
        task_id='t5',
        bash_command= 'python3 /home/airflow/dags/py_script.py',
    )

    t6 = BashOperator(
        task_id='t6',
        bash_command= 'echo "This task is part of Question 2"',
    )

    t7 = PythonOperator(
        task_id='t7',
        python_callable=print_function,
        retries=2,
    )

    t8 = PythonOperator(
        task_id='t8',
        python_callable=count_function,
        retries=2,
    )

    t9 = BashOperator(
        task_id='t9',
        bash_command='sleep 2',
    )

    t10 = BashOperator(
        task_id='t10',
        bash_command= 'date',
    )

    t11 = PythonOperator(
        task_id='t11',
        python_callable=print_function,
        retries=2,
    )

    t12 = PythonOperator(
        task_id='t12',
        python_callable=sleeping_function,
        retries=2,
    )

    t13 = BashOperator(
        task_id='t13',
        bash_command= 'date',
    )

    t14 = BashOperator(
        task_id='t14',
        bash_command= 'echo "This task is part of Question 2"',
    )

    t15 = PythonOperator(
        task_id='t15',
        python_callable=count_function,
        retries=2,
    )

    t16 = PythonOperator(
        task_id='t16',
        python_callable=sleeping_function,
        retries=2,
    )

    t17 = BashOperator(
        task_id='t17',
        bash_command= 'echo "This task 17 is part of Question 2"',
    )

    t18 = BashOperator(
        task_id='t18',
        bash_command= 'python3 /home/airflow/dags/py_script.py',
    )

    t19 = PythonOperator(
        task_id='t19',
        python_callable=print_function,
        retries=1,
    )


    # task dependencies 
    t1 >> [t2, t3, t4, t5]
    t2 >> t6
    t3 >> [t7, t12]
    t5 >> [t8, t9]
    t7 >> [t13, t14, t18]
    t8 >> [t10, t15]
    t9 >> [t11, t12]
    [t10, t11, t12] >> t14
    [t13, t15, t17] >> t18
    t14 >> [t16, t17]
    [t16, t18] >> t19

