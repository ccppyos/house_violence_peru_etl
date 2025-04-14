import os

import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.dates import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
import subprocess
import requests
import io
import json
import pandas as pd


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")
S3_DESTINATION= 'raw'
USER_NUMBER= os.environ.get("USER_NUMBER", "1")

year = "{{ ds[:4] }}"
file_link_template= f"https://raw.githubusercontent.com/ccppyos/data_fem/refs/heads/main/casos_fem_{year}.csv"
s3_script = "utils/scripts/"

SPARK_STEPS = [
    {
        "Name": "Transformation",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://data-camp-bucket-crp/utils/scripts/transformation.py",
                "--year",
                "{{ execution_date.strftime('%Y') }}"
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'ExtrasDataTransformer',
    'ReleaseLabel': 'emr-5.34.0',
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
    'LogUri': 's3://data-camp-bucket-crp/logs',
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Primary node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                "Name": "Core node",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'Steps': SPARK_STEPS,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}



def get_file_link(exec_date, **kwargs):

   file_link = f"https://raw.githubusercontent.com/ccppyos/data_fem/refs/heads/main/casos_fem_{exec_date}.csv"
   response = requests.get(file_link)
   df = pd.read_csv(io.StringIO(response.text), encoding='latin-1')

   print(exec_date)
   csv_buffer = io.StringIO()
   df.to_csv(csv_buffer, index=False,encoding='latin-1')

   s3_hook = S3Hook(aws_conn_id='aws_default')
   s3_hook.load_string(
      string_data=csv_buffer.getvalue(),
      key=f"{S3_DESTINATION}/casos_fem_{exec_date}.csv",
      bucket_name=S3_BUCKET,
      replace=True
   )

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "depends_on_past": False,  # the previous task instance needs to have succeeded for the current one to run
    "retries": 1,
}

with DAG(
    dag_id="etl_process_fem",
    schedule_interval="0 0 1 * *",  # run this dag every Tuesday at 11:55pm
    max_active_runs=3,
    catchup=False,
    tags=['s3', 'aws', 'ingestion', 'cycling'],
    default_args=default_args
) as dag:

    begin_task = DummyOperator(
        task_id="begin_task"
    )

    download_csv_file = PythonOperator(
        task_id="download_csv_file_to_s3",
        provide_context=True,
        python_callable=get_file_link,
        op_kwargs={
            "exec_date": "{{execution_date.strftime('%Y')}}",
        }
    )

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default'
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id=cluster_creator.output,
        steps=SPARK_STEPS,
        params={
            "BUCKET": S3_BUCKET,
            "s3_script": s3_script
        },
        aws_conn_id='aws_default'
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id=cluster_creator.output,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster', 
        job_flow_id=cluster_creator.output,
        aws_conn_id='aws_default'
    )

    copy_command = RedshiftDataOperator(
        task_id='copy_data_to_redshift',
        database='dev',
        cluster_identifier='redshift-cluster-0',
        db_user='redshifu',
        sql=f"""
            COPY public.reported_cases_peru 
            FROM 's3://data-camp-bucket-crp/landing/data_cleaned_{{ execution_date.strftime('%Y') }}/part-*'
            IAM_ROLE 'arn:aws:iam::'{USER_NUMBER}':role/etl_access' \
            FORMAT AS PARQUET;
        """,
        poll_interval=10,     
        aws_conn_id='aws_default',
        region='us-east-1',
        dag=dag
    )



    finish_task = DummyOperator(
        task_id="finish_task"
    )
    
    
    begin_task >> download_csv_file >>  cluster_creator >> step_adder >> step_checker >> cluster_remover >> copy_command>> finish_task
