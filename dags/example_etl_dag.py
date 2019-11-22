from datetime import datetime
import os
import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

# DECLARE ENV VARIABLES HERE
GIT_TAG = 'v0.0.1'
AWS_ACCOUNT = os.getenv("AWS_ACCOUNT")
IMAGE = f"{AWS_ACCOUNT}.dkr.ecr.eu-west-1.amazonaws.com/airflow-etl-pipeline-example:{GIT_TAG}"
IAM_ROLE = "airflow-postcodes-example-role"
SNAPSHOT_DATE = datetime.now().strftime("%Y-%m-%d")
EXTRACT_SCRIPT = 'write_data_to_land.py'
CHECK_SCRIPT = 'test_data.py'
GLUE_JOB_SCRIPT = 'run_glue_job.py'
GLUE_SCHEMA_SCRIPT = 'create_database_schema.py'

task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "isichei",
    "email": ["karik.isichei@digital.justice.gov.uk"],
}

dag = DAG(
    "etl_pipeline_example",
    default_args=task_args,
    description="Run through our example pipeline",
    start_date=datetime(2019, 2, 7),
    schedule_interval='30 13 * * 0',
    catchup=False
)

tasks = {}

task_id = "extract"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": EXTRACT_SCRIPT
    },
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": IAM_ROLE},
)

task_id = "test-extract"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": CHECK_SCRIPT
    },
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": IAM_ROLE},
)

task_id = "run-curated"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": GLUE_JOB_SCRIPT,
        "GIT_TAG": GIT_TAG,
        "IAM_ROLE": IAM_ROLE,
        "SNAPSHOT_DATE": SNAPSHOT_DATE,
        "GLUE_JOB_SCRIPT": GLUE_JOB_SCRIPT
    },
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": IAM_ROLE},
)

task_id = "deploy-database"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": GLUE_SCHEMA_SCRIPT
    },
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": IAM_ROLE},
)

tasks['extract'] >> tasks['test-extract']
tasks['test-extract'] >> tasks['run-curated']
tasks['run-curated'] >> tasks['deploy-database']