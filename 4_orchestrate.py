from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataProcPySparkOperator

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('fire_incident_workflow',
          default_args=default_args,
          description='Workflow para procesamiento de datos de incendios',
          schedule_interval=timedelta(days=1))

# Crear un clúster de Dataproc en Kubernetes
create_cluster = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    project_id='tu-proyecto-gcp',
    cluster_name='fire-incidents-cluster',
    num_workers=2,
    region='us-central1',
    zone='us-central1-f',
    dag=dag)

# Tarea de extracción de datos
extract_task = DataProcPySparkOperator(
    task_id='extract_data',
    main='gs://tu-bucket/1_extract_api_fire.py',
    cluster_name='fire-incidents-cluster',
    region='us-central1',
    dag=dag)

# Tarea de transformación de datos
transform_task = DataProcPySparkOperator(
    task_id='transform_data',
    main='gs://tu-bucket/2_transform_fire.py',
    cluster_name='fire-incidents-cluster',
    region='us-central1',
    dag=dag)

# Tarea de acceso y preparación de datos para BI
access_task = DataProcPySparkOperator(
    task_id='access_data',
    main='gs://tu-bucket/3_access_fire.py',
    cluster_name='fire-incidents-cluster',
    region='us-central1',
    dag=dag)

# Eliminar el clúster de Dataproc
delete_cluster = DataprocClusterDeleteOperator(
    task_id='delete_dataproc_cluster',
    project_id='tu-proyecto-gcp',
    cluster_name='fire-incidents-cluster',
    region='us-central1',
    trigger_rule='all_done',
    dag=dag)

# Definir el orden de las tareas
create_cluster >> extract_task >> transform_task >> access_task >> delete_cluster
