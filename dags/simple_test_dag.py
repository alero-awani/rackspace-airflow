"""
Simple Test DAG to verify Airflow and Spot Node setup
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Node selector for Rackspace Spot instances
spot_node_selector = {
    "servers.ngpc.rxt.io/type": "spot",
}


def print_context(**kwargs):
    """Simple Python function to print execution context"""
    print(f"Execution date: {kwargs['ds']}")
    print(f"Task instance: {kwargs['task_instance']}")
    print("Simple test task completed successfully!")
    return "success"


with DAG(
    "simple_test_workflow",
    default_args=default_args,
    description="Simple test workflow to verify Airflow and spot nodes",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test", "spot"],
) as dag:

    # Task 1: Simple Python task (runs on Airflow worker)
    python_task = PythonOperator(
        task_id="python_test",
        python_callable=print_context,
    )

    # Task 2: Simple bash command on spot node
    bash_test = KubernetesPodOperator(
        task_id="bash_test_on_spot",
        name="bash-test-pod",
        namespace="airflow",
        image="busybox:latest",
        cmds=["sh", "-c"],
        arguments=[
            "echo 'Running on spot node'; "
            "echo 'Hostname:' $(hostname); "
            "echo 'Date:' $(date); "
            "sleep 10; "
            "echo 'Test completed successfully!'"
        ],
        node_selector=spot_node_selector,
        tolerations=[],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "128Mi", "cpu": "100m"},
            limits={"memory": "256Mi", "cpu": "200m"},
        ),
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=300,
    )

    # Task 3: Python script on spot node
    python_pod_test = KubernetesPodOperator(
        task_id="python_pod_test",
        name="python-test-pod",
        namespace="airflow",
        image="python:3.11-slim",
        cmds=["python", "-c"],
        arguments=[
            "import sys; "
            "import platform; "
            "print(f'Python version: {sys.version}'); "
            "print(f'Platform: {platform.platform()}'); "
            "print('Python pod test completed!'); "
        ],
        node_selector=spot_node_selector,
        tolerations=[],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "256Mi", "cpu": "100m"},
            limits={"memory": "512Mi", "cpu": "500m"},
        ),
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=300,
    )

    # Task 4: Longer running task to test spot stability
    stress_test = KubernetesPodOperator(
        task_id="spot_stability_test",
        name="stress-test-pod",
        namespace="airflow",
        image="busybox:latest",
        cmds=["sh", "-c"],
        arguments=[
            "echo 'Starting 2-minute stress test...'; "
            "for i in $(seq 1 24); do "
            "  echo \"Progress: $i/24 ($(($i * 5)) seconds)\"; "
            "  sleep 5; "
            "done; "
            "echo 'Stress test completed - spot node is stable!'"
        ],
        node_selector=spot_node_selector,
        tolerations=[],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "128Mi", "cpu": "100m"},
            limits={"memory": "256Mi", "cpu": "200m"},
        ),
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=300,
    )

    # Define task dependencies
    python_task >> bash_test >> python_pod_test >> stress_test
