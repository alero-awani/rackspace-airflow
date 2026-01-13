"""
Daily AI Model Retraining DAG
Runs feature generation and model training on ephemeral Rackspace Spot nodes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Node selector for Rackspace Spot instances
# Adjust these labels based on how your spot nodes are labeled
spot_node_selector = {
    "node.kubernetes.io/instance-type": "spot",
    # or 'rackspace.com/node-type': 'spot',
}

# Tolerations for spot instances (if using taints)
spot_tolerations = [
    k8s.V1Toleration(
        key="rackspace.com/spot", operator="Equal", value="true", effect="NoSchedule"
    )
]

# Resource requests for training pods
training_resources = k8s.V1ResourceRequirements(
    requests={
        "memory": "8Gi",
        "cpu": "4",
        # 'nvidia.com/gpu': '1',  # Uncomment if using GPUs
    },
    limits={
        "memory": "16Gi",
        "cpu": "8",
        # 'nvidia.com/gpu': '1',
    },
)

with DAG(
    "daily_model_retraining",
    default_args=default_args,
    description="Daily feature generation and model retraining on spot instances",
    schedule_interval="0 2 * * *",  # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "retraining", "spot"],
) as dag:

    # Task 1: Generate features from daily data
    generate_features = KubernetesPodOperator(
        task_id="generate_features",
        name="feature-generation-pod",
        namespace="airflow",
        image="your-registry/feature-generator:latest",
        cmds=["python"],
        arguments=[
            "generate_features.py",
            "--date",
            "{{ ds }}",  # Airflow execution date
            "--output",
            "s3://your-bucket/features/{{ ds }}/",
        ],
        node_selector=spot_node_selector,
        tolerations=spot_tolerations,
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "4Gi", "cpu": "2"}, limits={"memory": "8Gi", "cpu": "4"}
        ),
        # Delete pod after completion to free resources
        is_delete_operator_pod=True,
        # Get logs for debugging
        get_logs=True,
        # Startup timeout
        startup_timeout_seconds=600,
    )

    # Task 2: Train model on generated features
    train_model = KubernetesPodOperator(
        task_id="train_model",
        name="model-training-pod",
        namespace="airflow",
        image="your-registry/model-trainer:latest",
        cmds=["python"],
        arguments=[
            "train_model.py",
            "--features",
            "s3://your-bucket/features/{{ ds }}/",
            "--model-output",
            "s3://your-bucket/models/{{ ds }}/",
            "--epochs",
            "50",
            "--batch-size",
            "128",
        ],
        node_selector=spot_node_selector,
        tolerations=spot_tolerations,
        container_resources=training_resources,
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
        # Environment variables for cloud credentials
        env_vars={
            "AWS_ACCESS_KEY_ID": "{{ var.value.aws_access_key }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.aws_secret_key }}",
            "MLFLOW_TRACKING_URI": "{{ var.value.mlflow_uri }}",
        },
    )

    # Task 3: Validate trained model
    validate_model = KubernetesPodOperator(
        task_id="validate_model",
        name="model-validation-pod",
        namespace="airflow",
        image="your-registry/model-validator:latest",
        cmds=["python"],
        arguments=[
            "validate_model.py",
            "--model",
            "s3://your-bucket/models/{{ ds }}/",
            "--validation-data",
            "s3://your-bucket/validation/",
            "--metrics-output",
            "s3://your-bucket/metrics/{{ ds }}.json",
        ],
        node_selector=spot_node_selector,
        tolerations=spot_tolerations,
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "4Gi", "cpu": "2"}, limits={"memory": "8Gi", "cpu": "4"}
        ),
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
    )

    # Task 4: Deploy model if validation passes
    deploy_model = KubernetesPodOperator(
        task_id="deploy_model",
        name="model-deployment-pod",
        namespace="airflow",
        image="your-registry/model-deployer:latest",
        cmds=["python"],
        arguments=[
            "deploy_model.py",
            "--model",
            "s3://your-bucket/models/{{ ds }}/",
            "--deployment-target",
            "production",
        ],
        node_selector=spot_node_selector,
        tolerations=spot_tolerations,
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "2Gi", "cpu": "1"}, limits={"memory": "4Gi", "cpu": "2"}
        ),
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
    )

    # Define task dependencies
    generate_features >> train_model >> validate_model >> deploy_model
