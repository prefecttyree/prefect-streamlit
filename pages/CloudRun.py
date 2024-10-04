#import google.auth
from google.cloud import run_v2
from google.cloud import logging_v2
import time

def create_and_run_job(project_id, location, job_name, image_name):
    # Initialize the Cloud Run client
    client = run_v2.JobsClient()

    # Create the job
    job = run_v2.Job()
    job.template = run_v2.ExecutionTemplate()
    job.template.containers = [run_v2.Container(image=image_name)]

    parent = f"projects/{project_id}/locations/{location}"
    operation = client.create_job(parent=parent, job_id=job_name, job=job)
    response = operation.result()

    print(f"Created job: {response.name}")

    # Run the job
    run_client = run_v2.ExecutionsClient()
    execution = run_v2.Execution()
    execution.job = response.name

    operation = run_client.create_execution(parent=parent, execution=execution)
    execution = operation.result()

    print(f"Started execution: {execution.name}")
    return execution.name

def stream_logs(project_id, execution_name):
    logging_client = logging_v2.Client(project=project_id)

    filter_str = f'resource.type="cloud_run_revision" AND resource.labels.execution_name="{execution_name.split("/")[-1]}"'

    for entry in logging_client.list_entries(filter_=filter_str):
        print(f"{entry.timestamp}: {entry.payload}")

def main():
    # Replace these with your actual values
    project_id = "prefect-sbx-sales-engineering"
    location = "us-central1"
    job_name = "my-cloud-run-job"
    image_name = "gcr.io/prefect-sbx-sales-engineering/prefect-streamlit:latest"

    execution_name = create_and_run_job(project_id, location, job_name, image_name)

    print("Streaming logs...")
    stream_logs(project_id, execution_name)

if __name__ == "__main__":
    main()