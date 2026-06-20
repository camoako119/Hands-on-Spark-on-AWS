import boto3


GLUE_JOB_NAME = "process_reviews_job"
glue_client = boto3.client("glue")


def lambda_handler(event, context):
    """Start the AWS Glue job after reviews.csv is uploaded to S3."""
    print(f"Received S3 event: {event}")
    print(f"Starting AWS Glue job: {GLUE_JOB_NAME}")

    response = glue_client.start_job_run(JobName=GLUE_JOB_NAME)
    job_run_id = response["JobRunId"]

    print(f"Successfully started Glue job run: {job_run_id}")

    return {
        "statusCode": 200,
        "body": (
            f"Glue job {GLUE_JOB_NAME} started successfully. "
            f"Run ID: {job_run_id}"
        ),
    }
