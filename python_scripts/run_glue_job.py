from etl_manager.etl import GlueJob
import os

from constants import job_bucket

def main():
    iam_role = os.environ["IAM_ROLE"]
    github_tag = os.environ["GITHUB_TAG"]
    snapshot_date = os.environ["SNAPSHOT_DATE"]

    # Get job parameters for specific glue job
    job_args = {"--github_tag": github_tag, "--snapshot_date": snapshot_date}
    job = GlueJob(f"glue_jobs/example_job/", bucket = job_bucket, job_role = iam_role, job_arguments = job_args)

    print(f'Starting job "{job.job_name}"...')
    job.run_job()
    job.wait_for_completion(verbose=True)

    if job.job_run_state == 'SUCCEEDED':
        print('Job successful - cleaning up')
        job.cleanup()

if __name__ == "__main__":
    main()