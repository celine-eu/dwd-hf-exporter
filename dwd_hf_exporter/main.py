import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from huggingface_hub import HfApi, hf_hub_download
import time
import converter  # your converter

# Enable HF Transfer for large files
os.environ["HF_HUB_VERBOSE"] = "1"
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"


# -------------------------------------------------------------------
# Load environment variables
# -------------------------------------------------------------------

load_dotenv()

# -------------------------------------------------------------------
# S3 client factory
# -------------------------------------------------------------------


def get_s3_client():
    endpoint = os.getenv("AWS_S3_ENDPOINT")  # optional for MinIO

    return boto3.client(
        "s3",
        endpoint_url=endpoint if endpoint else None,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "eu-central-1"),
    )


# -------------------------------------------------------------------
# S3 Utility helpers
# -------------------------------------------------------------------


def s3_key_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def upload_to_s3(s3, local_path: str, bucket: str, key: str):
    s3.upload_file(local_path, bucket, key)


# -------------------------------------------------------------------
# HF Utility: list files for a specific date
# -------------------------------------------------------------------


def list_hf_files_for_date(api: HfApi, repo_id: str, date: datetime):
    """
    Returns all the .zarr.zip files available under:
    data/YYYY/MM/DD/
    """

    yyyy, mm, dd = date.year, date.month, date.day
    folder_path = f"data/{yyyy}/{mm}/{dd}"

    try:
        tree = api.list_repo_tree(
            repo_id=repo_id,
            repo_type="dataset",
            path_in_repo=folder_path,
        )
    except Exception as e:
        print(f" ⚠️ Could not list HF directory {folder_path}: {e}")
        return []

    files = [node.path for node in tree if node.path.endswith(".zarr.zip")]

    return files


# -------------------------------------------------------------------
# Process one single day (sequential)
# -------------------------------------------------------------------


def process_single_day(
    s3,
    api,
    repo_id: str,
    date: datetime,
    local_root: str,
    s3_bucket: str,
    s3_prefix: str,
    force: bool,
    dryrun: bool = False,
):

    if dryrun:
        print("**** Running in dryrun mode ****")

    yyyy, mm, dd = date.year, date.month, date.day
    print(f"\n=== {date.date()} ===")

    # 1. List files available for this date
    hf_files = list_hf_files_for_date(api, repo_id, date)

    if not hf_files:
        print(" No files found for this date on HF")
        return []

    results = []

    for remote_path in hf_files:
        filename = os.path.basename(remote_path)

        local_dir = os.path.join(local_root, f"{yyyy}/{mm}/{dd}")
        os.makedirs(local_dir, exist_ok=True)

        local_original = os.path.join(local_dir, filename)
        converted_filename = filename.replace(".zarr.zip", ".nc")
        converted_local = os.path.join(local_dir, converted_filename)

        converted_s3_key = f"{s3_prefix}/{yyyy}/{mm:02d}/{dd:02d}/{converted_filename}"

        print(f"\n → Processing file: {filename}")

        # 2. Skip if this file is already converted and uploaded
        if not force and s3_key_exists(s3, s3_bucket, converted_s3_key):
            print(f" ✓ Already in S3, skipping → {converted_s3_key}")
            results.append(f"SKIPPED {date.date()} {filename}")
            continue

        # 3. Download
        print(f"   Downloading HF file: {remote_path}")
        try:
            temp_local_dir = "./"
            if not dryrun:
                downloaded_path = hf_hub_download(
                    repo_id=repo_id,
                    filename=remote_path,
                    repo_type="dataset",
                    local_dir=temp_local_dir,
                    force_download=force,
                )
            else:
                downloaded_path = local_original
                print(f"*** dryrun: create local {downloaded_path}")
                with open(downloaded_path, "w") as f:
                    f.write("")
                time.sleep(1)

        except Exception as e:
            print(f"    Download failed: {e}")
            results.append(f"FAILED-DOWNLOAD {date.date()} {filename}")
            continue

        # 4. Convert
        print(f"   Converting...")
        try:
            converted_path = converter.extract(downloaded_path)
        except Exception as e:
            print(f"    Conversion failed: {e}")
            results.append(f"FAILED-CONVERT {date.date()} {filename}")
            continue

        # 5. Upload
        print(f"   Uploading to S3 → {converted_s3_key}")
        try:
            if not dryrun:
                upload_to_s3(s3, converted_path, s3_bucket, converted_s3_key)
            else:
                print(f"*** dryrun: skip upload {converted_path} -> {converted_s3_key}")
        except Exception as e:
            print(f"    Upload failed: {e}")
            results.append(f"FAILED-UPLOAD {date.date()} {filename}")
            continue

        # 6. Cleanup
        try:
            print(f"   Removing original...")
            os.remove(local_original)
        except OSError:
            pass

        print(f" ✓ Completed {filename}")
        results.append(f"OK {date.date()} {filename}")

    return results


# -------------------------------------------------------------------
# Sequential controller
# -------------------------------------------------------------------


def run_pipeline(
    repo_id: str,
    start_date: str,
    end_date: str,
    local_root: str,
    s3_bucket: str,
    s3_prefix: str,
    force: bool,
    dryrun: bool = False,
):

    print("Starting DWD HF Exporter Pipeline")
    api = HfApi()
    s3 = get_s3_client()

    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)

    print("Dates:", start.date(), "to", end.date())

    date = start
    results = []

    while date <= end:
        print(f"\nProcessing date: {date.date()}")
        day_results = process_single_day(
            s3=s3,
            api=api,
            repo_id=repo_id,
            date=date,
            local_root=local_root,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            force=force,
            dryrun=dryrun,
        )
        results.extend(day_results)
        date += timedelta(days=1)

    print("\n=== SUMMARY ===")
    for r in results:
        print(r)


# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------

if __name__ == "__main__":
    print("DWD HF Exporter main.py")
    run_pipeline(
        repo_id="openclimatefix/dwd-icon-eu",
        start_date="2025-07-01",
        end_date="2025-07-01",
        local_root="./data",
        s3_bucket="celine-pipelines-dwd",
        s3_prefix="openclimatefix--dwd-icon-eu",
        force=False,
        dryrun=False,
    )
