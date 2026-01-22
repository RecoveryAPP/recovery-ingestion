from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import boto3
import utils
import requests
from prefect import flow, task
from prefect.logging import get_run_logger


# ----------------------------
# Helpers (plain Python funcs)
# ----------------------------
def separate_list(content: Optional[str]) -> Optional[List[str]]:
    if not content:
        return None
    return [item.strip() for item in content.split(";") if item.strip()]


def transform_newspaper_level_fn(
    newspaper_orig_data: Iterable[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    newspaper_data: Dict[str, Dict[str, Any]] = {}
    for item in newspaper_orig_data:
        ident = item.get("Identifier")
        if ident:
            newspaper_data[ident] = item
    return newspaper_data


def retrieve_from_newspaper_level_fn(
    identifier: str,
    field_name: str,
    newspaper_data: Dict[str, Dict[str, Any]],
) -> str:
    data = newspaper_data.get(identifier) or {}
    value = data.get(field_name)
    return value if value is not None else "placeholder"


# -------------
# Prefect tasks
# -------------
@task
def create_clients(
    *,
    s3_access_key: str,
    s3_secret_key: str,
    s3_endpoint: str,
) -> Dict[str, Any]:
    """
    Creates an S3 client (MinIO or compatible), using deployment parameters.
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
    )
    return {"s3": s3_client}


@task
def map_item_to_metadata(
    item: Dict[str, Any],
    mapping: Dict[str, Any],
    template: Dict[str, Any],
    newspaper_data: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    logger = get_run_logger()

    asset: Dict[str, Any] = {}
    asset["title"] = item.get("Issue Title")
    asset["title_spanish"] = item.get("Issue Title [SPAN]")
    asset["alternative_title"] = item.get("Title Alternative")
    asset["alternative_title_spanish"] = item.get("Title Alternative [SPAN]")
    asset["issue_title"] = item.get("Issue Title")
    asset["creator"] = item.get(
        "'Creator (Alternative Title: Publisher? Most often printed as \"Director\" or \"Owner\")"
    )

    identifier = item.get("Identifier", "")
    asset["description"] = retrieve_from_newspaper_level_fn(
        identifier=identifier,
        field_name="Abstract",
        newspaper_data=newspaper_data,
    )
    asset["description_spanish"] = retrieve_from_newspaper_level_fn(
        identifier=identifier,
        field_name="Abstract [SPAN]",
        newspaper_data=newspaper_data,
    )

    asset["publicationtype"] = item.get("Type of Periodical")
    asset["author_name"] = item.get("Creator")

    tags = separate_list(item.get("Tags"))
    tags_spanish = separate_list(item.get("Tags [SPAN]"))
    if tags:
        asset["tags"] = tags
    if tags_spanish:
        asset["tags_spanish"] = tags_spanish

    if item.get("Language"):
        asset["language"] = item.get("Language")
    if item.get("Single Dates"):
        asset["production_date"] = item.get("Single Dates")
    if item.get("Coverage-Spatial"):
        asset["production_place"] = item.get("Coverage-Spatial")

    # NOTE: original code had mismatched key casing: "[SPAN]" vs "[Span]"
    if item.get("Issue Title [SPAN]"):
        asset["issue_title_spanish"] = item.get("Issue Title [SPAN]")
    if item.get("Type of Periodical [SPAN]"):
        asset["publicationtype_spanish"] = item.get("Type of Periodical [SPAN]")

    if tags or tags_spanish:
        asset["keywords"] = [*(tags_spanish or []), *(tags or [])]

    logger.info(asset)

    response = requests.post(
        url="http://houstonmapper:8099/mapper/",
        json={
            "metadata": asset,
            "template": template,
            "mapping": mapping,
        },
        timeout=120,
    )
    response.raise_for_status()
    logger.info({"mapper_response": response.json()})
    return response.json()


@task
def split_filenames(
    item: Dict[str, Any],
    s3_client,
    *,
    s3_bucket: str,
) -> Set[str]:
    prefix = item.get("Identifier", "")
    logger = get_run_logger()

    logger.info(f"Running object query for {item.get('Identifier')} - {item.get('Title')}")
    keys: Set[str] = set()

    filenames = separate_list(item.get("Pages"))
    logger.info({"pages": filenames})

    if not filenames or not prefix:
        return keys

    response = s3_client.list_objects_v2(Prefix=prefix, Bucket=s3_bucket)
    if response.get("Contents"):
        for obj in response["Contents"]:
            keys.add(obj["Key"])
    else:
        logger.info("No files found.")

    return keys


@task
def retrieve_files_for_metadata(
    filename_keys: Set[str],
    s3_client,
    *,
    s3_bucket: str,
) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    for key in filename_keys:
        filename_only = key.split("/")[-1]
        filedata = s3_client.get_object(Bucket=s3_bucket, Key=key)
        files.append({"file": (filename_only, filedata["Body"].read())})
    return files


@task
def ingest_metadata(
    refined_metadata: Dict[str, Any],
    *,
    endpoint_url: str,
    dt_alias: str,
    api_key: str,
) -> requests.Response:
    logger = get_run_logger()
    logger.info(refined_metadata)

    response = requests.post(
        url="http://houston-importer:8090/importer/",
        json={
            "metadata": refined_metadata,
            "dataverse_information": {
                "base_url": endpoint_url,
                "dt_alias": dt_alias,
                "api_token": api_key,
            },
        },
        timeout=300,
    )
    response.raise_for_status()
    logger.info({"importer_response": response.json()})
    return response


@task
def add_file(
    file: Dict[str, Any],
    doi: str,
    *,
    endpoint_url: str,
    dt_alias: str,
    api_key: str,
) -> requests.Response:
    data = {
        "json_data": json.dumps(
            {
                "doi": doi,
                "dataverse_information": {
                    "base_url": endpoint_url,
                    "dt_alias": dt_alias,
                    "api_token": api_key,
                },
            }
        )
    }
    response = requests.post(
        url="http://houston-importer:8090/file-upload/",
        files=file,
        data=data,
        timeout=300,
    )
    response.raise_for_status()
    return response

@task
def get_file_object(bucket_name: str, key: str) -> dict:
    logger = get_run_logger()

    minio_client = utils.create_s3_client()
    try:
        response = minio_client.get_object(
            Bucket=bucket_name,
            Key=key
        )
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        logger.error(f"Error retrieving file from S3: {e}")
        raise e
    
@task
def get_all_keys(bucket_name: str) -> List[str]:
    minio_client = utils.create_s3_client()

    paginator = minio_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name)
    keys = []
    for response in response_iterator:
        if response.get('Contents'):
            keys.extend(item['Key'] for item in response['Contents'])
    return keys


# ----------
# Sub-flows
# ----------
@flow
def process_item(
    item: Dict[str, Any],
    clients: Dict[str, Any],
    mappingjson: Dict[str, Any],
    templatejson: Dict[str, Any],
    newspaper_data: Dict[str, Dict[str, Any]],
    *,
    endpoint_url: str,
    dt_alias: str,
    api_key: str,
    s3_bucket: str,
) -> None:
    metadata = map_item_to_metadata(
        item=item,
        mapping=mappingjson,
        template=templatejson,
        newspaper_data=newspaper_data,
    )

    filenames = split_filenames(
        item=item,
        s3_client=clients["s3"],
        s3_bucket=s3_bucket,
    )
    files = retrieve_files_for_metadata(
        filename_keys=filenames,
        s3_client=clients["s3"],
        s3_bucket=s3_bucket,
    )

    ingest_resp = ingest_metadata(
        refined_metadata=metadata,
        endpoint_url=endpoint_url,
        dt_alias=dt_alias,
        api_key=api_key,
    )
    doi = ingest_resp.json()["data"]["persistentId"]

    for f in files:
        add_file(
            file=f,
            doi=doi,
            endpoint_url=endpoint_url,
            dt_alias=dt_alias,
            api_key=api_key,
        )


# -------------
# Main workflow
# -------------
@flow
def ingest_to_dataverse(
    api_key: str,
    dt_alias: str,
    endpoint_url: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_endpoint: str,
    s3_bucket: str,
    newspaper_data_key: str,
    issue_data_key: str,
    mappingjson_key: str,
    templatejson_key: str,
) -> None:
    """
    Required parameters (show up in the Prefect UI deployment):
      - api_key
      - dt_alias
      - endpoint_url
      - s3_access_key
      - s3_secret_key
      - s3_endpoint
      - s3_bucket
    """
    logger = get_run_logger()



    mappingjson = get_file_object(
        bucket_name=s3_bucket,
        key=mappingjson_key
    )

    templatejson = get_file_object(
        bucket_name=s3_bucket,
        key=templatejson_key
    )

    issue_data = get_file_object(
        bucket_name=s3_bucket,
        key=issue_data_key
    )

    newspaper_orig_data = get_file_object(
        bucket_name=s3_bucket,
        key=newspaper_data_key
    )

    newspaper_data = transform_newspaper_level_fn(newspaper_orig_data=newspaper_orig_data)

    clients = create_clients(
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_endpoint=s3_endpoint,
    )

    logger.info(f"Loaded {len(issue_data)} issue items; {len(newspaper_data)} newspaper records")
    logger.info({"s3_endpoint": s3_endpoint, "s3_bucket": s3_bucket})


    for item in issue_data:
        process_item(
            item=item,
            templatejson=templatejson,
            mappingjson=mappingjson,
            newspaper_data=newspaper_data,
            clients=clients,
            endpoint_url=endpoint_url,
            dt_alias=dt_alias,
            api_key=api_key,
            s3_bucket=s3_bucket,
        )


# --------------------
# Deployment-on-invoke
# --------------------
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deploy the Prefect flow to a local Prefect server.")
    # match your Makefile flags exactly
    p.add_argument("--api_key", required=True, help="Dataverse API token")
    p.add_argument("--dt_alias", required=True, help="Dataverse alias")
    p.add_argument("--endpoint_url", required=False, default="", help="Dataverse base URL (optional; can be set in UI)")
    # S3 data
    p.add_argument("--s3_access_key", required=True, help="S3/MinIO access key")
    p.add_argument("--s3_secret_key", required=True, help="S3/MinIO secret key")
    p.add_argument("--s3_endpoint", required=True, help="S3/MinIO endpoint URL")
    p.add_argument("--s3_bucket", required=True, help="S3 bucket name")
    # Mappinng keys
    p.add_argument("--newspaper_data_key", required=True, help="S3 key for newspaper-level data JSON")
    p.add_argument("--issue_data_key", required=True, help="S3 key for issue data JSON")
    p.add_argument("--mappingjson_key", required=True, help="S3 key for mapping JSON")
    p.add_argument("--templatejson_key", required=True, help="S3 key for template JSON")
    # Prefect stuff
    p.add_argument("--deployment-name", default="ingest-to-dataverse", help="Prefect deployment name")
    p.add_argument("--work-pool", default="local", help="Work pool name for the deployment")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    source_dir = str(Path(__file__).parent.resolve())
    ingest_to_dataverse.from_source(
        source=source_dir,
        entrypoint=f"{Path(__file__).name}:ingest_to_dataverse",
    ).deploy(
        name=args.deployment_name,
        work_pool_name=args.work_pool,
        parameters={
            "api_key": args.api_key,
            "dt_alias": args.dt_alias,
            "endpoint_url": args.endpoint_url,
            "s3_access_key": args.s3_access_key,
            "s3_secret_key": args.s3_secret_key,
            "s3_endpoint": args.s3_endpoint,
            "s3_bucket": args.s3_bucket,
            "newspaper_data_key": args.newspaper_data_key,
            "issue_data_key": args.issue_data_key,
            "mappingjson_key": args.mappingjson_key,
            "templatejson_key": args.templatejson_key,
        },
    )
