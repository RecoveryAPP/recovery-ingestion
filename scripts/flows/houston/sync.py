from pathlib import Path

from prefect import flow
from prefect.logging import get_run_logger
from rclone_python import rclone


@flow
def sync(
    source_remote_name: str,
    source_remote_path: str,
    destination_remote_name: str,
    destination_remote_path: str,
):
    logger = get_run_logger()

    src = f"{source_remote_name}:{source_remote_path.lstrip('/')}"
    dest = f"{destination_remote_name}:{destination_remote_path.lstrip('/')}"

    logger.info("Starting synchronization: %s -> %s", src, dest)
    rclone.sync(src_path=src, dest_path=dest)
    logger.info("Synchronization completed successfully.")


def _parse_args():
    import argparse

    p = argparse.ArgumentParser(description="Deploy Prefect sync flow")
    p.add_argument("--deployment-name", required=True)
    p.add_argument("--work-pool", required=True)

    p.add_argument("--source-remote-name", required=True)
    p.add_argument("--source-remote-path", required=True)
    p.add_argument("--destination-remote-name", required=True)
    p.add_argument("--destination-remote-path", required=True)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    source_dir = str(Path(__file__).parent.resolve())

    # Create a Prefect deployment via Python (no .serve()).
    sync.from_source(
        source=source_dir,
        entrypoint=f"{Path(__file__).name}:sync",
    ).deploy(
        name=args.deployment_name,
        work_pool_name=args.work_pool,
        parameters={
            "source_remote_name": args.source_remote_name,
            "source_remote_path": args.source_remote_path,
            "destination_remote_name": args.destination_remote_name,
            "destination_remote_path": args.destination_remote_path,
        },
    )
