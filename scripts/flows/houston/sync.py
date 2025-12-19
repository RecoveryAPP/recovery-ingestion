from prefect import flow
from prefect.logging import get_run_logger
from rclone_python import rclone


@flow
def sync(onedrive_path: str, bucket_name: str):
    """
    Flow to synchronize data with Houston.
    This flow is designed to be run in a Docker container.
    """
    logger = get_run_logger()
    logger.info("Starting synchronization...")
    
    rclone.sync(
        src_path=f'onedrive:{onedrive_path}',
        dest_path=f'minio:{bucket_name}',
    )
    
    logger.info("Synchronization completed successfully.")


if __name__ == '__main__':
    sync.serve(
        name='test_deployment'
    )