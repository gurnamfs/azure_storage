from fastapi import FastAPI, HTTPException, Query
from client import dl_service_client, directory_exists, storage_exists
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Thread pool for running blocking code asynchronously
executor = ThreadPoolExecutor(max_workers=5)


def delete_file_in_dir_sync(storage_name: str, dir_name: str, file_name: str) -> bool:
    """Synchronous function to delete a file in a directory."""
    try:
        if not storage_exists(storage_name):
            raise HTTPException(status_code=404, detail="Storage Does Not Exist")

        if directory_exists(storage_name, dir_name):
            file_system_client = dl_service_client.get_file_system_client(storage_name)
            dir_client = file_system_client.get_directory_client(dir_name)
            if dir_client.get_file_client(file_name).exists():
                file_client = dir_client.get_file_client(file_name)
                file_client.delete_file()
                return True
            else:
                raise HTTPException(status_code=404, detail="File Does Not Exist")
        else:
            raise HTTPException(status_code=404, detail="Directory Does Not Exist")

    except Exception as e:
        logger.error(f"Failed to delete file {os.path.join(dir_name, file_name)}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")

async def delete_file_in_dir_async(storage_name: str, dir_name: str, file_name: str) -> bool:
    """Async wrapper for the blocking delete_file_in_dir_sync function."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, delete_file_in_dir_sync, storage_name, dir_name, file_name)
