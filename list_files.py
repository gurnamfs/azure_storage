from client import dl_service_client, directory_exists
import asyncio
from concurrent.futures import ThreadPoolExecutor
from azure.storage.filedatalake import DataLakeDirectoryClient
from typing import List
from fastapi import HTTPException


# Thread pool for running blocking code asynchronously
executor = ThreadPoolExecutor(max_workers=5)

def list_files_in_dir_sync(storage_name: str, dir_name: str) -> List[str]:
    """Synchronous function to list files in a directory"""
    files = []
    try:
        if directory_exists(storage_name, dir_name):
            file_system_client = dl_service_client.get_file_system_client(storage_name)
            paths = file_system_client.get_paths(path=dir_name) or []
            files = [path['name'].replace(f'{dir_name}/',"") for path in paths]
            return files
        else:
            raise HTTPException(status_code=404, detail=f"Directory {dir_name} does not exist.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {e}")
    return files


async def list_files_in_dir_async(storage_name: str, dir_name: str) -> List[str]:
    """Async wrapper for the blocking list_files_in_dir_sync function"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, list_files_in_dir_sync, storage_name, dir_name)