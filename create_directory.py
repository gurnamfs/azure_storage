from fastapi import HTTPException, Depends
from concurrent.futures import ThreadPoolExecutor
import asyncio
from typing import Tuple
from client import directory_exists, dl_service_client


executor = ThreadPoolExecutor(max_workers=5)

def create_directory_sync(storage_name: str, dir_name: str) -> str:
    """Synchronous function to create a directory."""
    try:
        if not directory_exists(storage_name, dir_name):
            file_system_client = dl_service_client.get_file_system_client(storage_name)
            file_system_client.create_directory(dir_name)
            return "Directory Created Successfully!"
        else:
            raise HTTPException(status_code=409, detail="Directory Already Exists")
    except Exception as e:
        raise HTTPException(status_code=409, detail=f"Failed to create directory: {e}")

async def create_directory_async(storage_name: str, dir_name: str) -> str:
    """Async wrapper for the blocking create_directory_sync function."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, create_directory_sync, storage_name, dir_name)