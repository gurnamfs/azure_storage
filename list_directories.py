from fastapi import HTTPException
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union
from client import dl_service_client, storage_exists

# Create a ThreadPoolExecutor for running the synchronous function in a separate thread
executor = ThreadPoolExecutor(max_workers=5)

def list_dirs_sync(storage_name: str) -> Union[List[str], bool]:
    """Synchronous function to list directories"""
    if storage_exists(storage_name):
        fs_client = dl_service_client.get_file_system_client(storage_name)
        directories = [i['name'] for i in fs_client.get_paths() or [] if i['is_directory']]
        if not directories:
            raise HTTPException(status_code=404, detail="No Folders Exist")
        return directories
    else:
        raise HTTPException(status_code=404, detail="Storage Does Not Exist")

async def list_dirs_async(storage_name: str) -> Union[List[str], bool]:
    """Async wrapper for the blocking list_dirs_sync function"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, list_dirs_sync, storage_name)
