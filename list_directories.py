from fastapi import FastAPI, HTTPException
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union
from client import dl_service_client, storage_exists

# Initialize FastAPI app
app = FastAPI()

# Create a ThreadPoolExecutor for running the synchronous function in a separate thread
executor = ThreadPoolExecutor(max_workers=5)

def list_dirs_sync(storage_name: str) -> Union[List[str], bool]:
    """Synchronous function to list directories"""
    if storage_exists(storage_name):
        fs_client = dl_service_client.get_file_system_client(storage_name)
        directories = [i['name'] for i in fs_client.get_paths() or [] if i['is_directory']]
        return directories
    else:
        return False

async def list_dirs_async(storage_name: str) -> Union[List[str], bool]:
    """Async wrapper for the blocking list_dirs_sync function"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, list_dirs_sync, storage_name)
