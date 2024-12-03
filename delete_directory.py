from fastapi import HTTPException
from client import dl_service_client, directory_exists, storage_exists
import asyncio
from concurrent.futures import ThreadPoolExecutor


# Thread pool for running blocking code asynchronously
executor = ThreadPoolExecutor(max_workers=5)

def delete_dir_sync(storage_name: str, dir_name: str) -> bool:
    """Synchronous function to delete a directory"""
    try:
        if not storage_exists(storage_name):
            raise HTTPException(status_code=409, detail="Storage Does Not Exist.")       
        if directory_exists(storage_name, dir_name):
            fs_client = dl_service_client.get_file_system_client(storage_name)
            fs_client.delete_directory(dir_name)
            return True
        else:
            raise HTTPException(status_code=409, detail="Directory Does Not Exist.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete directory: {e}")

async def delete_dir_async(storage_name: str, dir_name: str) -> bool:
    """Async wrapper for the blocking delete_dir_sync function"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, delete_dir_sync, storage_name, dir_name)
