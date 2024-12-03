from client import dl_service_client
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import tempfile
from client import directory_exists

# Thread pool for running blocking code asynchronously
executor = ThreadPoolExecutor(max_workers=5)

def upload_file_sync(storage_name: str, dir_name: str, file_name: str, file_path: str) -> bool:
    """Synchronous function to upload a file"""
    try:
        if directory_exists(storage_name, dir_name):
            directory_client = dl_service_client.get_directory_client(storage_name, dir_name)
            file_client = directory_client.create_file(file_name)
            with open(file_path, "rb") as data:
                file_client.upload_data(data=data.read(), overwrite=True)
            return True
        else:
            return False
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{file_path}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")

async def upload_file_async(storage_name, dir_name: str, file_name: str, file_path: str) -> bool:
    """Async wrapper for the blocking upload_file_sync function"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, upload_file_sync,storage_name, dir_name, file_name, file_path)
