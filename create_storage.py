from azure.storage.filedatalake import DataLakeServiceClient
from client import dl_service_client
from client import storage_exists
import asyncio
from fastapi import HTTPException

async def create_storage(storage_name: str) -> bool:
    """
    Creates a new storage system if it does not already exist.
    """
    try:
        if not storage_exists(storage_name):
            fs_client = await asyncio.get_event_loop().run_in_executor(
                None, lambda: dl_service_client.create_file_system(storage_name)
            )
            return True
        else:
            raise HTTPException(status_code=409, detail="Storage Already Exists")
    except Exception as e:
        raise HTTPException(status_code=409, detail=f"Failed to create directory: {e}")