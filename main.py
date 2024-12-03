from fastapi import FastAPI, HTTPException, UploadFile
from list_directories import list_dirs_async
from list_files import list_files_in_dir_async
from create_directory import create_directory_async
from upload_file import upload_file_async
from delete_directory import delete_dir_async
from delete_file import delete_file_in_dir_async
from create_storage import create_storage
from list_storages import get_storages
# from client import dl_service_client
from client import dl_service_client, storage_exists
import tempfile
import os
import logging
from typing import Union, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.post("/create-directory/{storage_name}/{dir_name}", response_model=str)
async def create_directory(
    storage_name: str,
    dir_name: str,
) -> str:
    """Endpoint to create a directory in the given storage."""
    try:
        result = await create_directory_async(storage_name, dir_name)
        return result
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


@app.post("/create_storage/{storage_name}")
async def create_storage_endpoint(storage_name: str):
    """
    FastAPI endpoint to create a new storage system.
    """
    try:
        result = await create_storage(storage_name)
        if result:
            return f"Storage {storage_name} created successfully."
        else:
            raise HTTPException(status_code=400, detail="Storage already exists.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/delete-directory/")
async def delete_directory(storage_name: str, dir_name: str):
    """API endpoint to delete a directory"""
    try:
        success = await delete_dir_async(storage_name, dir_name)
        if success:
            return "Directory deleted successfully"
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")


@app.delete("/delete-file/")
async def delete_file(storage_name: str, dir_name: str, file_name: str):
    """Endpoint to delete a file in a given directory."""
    try:
        success = await delete_file_in_dir_async(storage_name, dir_name, file_name)
        if success:
            return {"message": f"File {file_name} deleted successfully from {dir_name}."}
        else:
            raise HTTPException(status_code=404, detail="File not found.")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting file: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error.")


@app.delete("/delete_storage/{storage_name}")
async def delete_storage(storage_name: str):
    try:
        # Directly check if the storage_name exists in the file systems.
        if storage_exists(storage_name):
            dl_service_client.delete_file_system(storage_name)
            return f"Storage '{storage_name}' deleted successfully."
        else:
            raise HTTPException(status_code=404, detail="Storage not found.")
    except Exception as e:
        # Handle possible exceptions
        raise HTTPException(status_code=500, detail=f"Error deleting storage '{storage_name}': {str(e)}")


# FastAPI endpoint to list directories asynchronously
@app.get("/list_dirs/{storage_name}", response_model=Union[List[str], bool])
async def get_directories(storage_name: str):
    """Endpoint to fetch directories in a storage"""
    result = await list_dirs_async(storage_name)
    if result is False:
        raise HTTPException(status_code=404, detail="Storage not found or not accessible")
    return result


@app.get("/list_files/{storage_name}/{dir_name}", response_model=List[str])
async def list_files(storage_name: str, dir_name: str):
    """API route to list files in a specified directory in DataLake"""
    try:
        files = await list_files_in_dir_async(storage_name, dir_name)
        if files is False:
            raise HTTPException(status_code=404, detail="Directory not found or empty")
        return files
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/storages", response_model=List[str])
async def read_storages():
    # Call the get_storages function and return the result
    storages = await get_storages()
    return storages


@app.post("/upload-file/")
async def upload_file(storage_name: str, dir_name: str, file_name: str, file: UploadFile):
    """FastAPI endpoint to upload a file to Azure Data Lake"""
    try:
        # Ensure the file is a PDF
        if not file.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail="Only PDF files are allowed.")

        # Create a temporary file using the tempfile module
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name

            # Save the uploaded file to the temporary location
            with open(temp_file_path, "wb") as buffer:
                buffer.write(await file.read())

            # Call the async function to upload the file
            success = await upload_file_async(storage_name, dir_name, file_name, temp_file_path)

        # Clean up the temporary file after upload
        os.remove(temp_file_path)

        if success:
            return f"File '{file_name}' uploaded successfully to directory '{dir_name}'."
        else:
            raise HTTPException(status_code=500, detail="Failed to upload file")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))