from typing import List
import asyncio
from client import dl_service_client  # Assuming this is the correct import


# Define the asynchronous function to fetch storages
async def get_storages() -> List[str]:
    # Assuming dl_service_client.list_file_systems is a blocking function,
    # so we use asyncio.to_thread to run it in a separate thread.
    file_systems = await asyncio.to_thread(dl_service_client.list_file_systems)
    
    # Extract storage names
    storages = [fs['name'] for fs in file_systems]
    return storages