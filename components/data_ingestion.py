from fastapi import FastAPI, HTTPException
import os
from pydantic import BaseModel
from dotenv import load_dotenv

from couchdb_service import CouchDBClient
from minio_service import MinIOClient
from parse_funcs import *
from config import CouchDb, UPLOAD_FOLDER, setup_logging

load_dotenv()
app = FastAPI()
minio_client = MinIOClient()
couchdb_client = CouchDBClient()
logger = setup_logging()
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Define the structure of the incoming JSON payload
class FileEvent(BaseModel):
    EventName: str
    Key: str
    Records: list


@app.post("/webhook/new-file")
async def retrieve_file(event: FileEvent):
    """
    Endpoint to handle MinIO event notifications and retrieve the uploaded file.

    This function listens for POST requests with an event payload from MinIO.
    It verifies the event type, extracts the bucket name and file key from the payload,
    downloads the file from MinIO, and processes it (including parsing and storing 
    the file into CouchDB).

    Args:
        event (FileEvent): A Pydantic model representing the structure of the event payload.
            - EventName (str): The name of the event, typically 's3:ObjectCreated:Put'.
            - Key (str): The key (path) of the uploaded file in the MinIO bucket.
            - Records (list): A list of records containing bucket and object details.

    Returns:
        dict: A success message indicating the file has been processed, including the file name.

    Raises:
        HTTPException: If the file download or processing fails.
    """
    logger.info("Received webhook event.")

    if event.EventName == "s3:ObjectCreated:PutTagging":
        logger.info("PutTAGGING")
        return {"message": "File tag updated"}

    # Extract the bucket name and file key
    key = event.Key
    bucket_name = event.Records[0]["s3"]["bucket"]["name"]
    logger.debug(f"Extracted bucket_name: {bucket_name}, key: {key}")

    # Define the temporary file path
    file_name = key.split("/")[-1]
    file_path = os.path.join(UPLOAD_FOLDER, file_name)
    logger.debug(f"Computed file_name: {file_name}, file_path: {file_path}")

    # Download the file from MinIO
    minio_client.download_file(bucket_name, file_name, file_path)

    # Process the file and insert into CouchDB
    try:
        for filename in os.listdir(UPLOAD_FOLDER):
            upload_folder_file_path = os.path.join(UPLOAD_FOLDER, filename)
            if os.path.isfile(
                upload_folder_file_path
            ):  # Ensure it's a file and not a directory
                parsed_document = parse_document(file_path)
                logger.info(f"Successfully parsed file '{file_name}'")
                for i in parsed_document:

                    couchdb_client.insert_document(CouchDb.db_name, i)
                # If insertion is successful, remove the file
                os.remove(upload_folder_file_path)
                logger.info(
                    f"Processed and removed file '{file_name}' from '{UPLOAD_FOLDER}'"
                )
    except Exception as e:
        # print(f"Failed to process the file '{upload_folder_file_path}: {e}")
        logger.error(f"Failed to process the file '{file_name}': {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to process the file: {str(e)}"
        )

    # Return a success response
    logger.info(f"File '{file_name}' processed successfully")
    return {"message": "File processed successfully", "file": file_name}


@app.post("/webhook/delete-file")
async def delete_file(event: FileEvent):
    """
    Endpoint to handle MinIO event notifications and delete the corresponding document from CouchDB.

    This function listens for POST requests with an event payload from MinIO. 
    It verifies the event type, extracts the bucket name and file key from the payload, 
    constructs the source path, and deletes the corresponding document from CouchDB.

    Args:
        event (FileEvent): A Pydantic model representing the structure of the event payload.
            - EventName (str): The name of the event, expected to be 's3:ObjectRemoved:Delete'.
            - Key (str): The key (path) of the deleted file in the MinIO bucket.
            - Records (list): A list of records containing bucket and object details.

    Returns:
        None

    Raises:
        Error in logger if the deletion from CouchDB fails.
    """
    print("Delete file webhook received")

    # Extract the bucket name and file key
    key = event.Key
    bucket_name = event.Records[0]["s3"]["bucket"]["name"]
    logger.debug(f"Extracted bucket_name: {bucket_name}, key: {key}")

    # Define the temporary file path
    file_name = key.split("/")[-1]
    file_path = os.path.join(UPLOAD_FOLDER, file_name)
    logger.debug(f"Computed file_name: {file_name}, file_path: {file_path}")
    source_value = f"./uploads/{file_name}"
    logger.debug(f"Constructed source_value: {source_value}")

    # Perform the deletion from CouchDB
    try:
        couchdb_client.delete_document_by_source(CouchDb.db_name, source_value)
        logger.info(
            f"Successfully deleted document with source '{source_value}' from CouchDB"
        )
    except Exception as e:
        logger.error(
            f"Failed to delete document with source '{source_value}': {str(e)}"
        )

# Uncomment if want to run FastAPI application directly
# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8001)
