from fastapi import FastAPI, Path, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from pydantic import BaseModel
from indexing import Indexing_Pipeline 
from querying import Query_Pipeline 
import tempfile
import os
import uvicorn
from minio import Minio
from llama_index.core import Settings


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Define Pydantic models for request validation
class Document(BaseModel):
    id: str
    text: str

class QueryRequest(BaseModel):
    query: str

minio_client = Minio(
    endpoint=os.getenv("MINIO_ENDPOINT"),  # MinIO endpoint (e.g., 'localhost:9000')
    access_key=os.getenv("MINIO_ACCESS_KEY"),  # MinIO access key
    secret_key=os.getenv("MINIO_SECRET_KEY"),  # MinIO secret key
    secure=False  # Disable SSL for local MinIO; set to True for HTTPS endpoints
)

bucket_name = os.getenv("MINIO_BUCKET_NAME")  # MinIO bucket name
# indexing_status = {}

# Background task for document indexing
def index_document_in_background(file_path):
    try: 
        indexing_pipeline = Indexing_Pipeline()
        index = indexing_pipeline.run([file_path])
        return index
    
    except Exception as e:
        print(f"Error indexing document: {e}") 


# Helper function for querying
def query_pipeline_execution(query: str):
    try:
        query_pipeline= Query_Pipeline() 
        # Set the embedder and LLM model in the settings
        Settings.embed_model = query_pipeline.embedder
        Settings.llm = query_pipeline.llm_model
        response = query_pipeline.run(query)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying documents: {e}")


@app.post("/index")
async def index_document(file_name: str):
    try:
        print(f"Attempting to retrieve file from MinIO: {file_name}")
        response = minio_client.get_object(bucket_name, file_name)
        
        if not response:
            raise HTTPException(status_code=404, detail=f"Document '{file_name}' not found in MinIO")

        index = index_document_in_background(file_name)
        return {"index": index}
    
    except Exception as e:
        print(f"Error occurred: {e}")  # Log the error
        raise HTTPException(status_code=500, detail=f"Error indexing document: {e}")
    
    finally:
        if 'response' in locals():
            response.close()
            response.release_conn()

# Route to handle document querying
@app.post("/query")
async def query_documents(query: QueryRequest):
    try:
        response = query_pipeline_execution(query.query) 
        return {"response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving response: {e}")

    
@app.delete("/delete")
async def delete_indexes(file_name: str = Query(...)):
    indexing_pipeline = Indexing_Pipeline()
    response = indexing_pipeline.delete_milvus_indexes_using_filename(file_name)
    return response
    

# Run the FastAPI application
if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)