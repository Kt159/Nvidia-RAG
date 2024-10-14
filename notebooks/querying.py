from dotenv import load_dotenv
from typing import List, Optional
import os

from pymilvus import Collection, connections
from llama_index.llms.nvidia import NVIDIA
from llama_index.vector_stores.milvus import MilvusVectorStore
from llama_index.core import VectorStoreIndex 



class Query_pipeline():
    def __init__(self, 
                vector_store: VectorStoreIndex,
                llm_model: Optional[str] = "mistralai/mistral-7b-instruct-v0.2"):
        
        self.vector_store = vector_store
        self.llm_model = llm_model
    
    def connect_to_vector_store(self):
        vectorstore = connections.connect(host='localhost', port='19530')
        return vectorstore
        
    def run(self, query:str):
        llm = NVIDIA(model=self.llm_model)
        query_engine = self.vector_store.as_query_engine(llm=llm)
        response = query_engine.query(query)

        return response
        
        
        