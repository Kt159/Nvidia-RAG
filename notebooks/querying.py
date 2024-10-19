from dotenv import load_dotenv
from typing import List, Optional
import os

from llama_index.llms.nvidia import NVIDIA
from llama_index.llms.azure_openai import AzureOpenAI
from llama_index.vector_stores.milvus import MilvusVectorStore
from llama_index.core import VectorStoreIndex

load_dotenv()
os.environ["NVIDIA_API_KEY"] = os.getenv("NVIDIA_API_KEY")

class Query_Pipeline():

    """Pipeline for querying the vector store. 
       Current implementation supports querying the vector store using Azure OpenAI and NVIDIA models.

    Args:
        model_host (Optional[str], optional): Host of the model (E.g. Azure, NVIDIA). Defaults to "NVIDIA".
        model_name (Optional[str], optional): Name of the model (E.g. gpt-35-turbo, mistralai/mistral-7b-instruct-v0.2). Defaults to "mistralai/mistral-7b-instruct-v0.2".
    
    """
    def __init__(self,
                 model_host: Optional[str] = "NVIDIA", 
                 model_name: Optional[str] = "mistralai/mistral-7b-instruct-v0.2"):
        
        self.model_host = model_host
        self.model_name = model_name
        self.llm_model = None

    def run(self, 
            milvus_vector_store:VectorStoreIndex, 
            query:str):
        
        """Run the query pipeline

        Args:
            milvus_vector_store (MilvusVectorStore): Milvus vector store
            query (str): Query

        Returns:
            response: Response to query
        """
        if self.model_host == "Azure":
            self.llm_model = AzureOpenAI(model=self.model_name,
                                         engine=self.model_name,
                                         api_version="2024-07-01-preview",
                                         azure_endpoint=os.getenv('AZURE_3.5_ENDPOINT'),
                                         api_key=os.getenv('AZURE_3.5_API_KEY')
                                         )
    

        elif self.model_host == "NVIDIA":
            self.llm_model = NVIDIA(model=self.model_name)

        query_engine = milvus_vector_store.as_query_engine(llm=self.llm_model)
        response = query_engine.query(query)

        return response.response
        
        
        