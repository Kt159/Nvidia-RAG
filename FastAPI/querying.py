from dotenv import load_dotenv
import os

from pydantic import BaseModel, Field
from llama_index.core import PromptTemplate
from llama_index.core.query_engine import CustomQueryEngine
from llama_index.core.retrievers import BaseRetriever
from llama_index.core import get_response_synthesizer
from llama_index.core.response_synthesizers import BaseSynthesizer
from llama_index.core import VectorStoreIndex

from llama_index.embeddings.nvidia import NVIDIAEmbedding
from llama_index.llms.nvidia import NVIDIA
from llama_index.llms.azure_openai import AzureOpenAI
from llama_index.embeddings.azure_openai import AzureOpenAIEmbedding

from llama_index.vector_stores.milvus import MilvusVectorStore
from pymilvus import connections, utility


load_dotenv()
os.environ["NVIDIA_API_KEY"] = os.getenv("NVIDIA_API_KEY")

class Query_Pipeline():

    """Pipeline for querying the vector store. 
       Current implementation supports querying the vector store using Azure OpenAI and NVIDIA models.

    Args:
        model_host (Optional[str], optional): Host of the model (E.g. Azure, NVIDIA). Defaults to "NVIDIA".
        model_name (Optional[str], optional): Name of the model (E.g. gpt-35-turbo, mistralai/mistral-7b-instruct-v0.2). Defaults to "mistralai/mistral-7b-instruct-v0.2".
    
    """
    def __init__(self):
        self.model_host = os.getenv("MODEL_HOST")
        self.milvus_host_IP = os.getenv("MILVUS_HOST")
        self.milvus_port = os.getenv("MILVUS_PORT")
        self.collection_name = os.getenv("MILVUS_COLLECTION_NAME")
        self.embedder = self.initialize_embedder()  
        self.milvus_store = self.connect_to_milvus_store()
        self.llm_model = self.initialize_llm_model()

    def initialize_embedder(self):
        if self.model_host == "NVIDIA":
            embedder = NVIDIAEmbedding(
                model=os.getenv('EMBEDDING_MODEL'),
                truncate="END")

        elif self.model_host == "AZURE":
            embedder = AzureOpenAIEmbedding(
                model=os.getenv('EMBEDDING_MODEL'),
                engine=os.getenv('EMBEDDING_MODEL'),
                api_version=os.getenv('API_VERSION'),
                azure_endpoint=os.getenv('ENDPOINT'),
                api_key=os.getenv('API_KEY')
            )  

        return embedder
    
    def connect_to_milvus_store(self):
        """
        Connects to an existing Milvus collection if it exists.

        Returns:
            str: Message indicating the status of the connection
        """
        
        # Establish a connection to Milvus
        connections.connect(host=self.milvus_host_IP, port=self.milvus_port)
        
        # Check if the collection already exists
        if utility.has_collection(self.collection_name):
            print(f"Milvus collection '{self.collection_name}' exists. Querying from collection.")
            milvus_store = MilvusVectorStore(
                collection_name=self.collection_name,
                uri=f"http://{self.milvus_host_IP}:{self.milvus_port}/",
                overwrite=False  # Reuse the existing collection without overwriting
            )
            
            return milvus_store
        
        else:
            raise Exception(f"Milvus collection '{self.collection_name}' does not exist. Please index documents before querying.")
        
    def initalize_retriever(self):
        milvus_store = self.milvus_store
        index = VectorStoreIndex.from_vector_store(vector_store=milvus_store)
        retriever = index.as_retriever(
            similarity_top_k=5,
        )

        return retriever

    def initialize_llm_model(self):
        if self.model_host == "AZURE":
            llm_model = AzureOpenAI(model=os.getenv('LLM_MODEL'),
                                    engine=os.getenv('LLM_MODEL'),
                                    api_version=os.getenv('API_VERSION'),
                                    azure_endpoint=os.getenv('ENDPOINT'),
                                    api_key=os.getenv('API_KEY')
                                    )

        elif self.model_host == "NVIDIA":
            llm_model = NVIDIA(model=os.getenv('LLM_MODEL'))

        else:
            raise ValueError("Unsupported model host specified in configuration.")

        return llm_model
    
    
    def run(self, query:str):
        
        """Run the query pipeline.

        Args:
            query (str): Query

        Returns:
            response: Response to query
        """

        qa_prompt = PromptTemplate( "You are a helpful chatbot assisting a user with a question.\n"
                                "If the user asks a question that you do not have the context to, say I don't have the required context.\n"
                                "Context information is below.\n"
                                "---------------------\n"
                                "{context_str}\n"
                                "---------------------\n"
                                "Given the context information and not prior knowledge, "
                                "answer the query.\n"
                                "Query: {query_str}\n"
                                "Answer: "
                                )
        
        # Set up synthesizer, LLM, and query engine
        synthesizer = get_response_synthesizer(response_mode="compact")
        llm = self.llm_model

        query_engine = RAGStringQueryEngine(
            retriever=self.initalize_retriever(),
            response_synthesizer=synthesizer,
            llm=llm,
            qa_prompt=qa_prompt,
        )

        response = query_engine.query(query)

        return response.response
        

class RAGStringQueryEngine(CustomQueryEngine, BaseModel):
    """Custom RAG String Query Engine."""

    retriever: BaseRetriever = Field(...)
    response_synthesizer: BaseSynthesizer = Field(...)
    llm: AzureOpenAI = Field(...)
    qa_prompt: PromptTemplate = Field(...)

    def custom_query(self, query_str: str) -> str:
        # Retrieve relevant nodes
        nodes = self.retriever.retrieve(query_str)
        
        # Generate context string from nodes
        context_str = "\n\n".join([n.node.get_content() for n in nodes])
        
        # Format prompt and query LLM
        formatted_prompt = self.qa_prompt.format(context_str=context_str, query_str=query_str)
        response = self.llm.complete(prompt=formatted_prompt)
        
        return str(response)

