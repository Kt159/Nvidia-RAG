from dotenv import load_dotenv
from typing import List, Optional
import os

from llama_index.embeddings.nvidia import NVIDIAEmbedding
from llama_index.core.node_parser import SemanticSplitterNodeParser, SentenceSplitter
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader, StorageContext, Document
from llama_index.core.schema import BaseNode
from llama_index.vector_stores.milvus import MilvusVectorStore

from pymilvus import Collection, connections


#To be removed
from llama_index.embeddings.azure_openai import AzureOpenAIEmbedding


load_dotenv()
os.environ["NVIDIA_API_KEY"] = os.getenv("NVIDIA_API_KEY")



class Indexing_Pipeline():
    def __init__(self, 
                 embed_model: Optional[str] = "NV-Embed-QA"):
        
        self.embed_model = embed_model
        self.embedder = None  
        self.vector_store = None

    def read_document(self, path:List[str]) -> List[Document]:
        """Reads documents from the given path
        
        Args:
            path (List[str]): List of paths to the files (pdf, docx)
        
        Returns:
            List[Document]: List of documents (each document is a page of the file with associated metadata)
        
        """
        reader = SimpleDirectoryReader(input_files=path)
        documents = reader.load_data()
        return documents
    
    def initialize_embedder(self):
        if self.embed_model == "NV-Embed-QA":
            embedder = NVIDIAEmbedding(model=self.embed_model, truncate="END")

        elif self.embed_model == "text-embedding-ada-002":
            embedder = AzureOpenAIEmbedding(
                model=self.embed_model,
                engine=self.embed_model,
                api_version="2024-07-01-preview",
                azure_endpoint=os.getenv('AZURE_3.5_ENDPOINT'),
                api_key=os.getenv('AZURE_3.5_API_KEY')
            )  
        
        self.embedder = embedder
        return embedder

    def chunk_document(self, documents:List[Document], chunk_size:int) -> List[BaseNode]:
        """Chunks the document into smaller parts

        Args:
            documents (List[Document]): List of documents

        Returns:
            List[BaseNode]: List of chunks
        """
        embedder = self.initialize_embedder()
        base_splitter = SentenceSplitter(chunk_size=chunk_size) 
        splitter = SemanticSplitterNodeParser(
            buffer_size=1, breakpoint_percentile_threshold=95, embed_model=embedder, base_splitter=base_splitter
        )
        chunks = splitter.get_nodes_from_documents(documents)
        print(f"Chunked the document into {len(chunks)} chunks")

        return chunks
    
    def initialize_milvus_store(self, dim):
        """
        Initializes the milvus store with the given vector dimensions

        Args:
            dim (int): Dimension of the vectors
            collection_name (str): Name of the collection

        Returns:
            str: Message indicating the status of the initialization
        
        """
        collection_name = os.getenv("MILVUS_COLLECTION_NAME")
        milvus_port = os.getenv("MILVUS_PORT")
        host_IP = os.getenv("MILVUS_HOST")

        milvus_store = MilvusVectorStore(
            dim=dim,
            collection_name=collection_name,
            uri=f"http://{host_IP}:{milvus_port}/",
            overwrite=False
        )
        if milvus_store:
            self.vector_store = milvus_store
            print (f"Initialized {milvus_store.collection_name} milvus store at {milvus_store.uri}")
        
        else:
            return "Error with milvus store initialization"
        
    def add_to_milvus_store(self, chunks:List[BaseNode]):
        """
        Adds the chunks to the milvus store

        Args:
            chunks (List[BaseNode]): List of chunks to be added
            vector_store_index (VectorStoreIndex): VectorStoreIndex object containing previous chunks
        """
        storage_context = StorageContext.from_defaults(vector_store=self.vector_store)
        VectorStoreIndex.from_documents(
            [Document(text=chunk.text) for chunk in chunks], storage_context=storage_context, embed_model=self.embedder
        )
        print(f"Added {len(chunks)} chunks to the milvus store")

        return
    
    def reset_milvus_store(self):
        """
        Resets the milvus store by deleting the collection
        """
        milvus_port = os.getenv("MILVUS_PORT")
        host_IP = os.getenv("MILVUS_HOST")
        
        connections.connect(host=host_IP, port=milvus_port)
        collection = Collection(self.vector_store.collection_name)
        num_entities = collection.num_entities
        if num_entities > 0:
            collection.delete(expr=f"id in [0, {num_entities - 1}]")  # Delete all vectors
            print(f"Cleared {num_entities} vectors from collection '{self.vector_store.collection_name}'")
        else:
            print(f"Collection '{self.vector_store.collection_name}' is already empty.")
       
        return
        
    def run(self, path:List[str]) -> VectorStoreIndex:
        documents = self.read_document(path)
        chunks = self.chunk_document(documents=documents, chunk_size=512) #Chunk_Size = 512 is the Token limit for NV-Embed-QA embedding model 

        if not self.vector_store:
            if self.embed_model == "NV-Embed-QA":
                self.initialize_milvus_store(dim=512)
            elif self.embed_model == "text-embedding-ada-002":
                self.initialize_milvus_store(dim=1536) #Dimension of the vectors for Azure OpenAI Embedding model

            self.add_to_milvus_store(chunks=chunks)
        
        else:
            self.add_to_milvus_store(chunks=chunks)
                

    def check_milvus_store(self):
        collection = Collection(self.vector_store.collection_name)
        vector_count = collection.num_entities
        print(f"Number of vectors in the collection: {vector_count}")

        return



        
