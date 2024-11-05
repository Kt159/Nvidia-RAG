from dotenv import load_dotenv
from typing import List, Optional
import os
import PyPDF2
from io import BytesIO

from llama_index.embeddings.nvidia import NVIDIAEmbedding
from llama_index.core.node_parser import SemanticSplitterNodeParser, SentenceSplitter
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader, StorageContext, Document
from llama_index.core.schema import BaseNode
from llama_index.vector_stores.milvus import MilvusVectorStore

from minio import Minio
from pymilvus import Collection, connections, utility


#To be removed
from llama_index.embeddings.azure_openai import AzureOpenAIEmbedding


load_dotenv()
os.environ["NVIDIA_API_KEY"] = os.getenv("NVIDIA_API_KEY")


class Indexing_Pipeline():

    """Pipeline for indexing the documents.
       Current implementation supports indexing the documents using NVIDIA and Azure OpenAI models.

    """

    def __init__(self, chunk_size:int = 512):
        self.chunk_size = chunk_size   #Chunk_Size = 512 is the Token limit for NV-Embed-QA embedding model
        self.embed_model = os.getenv("EMBEDDING_MODEL") or "text-embedding-ada-002"
        self.collection_name = os.getenv("MILVUS_COLLECTION_NAME")
        self.milvus_port = os.getenv("MILVUS_PORT")
        self.milvus_host_IP = os.getenv("MILVUS_HOST")
        self.model_host = os.getenv("MODEL_HOST")
        self.embedder = self.initialize_embedder()
        self.minio_bucket = os.getenv("MINIO_BUCKET_NAME")
        self.minio_client = Minio(
                                endpoint=os.getenv("MINIO_ENDPOINT"),  # Ensure this is 9000 for non-SSL
                                access_key=os.getenv("MINIO_ACCESS_KEY"),
                                secret_key=os.getenv("MINIO_SECRET_KEY"),
                                secure=False  
                            )
        self.milvus_store = None

    def read_document(self, path:List[str]) -> List[Document]:
        """Reads documents from the given path
        
        Args:
            path (List[str]): List of paths to the files (pdf, docx)
        
        Returns:
            List[Document]: List of documents (each document is a page of the file with associated metadata)
        
        """
        documents = []

        for file_name in path:
            # Fetch the file from MinIO
            response = self.minio_client.get_object(self.minio_bucket, file_name)
            file_content = response.read()  # Read the file as binary
            
            if file_name.endswith('.pdf'):
                # Handle PDF files
                pdf_stream = BytesIO(file_content)
                pdf_reader = PyPDF2.PdfReader(pdf_stream)

                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    pdf_text = page.extract_text() or ""

                     # Sanitize the extracted text
                    pdf_text = pdf_text.encode('utf-8', 'ignore').decode('utf-8', 'ignore')

                    documents.append(Document(text=pdf_text, metadata={"file_name": file_name, "page_num": page_num}))

        return documents
    
    def initialize_embedder(self):
        """
        Initializes the embedder based on the model host (NVIDIA or Azure)
        """

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

    def chunk_document(self, documents:List[Document], chunk_size:int) -> List[BaseNode]:
        """Chunks the document into smaller parts

        Args:
            documents (List[Document]): List of documents

        Returns:
            List[BaseNode]: List of chunks
        """
        base_splitter = SentenceSplitter(chunk_size=chunk_size) 
        splitter = SemanticSplitterNodeParser(
            buffer_size=1, breakpoint_percentile_threshold=95, embed_model=self.embedder, base_splitter=base_splitter
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
        if self.milvus_store:
            return f"Milvus store already initialized at {self.milvus_store.uri}, skipping initialization"
        
        connections.connect(host=self.milvus_host_IP, port=self.milvus_port)
        
        # Check if the collection already exists
        if utility.has_collection(self.collection_name):
            print(f"Milvus collection '{self.collection_name}' already exists. Reusing the existing collection.")
            self.milvus_store = MilvusVectorStore(
                collection_name=self.collection_name,
                uri=f"http://{self.milvus_host_IP}:{self.milvus_port}/",
                overwrite=False  # Avoid overwriting the existing collection
            )
        else:
            # Initialize a new collection if it does not exist
            print(f"Milvus collection '{self.collection_name}' does not exist. Initializing a new collection.")
            self.milvus_store = MilvusVectorStore(
                dim=dim,
                collection_name=self.collection_name,
                uri=f"http://{self.milvus_host_IP}:{self.milvus_port}/",
                overwrite=True  
            )
        
        print(f"Initialized Milvus store at {self.milvus_store.uri} with {self.milvus_store.dim} dimensions")
        
   
    def reset_milvus_store(self):
        """
        Resets the milvus store by dropping the collection and recreating empty collection
        """
        connections.connect(host=self.milvus_host_IP, port=self.milvus_port)

        if self.milvus_store:
            collection = Collection(name=self.milvus_store.collection_name)
            collection.drop()
            print(f"Deleted {self.milvus_store.collection_name} from milvus store, please re-run the indexing pipeline")
            self.milvus_store = None

        else:
            print("No collection found in the milvus store")
        
    
    def run(self, path: List[str]) -> VectorStoreIndex:
        """
        Runs the indexing pipeline to index the documents

        Args:
            path (List[str]): List of paths to the files (pdf)

        Returns:
            VectorStoreIndex: VectorStoreIndex object containing the indexed documents
        """
        documents = self.read_document(path)
        chunks = self.chunk_document(documents, chunk_size=self.chunk_size)

        # Initialize Milvus store based on the embedding model
        if not self.milvus_store:
            if self.embed_model == "NV-Embed-QA":
                self.initialize_milvus_store(dim=512)
            elif self.embed_model == "text-embedding-ada-002":
                self.initialize_milvus_store(dim=1536)

        # Initialize storage context with Milvus vector store
        storage_context = StorageContext.from_defaults(vector_store=self.milvus_store)

        # Add documents (or chunks) to the index
        index = VectorStoreIndex.from_documents(
            [Document(text=chunk.text) for chunk in chunks], storage_context=storage_context, embed_model=self.embedder
        )

        print(f"Indexed {len(chunks)} chunks into Milvus.")
        return index





        

