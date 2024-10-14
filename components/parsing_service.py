from cleantext import clean  # Meta Data Cleaning
from pathlib import Path  # File type retrieval

from langchain_community.document_loaders import PyMuPDFLoader  # PDF
from unstructured.partition.auto import partition  
from unstructured.staging.base import convert_to_dict  #



# Main Parsing Function
def parse_document(file_path) -> list[dict]:
    """
    Parses a PDF, removes irrelevant metadata, and returns the cleaned text.

    This function processes the document based on its file type, which is
    determined by the file extension, and follows these steps:

    - For PDF files: Extracts the raw text from the PDF and removes irrelevant
    metadata.

    Args:
        file_path (str): The path to the document to be parsed.

    Returns:
        cleaned_data (list[dict]): A list of dictionaries containing the
        cleaned data extracted from the document.
    """

    # Get file extension
    file_type = get_file_extension(file_path)

    # If file type is PDF, parse document. Currently only supports PDF.
    if file_type == "pdf":
        parsed_data = parse_with_PyMuPDF(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    # Remove the irrelevant metadata and clean parsed text by removing '/n' etc
    cleaned_data = clean_metadata(parsed_data, file_type)

    return cleaned_data


def parse_with_PyMuPDF(file_path) -> list[dict]:
    """
    Parses a PDF document using PyMuPDF and returns extracted text data.

    This function uses the `PyMuPDFLoader` to load a PDF document and then
    extracts text
    from the document. The extracted text is converted into a list of
    dictionaries, where each dictionary represents the content of a page.

    Args:
        file_path (str): The path to the PDF file that needs to be parsed.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary contains the
        extracted text data from a page of the PDF.
    """
    loader = PyMuPDFLoader(file_path)
    documents = loader.load()  # Returns document objects "Document[text: xx]"

    # This converts the document objects to JSON
    data = convert_documents_to_dict_list(documents, "PDF")
    return data


def parse_with_Unstructured(file_path) -> list[dict]:
    """
    Parses an unstructured document file and returns the extracted data as a
    dictionary.

    This function uses 'Unstructured' to process a document file of any type,
    extracting its elements. The extracted elements are then converted into a
    dictionary format.

    Args:
        file_path (str): The path to the document file that needs to be parsed.

    Returns:
        list[dict]: A dictionary containing the extracted data from the document.
    """
    elements = partition(filename=file_path)
    data = convert_to_dict(elements)
    return data


def get_file_extension(filename: str) -> str:
    """
    Extracts and returns the file extension from a given filename.

    This function takes a filename as input, uses `Path` from the `pathlib`
    module to extract the file extension, and returns the extension in
    lowercase without the leading dot (e.g., 'pdf', 'docx').

    Args:
        filename (str): The name of the file including its extension.

    Returns:
        str: The file extension in lowercase, without the leading dot
        (e.g., 'pdf').
    """
    return Path(filename).suffix.lower().lstrip(".")


def convert_documents_to_dict_list(documents, doc_type) -> list[dict]:
    """
    Converts a list of Document objects into a list of dictionaries based on
    the document type.

    This function processes each Document object in the `documents` list,
    converting them into dictionaries that combine metadata with content.
    The structure of the output dictionary depends on the `doc_type` provided:

    - For 'PDF' documents, the dictionary combines metadata with the
    `page_content` directly.

    Args:
        documents (list): A list of Document objects to be converted.

    Returns:
        list[dict]: A list of dictionaries where each dictionary represents a
        Document object with combined metadata and content.

    Raises:
        ValueError: If an unsupported document type is provided.
    """
    documents_dict_list = []

    for doc in documents:
        if doc_type.upper() == "PDF":
            # For PDF, directly combine metadata with page_content
            combined_dict = {
                "metadata": {**doc.metadata},
                "page_content": doc.page_content,
            }
        else:
            raise ValueError(f"Unsupported document type: {doc_type}")

        documents_dict_list.append(combined_dict)

    return documents_dict_list


def clean_metadata(parsed_data, file_type) -> list[dict]:
    """
    Cleans the parsed data by retaining only the necessary keys and adjusting
    the 'source' field for Word Doc files.

    Args:
        parsed_data (list[dict]): The original parsed data containing metadata
        and page content.
        file_type (str): The type of file parsed (e.g., 'pdf', '.docx', '.doc').

    Returns:
        list[dict]: A list of dictionaries with cleaned metadata and page
        content.
    """
    cleaned_data = []
    for item in parsed_data:

        # Clean metadata to remove markdown elements etc.
        cleaned_content = clean(
            item.get("page_content"), lower=False, no_line_breaks=True
        )

        if file_type == "pdf":
            cleaned_item = {
                "metadata": {
                    "source": item.get("metadata").get("source"),
                    "page": item.get("metadata").get("page"),
                    "total_pages": item.get("metadata").get("total_pages"),
                },
                "page_content": cleaned_content,
            }
        cleaned_data.append(cleaned_item)
    return cleaned_data
