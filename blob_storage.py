import os
import json
import logging
import yaml
from typing import Optional, Dict, Any, Union
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
import pandas as pd
from io import BytesIO, StringIO

class BlobStorageManager:
    """
    A utility class for managing Azure Blob Storage operations including
    reading Excel files and uploading various file types.
    """
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        """
        Initialize the BlobStorageManager with configuration.
        
        Args:
            config_path: Path to the configuration YAML file
        """
        self.config = self._load_config(config_path)
        self.credential = DefaultAzureCredential()
        
        # Extract blob storage configuration
        self.storage_account_name = self.config.get("storage_name")
        self.container_name = self.config.get("container_name")
        self.folder = self.config.get("folder", "")
        
        if not self.storage_account_name or not self.container_name:
            raise ValueError("storage_name and container_name must be specified in config")
        
        # Initialize blob service client
        self.account_url = f"https://{self.storage_account_name}.blob.core.windows.net/"
        self.blob_service_client = BlobServiceClient(
            account_url=self.account_url,
            credential=self.credential
        )
        
        logging.info(f"BlobStorageManager initialized for account: {self.storage_account_name}")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            logging.error(f"Failed to load config from {config_path}: {str(e)}")
            raise

    def _get_blob_name(self, filename: str) -> str:
        """
        Construct the full blob name including folder path.
        
        Args:
            filename: The name of the file
            
        Returns:
            Full blob path including folder
        """
        if self.folder:
            return f"{self.folder}/{filename}"
        return filename

    def download_excel_file(self, excel_filename: str, local_path: str) -> bool:
        """
        Download an Excel file from blob storage to local path.
        
        Args:
            excel_filename: Name of the Excel file in blob storage
            local_path: Local path where the file should be saved
            
        Returns:
            True if successful, False otherwise
        """
        try:
            blob_name = self._get_blob_name(excel_filename)
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            # Ensure local directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download the file
            with open(local_path, "wb") as download_file:
                download_data = blob_client.download_blob()
                download_file.write(download_data.readall())
            
            logging.info(f"Successfully downloaded {blob_name} to {local_path}")
            return True
            
        except ResourceNotFoundError:
            logging.error(f"Excel file {blob_name} not found in blob storage")
            return False
        except Exception as e:
            logging.error(f"Failed to download Excel file {excel_filename}: {str(e)}")
            return False

    def read_excel_from_blob(self, excel_filename: str, sheet_name: str) -> Optional[pd.DataFrame]:
        """
        Read Excel file directly from blob storage into a pandas DataFrame.
        
        Args:
            excel_filename: Name of the Excel file in blob storage
            sheet_name: Name of the sheet to read
            
        Returns:
            pandas DataFrame if successful, None otherwise
        """
        try:
            blob_name = self._get_blob_name(excel_filename)
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            # Download blob content to memory
            blob_data = blob_client.download_blob()
            excel_data = BytesIO(blob_data.readall())
            
            # Read Excel file from memory
            df = pd.read_excel(excel_data, sheet_name=sheet_name)
            logging.info(f"Successfully read Excel file {blob_name}, sheet '{sheet_name}' from blob storage")
            return df
            
        except ResourceNotFoundError:
            logging.error(f"Excel file {blob_name} not found in blob storage")
            return None
        except Exception as e:
            logging.error(f"Failed to read Excel file {excel_filename} from blob: {str(e)}")
            return None

    def upload_file(self, local_file_path: str, blob_filename: Optional[str] = None, overwrite: bool = True) -> bool:
        """
        Upload a file to blob storage.
        
        Args:
            local_file_path: Path to the local file to upload
            blob_filename: Name for the file in blob storage (if None, uses local filename)
            overwrite: Whether to overwrite existing files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not os.path.exists(local_file_path):
                logging.error(f"Local file {local_file_path} does not exist")
                return False
            
            # Use local filename if blob_filename not provided
            if blob_filename is None:
                blob_filename = os.path.basename(local_file_path)
            
            blob_name = self._get_blob_name(blob_filename)
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            # Upload the file
            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=overwrite)
            
            blob_url = f"{self.account_url}{self.container_name}/{blob_name}"
            logging.info(f"Successfully uploaded {local_file_path} to blob storage at: {blob_url}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to upload file {local_file_path}: {str(e)}")
            return False

    def upload_data(self, data: Union[str, bytes], blob_filename: str, overwrite: bool = True) -> bool:
        """
        Upload data directly to blob storage without creating a local file.
        
        Args:
            data: String or bytes data to upload
            blob_filename: Name for the file in blob storage
            overwrite: Whether to overwrite existing files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            blob_name = self._get_blob_name(blob_filename)
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            # Convert string to bytes if necessary
            if isinstance(data, str):
                data = data.encode('utf-8')
            
            # Upload the data
            blob_client.upload_blob(data, overwrite=overwrite)
            
            blob_url = f"{self.account_url}{self.container_name}/{blob_name}"
            logging.info(f"Successfully uploaded data to blob storage at: {blob_url}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to upload data to {blob_filename}: {str(e)}")
            return False

    def upload_json_data(self, json_data: Dict[str, Any], blob_filename: str, overwrite: bool = True, indent: int = 4) -> bool:
        """
        Upload JSON data directly to blob storage without creating a local file.
        
        Args:
            json_data: Dictionary or JSON-serializable data
            blob_filename: Name for the JSON file in blob storage
            overwrite: Whether to overwrite existing files
            indent: JSON formatting indent
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert data to JSON string
            json_string = json.dumps(json_data, indent=indent)
            
            # Upload using upload_data method
            return self.upload_data(json_string, blob_filename, overwrite)
            
        except Exception as e:
            logging.error(f"Failed to upload JSON data to {blob_filename}: {str(e)}")
            return False

    def download_json_data(self, blob_filename: str) -> Optional[Dict[str, Any]]:
        """
        Download and parse JSON data from blob storage.
        
        Args:
            blob_filename: Name of the JSON file in blob storage
            
        Returns:
            Parsed JSON data as dictionary, None if failed
        """
        try:
            blob_name = self._get_blob_name(blob_filename)
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            # Download blob content
            blob_data = blob_client.download_blob()
            json_string = blob_data.readall().decode('utf-8')
            
            # Parse JSON
            json_data = json.loads(json_string)
            logging.info(f"Successfully downloaded and parsed JSON from {blob_name}")
            return json_data
            
        except ResourceNotFoundError:
            logging.error(f"JSON file {blob_name} not found in blob storage")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON from {blob_filename}: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Failed to download JSON from {blob_filename}: {str(e)}")
            return None

    def upload_multiple_files(self, file_paths: list, overwrite: bool = True) -> Dict[str, bool]:
        """
        Upload multiple files to blob storage.
        
        Args:
            file_paths: List of local file paths to upload
            overwrite: Whether to overwrite existing files
            
        Returns:
            Dictionary mapping file paths to success status
        """
        results = {}
        for file_path in file_paths:
            results[file_path] = self.upload_file(file_path, overwrite=overwrite)
        return results

    def upload_json_file(self, local_json_path: str, blob_filename: Optional[str] = None, overwrite: bool = True) -> bool:
        """
        Upload a JSON file to blob storage.
        
        Args:
            local_json_path: Path to the local JSON file
            blob_filename: Name for the file in blob storage
            overwrite: Whether to overwrite existing files
            
        Returns:
            True if successful, False otherwise
        """
        return self.upload_file(local_json_path, blob_filename, overwrite)

    def upload_csv_file(self, local_csv_path: str, blob_filename: Optional[str] = None, overwrite: bool = True) -> bool:
        """
        Upload a CSV file to blob storage.
        
        Args:
            local_csv_path: Path to the local CSV file
            blob_filename: Name for the file in blob storage
            overwrite: Whether to overwrite existing files
            
        Returns:
            True if successful, False otherwise
        """
        return self.upload_file(local_csv_path, blob_filename, overwrite)

    def list_blobs(self, prefix: Optional[str] = None) -> list:
        """
        List all blobs in the container with optional prefix filter.
        
        Args:
            prefix: Optional prefix to filter blobs
            
        Returns:
            List of blob names
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            
            if prefix:
                blob_list = container_client.list_blobs(name_starts_with=prefix)
            else:
                blob_list = container_client.list_blobs()
            
            return [blob.name for blob in blob_list]
            
        except Exception as e:
            logging.error(f"Failed to list blobs: {str(e)}")
            return []

    def delete_blob(self, blob_filename: str) -> bool:
        """
        Delete a blob from storage.
        
        Args:
            blob_filename: Name of the blob to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            blob_name = self._get_blob_name(blob_filename)
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            blob_client.delete_blob()
            logging.info(f"Successfully deleted blob: {blob_name}")
            return True
            
        except ResourceNotFoundError:
            logging.warning(f"Blob {blob_name} not found (already deleted?)")
            return True
        except Exception as e:
            logging.error(f"Failed to delete blob {blob_filename}: {str(e)}")
            return False

    def blob_exists(self, blob_filename: str) -> bool:
        """
        Check if a blob exists in storage.
        
        Args:
            blob_filename: Name of the blob to check
            
        Returns:
            True if blob exists, False otherwise
        """
        try:
            blob_name = self._get_blob_name(blob_filename)
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            return blob_client.exists()
            
        except Exception as e:
            logging.error(f"Error checking blob existence {blob_filename}: {str(e)}")
            return False

    def get_blob_url(self, blob_filename: str) -> str:
        """
        Get the full URL of a blob.
        
        Args:
            blob_filename: Name of the blob
            
        Returns:
            Full URL to the blob
        """
        blob_name = self._get_blob_name(blob_filename)
        return f"{self.account_url}{self.container_name}/{blob_name}"


# Convenience functions for easy import and use
def create_blob_manager(config_path: str = "config/settings.yaml") -> BlobStorageManager:
    """
    Create and return a BlobStorageManager instance.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        BlobStorageManager instance
    """
    return BlobStorageManager(config_path)


def upload_generated_files(local_files: list, config_path: str = "config/settings.yaml") -> Dict[str, bool]:
    """
    Convenience function to upload multiple generated files to blob storage.
    
    Args:
        local_files: List of local file paths to upload
        config_path: Path to configuration file
        
    Returns:
        Dictionary mapping file paths to success status
    """
    blob_manager = create_blob_manager(config_path)
    return blob_manager.upload_multiple_files(local_files)


def upload_json_to_blob(json_data: Dict[str, Any], filename: str, config_path: str = "config/settings.yaml") -> bool:
    """
    Convenience function to upload JSON data directly to blob storage.
    
    Args:
        json_data: Dictionary or JSON-serializable data
        filename: Name for the JSON file in blob storage
        config_path: Path to configuration file
        
    Returns:
        True if successful, False otherwise
    """
    blob_manager = create_blob_manager(config_path)
    return blob_manager.upload_json_data(json_data, filename)


def download_json_from_blob(filename: str, config_path: str = "config/settings.yaml") -> Optional[Dict[str, Any]]:
    """
    Convenience function to download and parse JSON data from blob storage.
    
    Args:
        filename: Name of the JSON file in blob storage
        config_path: Path to configuration file
        
    Returns:
        Parsed JSON data as dictionary, None if failed
    """
    blob_manager = create_blob_manager(config_path)
    return blob_manager.download_json_data(filename)