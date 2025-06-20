import pandas as pd
import json
import yaml
import os
import logging # Added logging

# Assuming blob_storage.py is in the same src directory or accessible in PYTHONPATH
try:
    from .blob_storage import BlobStorageManager 
except ImportError: # Fallback for running script directly if not part of a package
    from blob_storage import BlobStorageManager


# Use logging instead of print for messages
logger = logging.getLogger(__name__)

def load_config(config_path="config/settings.yaml"): # This is mostly for standalone execution
    """Loads configuration from a YAML file."""
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Error: Config file not found at {config_path}")
        return None
    except yaml.YAMLError as e:
        logger.error(f"Error parsing config file: {e}")
        return None


def preprocess_excel_data(df):
    """Preprocess the DataFrame obtained from Excel."""
    try:
        # Rename columns to remove leading/trailing spaces
        df.columns = df.columns.str.strip()

        # Expected columns - these should be consistent across all files
        expected_columns = [
            "Schema Name", 
            "Attributes Details", 
            "Data Type", 
            "Business Rules", 
            "Mandatory Field", 
            "Required from Source to have data populated", 
            "Primary Key", 
            "Required for Deployment Validation", 
            "Deployment Validation"
        ]
        
        # Verify all expected columns exist
        for expected_col in expected_columns:
            if expected_col not in df.columns:
                # Try a case-insensitive match as well, stripping spaces
                found_alt = any(expected_col.lower().strip() == actual_col.lower().strip() for actual_col in df.columns)
                if not found_alt:
                    raise ValueError(f"Required column '{expected_col}' not found in Excel sheet. Available columns: {list(df.columns)}")

        # Fill down the "Schema Name" category - assuming it's the first column conceptually
        # Find the actual "Schema Name" column name after stripping
        schema_col_actual_name = [col for col in df.columns if col.lower().strip() == "schema name"][0]
        df[schema_col_actual_name] = df[schema_col_actual_name].ffill()

        return df

    except Exception as e:
        logger.error(f"Error preprocessing Excel DataFrame: {e}")
        return None


def extract_rules_from_dataframe(df):
    """Extract rules from the cleaned dataframe."""
    try:
        # Standardize column name access after stripping in preprocess_excel_data
        column_map = {col.lower().strip(): col for col in df.columns}

        schema_col = column_map["schema name"]
        attribute_col = column_map["attributes details"]
        data_type_col = column_map["data type"]
        business_rules_col = column_map["business rules"]
        mandatory_field_col = column_map["mandatory field"]
        from_source_col = column_map["required from source to have data populated"]
        primary_key_col = column_map["primary key"]
        required_for_deployment_col = column_map["required for deployment validation"]
        deployment_validation_col = column_map["deployment validation"]
        
        type_mapping = {
            'datetime': 'datetime64[ns]', 'date': 'datetime64[ns]', 'timestamp': 'datetime64[ns]',
            'int': 'int64', 'integer': 'int64',
            'float': 'float64', 'decimal': 'float64',
            'boolean': 'bool', 'bool': 'bool',
            'string': 'string', 'text': 'string', 'varchar': 'string', 'char': 'string'
        }
        
        extracted_rules = {}
        
        for _, row in df.iterrows():
            schema_name = row[schema_col]
            attribute_name = row[attribute_col]
            
            if pd.isna(schema_name) and pd.isna(attribute_name):
                continue
            if pd.isna(attribute_name) or str(attribute_name).strip() == "":
                continue
                
            schema_key = str(schema_name).strip()
            if schema_key not in extracted_rules:
                extracted_rules[schema_key] = {"fields": {}}
            
            def is_yes(value):
                if pd.isna(value): return False
                return str(value).strip().lower() in ["yes", "y", "true", "1"]
            
            attribute_key = str(attribute_name).strip()
            
            raw_data_type = str(row[data_type_col]).strip().lower() if pd.notna(row[data_type_col]) else "string"
            raw_data_type = raw_data_type.split('(')[0].strip()
            data_type = type_mapping.get(raw_data_type, 'object') # Default to 'object' (pandas term for string) or 'string'
            
            business_rules = str(row[business_rules_col]).strip() if pd.notna(row[business_rules_col]) else ""
            
            extracted_rules[schema_key]["fields"][attribute_key] = {
                "data_type": data_type,
                "mandatory_field": is_yes(row[mandatory_field_col]),
                "from_source": is_yes(row[from_source_col]),
                "primary_key": is_yes(row[primary_key_col]),
                "required_for_deployment": is_yes(row[required_for_deployment_col]),
                "deployment_validation": is_yes(row[deployment_validation_col]),
                "business_rules": business_rules
            }
            
        return extracted_rules
    except Exception as e:
        logger.error(f"Error extracting rules: {e}", exc_info=True)
        return {}

def parse_excel(config):
    """Parses the Excel file (from local or blob) and extracts the rules."""
    excel_sheet_name = config.get("excel_sheet_name")
    if not excel_sheet_name:
        logger.error("Error: excel_sheet_name not found in config.")
        return None

    df = None
    if config.get("use_blob_storage"):
        logger.info("Attempting to read Excel from Azure Blob Storage.")
        excel_blob_filename = config.get("excel_blob_filename")
        if not excel_blob_filename:
            logger.error("Error: excel_blob_filename not specified in config while use_blob_storage is true.")
            return None
        
        # Assuming config/settings.yaml is the path from execution root
        # BlobStorageManager will load its necessary details (storage_name, container_name, folder)
        # from this config file.
        try:
            blob_manager = BlobStorageManager(config_path="config/settings.yaml")
            df = blob_manager.read_excel_from_blob(excel_blob_filename, excel_sheet_name)
            if df is None:
                logger.error(f"Failed to read Excel '{excel_blob_filename}' (sheet: '{excel_sheet_name}') from blob.")
                return None
            logger.info(f"Successfully read Excel '{excel_blob_filename}' from blob storage.")
        except Exception as e:
            logger.error(f"Error initializing BlobStorageManager or reading from blob: {e}")
            return None
    else:
        logger.info("Attempting to read Excel from local file system.")
        excel_file_path = config.get("excel_file")
        if not excel_file_path:
            logger.error("Error: excel_file (local path) not found in config.")
            return None
        if not os.path.exists(excel_file_path):
            logger.error(f"Error: Local Excel file not found at {excel_file_path}")
            return None
        try:
            df = pd.read_excel(excel_file_path, sheet_name=excel_sheet_name)
            logger.info(f"Successfully read Excel '{excel_file_path}' from local file system.")
        except Exception as e:
            logger.error(f"Error reading local Excel file {excel_file_path}: {e}")
            return None

    if df is not None:
        processed_df = preprocess_excel_data(df)
        if processed_df is not None:
            rules = extract_rules_from_dataframe(processed_df)
            return rules
    return None

def save_rules(rules, rules_output_config_key, config):
    """Saves the extracted rules to a JSON file (local or blob)."""
    # rules_output_config_key is the actual filepath for local, or base filename for blob
    output_filename = os.path.basename(rules_output_config_key) # e.g., "processed-rules.json"
    
    try:
        if config.get("use_blob_storage"):
            logger.info(f"Attempting to save rules to Azure Blob Storage as '{output_filename}'.")
            # BlobStorageManager will use 'folder' from its config to place the file.
            blob_manager = BlobStorageManager(config_path="config/settings.yaml")
            if blob_manager.upload_json_data(rules, output_filename, indent=4):
                blob_path = f"{blob_manager.folder}/{output_filename}" if blob_manager.folder else output_filename
                logger.info(f"✅ Rules extracted and saved to Azure Blob Storage: {blob_manager.container_name}/{blob_path}")
            else:
                logger.error(f"Failed to save rules to Azure Blob Storage as '{output_filename}'.")
        else:
            logger.info(f"Attempting to save rules to local file: '{rules_output_config_key}'.")
            # Ensure directory exists for local saving
            local_dir = os.path.dirname(rules_output_config_key)
            if local_dir:
                os.makedirs(local_dir, exist_ok=True)
            
            with open(rules_output_config_key, "w") as f:
                json.dump(rules, f, indent=4)
            logger.info(f"✅ Rules extracted and saved to {rules_output_config_key}")
    except Exception as e: # Catch any exception, including IOError
        logger.error(f"Error saving rules to '{output_filename}' (blob or local): {e}", exc_info=True)


if __name__ == "__main__":
    # This block is for standalone execution/testing of this script.
    # It assumes config/settings.yaml is accessible from the current working directory.
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Path relative to where this script might be run from, if not from project root
    # For robust standalone running, adjust this path or use an absolute path.
    # If script is in src/, and config is in ../config/, path would be "../config/settings.yaml"
    # However, app.py sets CWD, so "config/settings.yaml" might be fine if run via app.py
    cfg = load_config(config_path="config/settings.yaml") # Adjust if necessary for standalone
    if cfg is None:
        exit()

    rules_data = parse_excel(cfg)
    if rules_data:
        output_file_key = cfg.get("processed_rules_file")
        if output_file_key:
            save_rules(rules_data, output_file_key, cfg)
        else:
            logger.error("Error: processed_rules_file not found in config.")
