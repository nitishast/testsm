import pandas as pd
import json
import yaml

def load_config(config_path="config/settings.yaml"):
    """Loads configuration from a YAML file."""
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found at {config_path}")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing config file: {e}")
        return None


def preprocess_excel(file_path, sheet_name):
    """Preprocess the Excel file."""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)

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
        
        # Verify all expected columns exist (with some flexibility for whitespace)
        for expected_col in expected_columns:
            found = False
            for col in df.columns:
                if expected_col.lower().strip() == col.lower().strip():
                    found = True
                    break
            if not found:
                raise ValueError(f"Required column '{expected_col}' not found in Excel sheet")

        # Fill down the "Schema Name" category
        schema_col = df.columns[0]  # Schema Name is always the first column
        df[schema_col] = df[schema_col].ffill()

        return df

    except Exception as e:
        print(f"Error preprocessing Excel file: {e}")
        return None

def extract_rules_from_dataframe(df):
    """Extract rules from the cleaned dataframe."""
    try:
        # Get column names - using exact header names from Excel
        schema_col = "Schema Name"
        attribute_col = "Attributes Details"
        data_type_col = "Data Type"
        business_rules_col = "Business Rules"
        mandatory_field_col = "Mandatory Field"
        from_source_col = "Required from Source to have data populated"
        primary_key_col = "Primary Key"
        required_for_deployment_col = "Required for Deployment Validation"
        deployment_validation_col = "Deployment Validation"
        
        # Mapping for data type standardization
        type_mapping = {
            'datetime': 'datetime64[ns]',
            'date': 'datetime64[ns]',
            'timestamp': 'datetime64[ns]',
            'int': 'int64',
            'integer': 'int64',
            'float': 'float64',
            'decimal': 'float64',
            'boolean': 'bool',
            'bool': 'bool',
            'string': 'string',
            'text': 'string'
        }
        
        extracted_rules = {}
        
        for _, row in df.iterrows():
            schema_name = row[schema_col]
            attribute_name = row[attribute_col]
            
            # Skip rows with missing schema and attribute
            if pd.isna(schema_name) and pd.isna(attribute_name):
                continue
                
            # Skip rows with missing attribute name
            if pd.isna(attribute_name) or str(attribute_name).strip() == "":
                continue
                
            # Ensure schema exists in output dictionary
            schema_key = str(schema_name).strip()
            if schema_key not in extracted_rules:
                extracted_rules[schema_key] = {"fields": {}}
            
            # Helper function to handle Yes/No fields
            def is_yes(value):
                if pd.isna(value):
                    return False
                value_str = str(value).strip().lower()
                return value_str in ["yes", "y", "true", "1"]
            
            # Process attribute data
            attribute_key = str(attribute_name).strip()
            
            # Standardize data type
            raw_data_type = str(row[data_type_col]).strip().lower() if pd.notna(row[data_type_col]) else "string"
            # Remove any additional qualifiers like (10,2) for decimals
            raw_data_type = raw_data_type.split('(')[0].strip()
            
            # Map to standardized type, default to object (string)
            data_type = type_mapping.get(raw_data_type, 'object')
            
            business_rules = str(row[business_rules_col]).strip() if pd.notna(row[business_rules_col]) else ""
            
            # Build field object
            extracted_rules[schema_key]["fields"][attribute_key] = {
                "data_type": data_type,
                # "original_data_type": raw_data_type,  # Keep original type for reference
                "mandatory_field": is_yes(row[mandatory_field_col]),
                "from_source": is_yes(row[from_source_col]),
                "primary_key": is_yes(row[primary_key_col]),
                "required_for_deployment": is_yes(row[required_for_deployment_col]),
                "deployment_validation": is_yes(row[deployment_validation_col]),
                "business_rules": business_rules
            }
            
        return extracted_rules
    except Exception as e:
        print(f"Error extracting rules: {e}")
        return {}

def parse_excel(config):
    """Parses the Excel file and extracts the rules."""
    excel_file = config.get("excel_file")
    excel_sheet_name = config.get("excel_sheet_name")

    if not excel_file or not excel_sheet_name:
        print("Error: excel_file or excel_sheet_name not found in config.")
        return None

    df = preprocess_excel(excel_file, excel_sheet_name)

    if df is not None:
        rules = extract_rules_from_dataframe(df)
        return rules
    else:
        return None

def save_rules(rules, output_file):
    """Saves the extracted rules to a JSON file."""
    try:
        with open(output_file, "w") as f:
            json.dump(rules, f, indent=4)
        print(f"✅ Rules extracted and saved to {output_file}")
    except IOError as e:
        print(f"Error saving rules to {output_file}: {e}")

if __name__ == "__main__":
    config = load_config()
    if config is None:
        exit()

    rules = parse_excel(config)
    if rules:
        output_file = config.get("processed_rules_file")
        if output_file:
            save_rules(rules, output_file)
        else:
            print("Error: processed_rules_file not found in config.")
