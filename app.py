import json
import os
from typing import Dict, List, Optional, Any, Tuple
import yaml
from datetime import datetime
import logging
import re
import csv
from io import StringIO # For in-memory CSV generation

# Assuming llm.py and blob_storage.py are in the same src directory or accessible in PYTHONPATH
try:
    from . import llm
    from .blob_storage import BlobStorageManager
except ImportError: # Fallback for running script directly
    import llm
    from blob_storage import BlobStorageManager


# Set up logging for this module
logger = logging.getLogger(__name__)
# Configure root logger if not already configured by app.py (e.g., for standalone run)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/test_generation.log'), # Creates logs dir if not exists
            logging.StreamHandler()
        ]
    )
    os.makedirs('logs', exist_ok=True)


class TestCaseGenerator:
    def __init__(self, config: Optional[Dict[str, Any]] = None, config_path: str = "config/settings.yaml"):
        if config:
            self.config = config
        else:
            # This load is mainly for standalone or direct instantiation without passing config
            self.config = self._load_config(config_path)
        
        if not self.config:
            raise ValueError("Configuration could not be loaded for TestCaseGenerator.")
            
        self.field_specific_rules = self._initialize_field_rules()

    def _load_config(self, config_path: str) -> Optional[dict]:
        """Load configuration from YAML file with error handling."""
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {str(e)}")
            return None # Changed from raise to return None, constructor handles missing config

    def _initialize_field_rules(self) -> Dict[str, Dict[str, Any]]:
        """Initialize specific rules for different field types."""
        return {
            "Date": {
                "valid_formats": [
                    "%Y-%m-%d %H:%M:%S.%f",
                    # "%Y-%m-%d %H:%M:%S",
                    # "%Y/%m/%d %H:%M:%S",
                    # "%m/%d/%Y %H:%M:%S"
                ],
                "extra_validation": self._validate_date_format
            },
            "String": {
                "extra_validation": self._validate_string_format
            }
        }

    def _validate_date_format(self, test_case: Dict[str, Any]) -> Tuple[bool, str]:
        if test_case["input"] is None: return True, ""
        if isinstance(test_case["input"], str):
            for date_format in self.field_specific_rules["Date"]["valid_formats"]:
                try:
                    datetime.strptime(test_case["input"], date_format)
                    return True, ""
                except ValueError: continue
            return False, f"Invalid date format. Expected formats: {self.field_specific_rules['Date']['valid_formats']}"
        return False, "Date input must be a string"

    def _validate_string_format(self, test_case: Dict[str, Any]) -> Tuple[bool, str]:
        if test_case["input"] is None: return True, ""
        if not isinstance(test_case["input"], (str, type(None))): # Allow None explicitly
            if test_case["expected_result"] == "Pass":
                return False, "String field with non-string input should fail if expected to Pass"
        return True, ""

    def _generate_prompt(self, field_name: str, data_type: str, mandatory_field: bool, primary_key: bool,
                         business_rules: str) -> str:
        field_specific_info = ""
        if data_type == "Date": # This should match the "data_type" from processed rules, e.g. "datetime64[ns]"
             # Let's check for 'date' in the type string for broader compatibility
            if "date" in data_type.lower() or "time" in data_type.lower():
                field_specific_info = "\nFor Date fields, use these formats only:\n" + \
                                    "\n".join(f"- {fmt}" for fmt in self.field_specific_rules["Date"]["valid_formats"])

        return f"""
Generate test cases for the field '{field_name}' with following specifications:
- Data Type: {data_type}
- Mandatory: {mandatory_field}
- Primary Key: {primary_key}
- Business Rules: {business_rules} {field_specific_info}

Requirements:
1. Include ONLY the JSON array of test cases in your response
2. Each test case must have these exact fields:
   - "test_case": A clear, unique identifier for the test
   - "description": Detailed explanation of what the test verifies
   - "expected_result": MUST be exactly "Pass" or "Fail"
   - "input": The test input value (can be null, string, number, etc.)

3. Include these types of test cases:
   - Basic valid inputs
   - Basic invalid inputs
   - Null/empty handling (consider mandatory status)
   - Boundary conditions
   - Edge cases
   - Type validation

4. Consider field-specific requirements:
   - For Date fields: Adhere to specified valid date formats. Use "input": null for null date tests.
   - For String fields: Consider length limits and character restrictions if mentioned in business rules.
   - Handle nullable fields appropriately based on constraints. A non-mandatory field can have null input and Pass.

Return the response in this exact format:
[
    {{
        "test_case": "TC001_Valid_Basic",
        "description": "Basic valid input test",
        "expected_result": "Pass",
        "input": "example"
    }}
]

IMPORTANT: Return ONLY the JSON array. No additional text or explanation."""

    def _validate_test_case(self, test_case: Dict[str, Any], data_type: str) -> Tuple[bool, str]:
        if not all(field in test_case for field in ["test_case", "description", "expected_result", "input"]):
            return False, "Missing required fields"
        if test_case["expected_result"] not in ["Pass", "Fail"]:
            return False, "Invalid expected_result value"
        
        # Check against self.field_specific_rules using the simplified type key like "Date" or "String"
        # The data_type from rules can be 'datetime64[ns]', 'string', 'int64', etc.
        # We need to map these to simpler keys if using field_specific_rules directly with complex types.
        simplified_type = None
        if "date" in data_type.lower() or "time" in data_type.lower():
            simplified_type = "Date"
        elif "string" in data_type.lower() or "object" in data_type.lower(): # object is pandas type for string
            simplified_type = "String"
        # Add other mappings as needed (e.g., "Integer", "Float", "Boolean")

        if simplified_type and simplified_type in self.field_specific_rules:
            return self.field_specific_rules[simplified_type]["extra_validation"](test_case)
        return True, ""

    def _parse_llm_response(self, response_text: str, data_type: str) -> Optional[List[Dict[str, Any]]]:
        try:
            cleaned_text = response_text.replace("```json", "").replace("```", "").strip()
            cleaned_text = re.sub(r'\\([^"\\])', r'\\\\\1', cleaned_text)
            test_cases = json.loads(cleaned_text)
            if not isinstance(test_cases, list): raise ValueError("Response is not a JSON array")

            validated_cases = []
            for idx, case in enumerate(test_cases, 1):
                is_valid, error_msg = self._validate_test_case(case, data_type)
                if not is_valid:
                    logger.warning(f"Test case {idx} validation failed: {error_msg}. Skipping. Case: {case}")
                    continue
                case["expected_result"] = "Pass" if case["expected_result"].lower() == "pass" else "Fail"
                validated_cases.append(case)
            return validated_cases
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {str(e)} - Raw response snippet: {response_text[:500]}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing LLM response: {str(e)}")
            return None

    def generate_test_cases(self, rules_file_config_key: str, output_file_config_key: str, llm_client_instance) -> None:
        rules = None
        rules_filepath_or_name = self.config[rules_file_config_key]

        if self.config.get("use_blob_storage"):
            logger.info(f"Attempting to load rules from Azure Blob Storage: {os.path.basename(rules_filepath_or_name)}.")
            try:
                blob_manager = BlobStorageManager(config_path="config/settings.yaml")
                rules_blob_filename = os.path.basename(rules_filepath_or_name)
                rules = blob_manager.download_json_data(rules_blob_filename)
                if rules is None:
                    logger.error(f"Failed to load rules from blob: {rules_blob_filename}")
                    return # Critical error, cannot proceed
            except Exception as e:
                logger.error(f"Error initializing BlobStorageManager or downloading rules from blob: {e}")
                return
        else:
            logger.info(f"Attempting to load rules from local file: {rules_filepath_or_name}.")
            if not os.path.exists(rules_filepath_or_name):
                logger.error(f"Local rules file not found: {rules_filepath_or_name}")
                return
            try:
                with open(rules_filepath_or_name, "r") as f:
                    rules = json.load(f)
            except Exception as e:
                logger.error(f"Error loading local rules file {rules_filepath_or_name}: {e}")
                return
        
        if not rules:
            logger.error("Rules could not be loaded. Aborting test case generation.")
            return

        all_test_cases = {}
        total_fields = sum(len(details["fields"]) for details in rules.values())
        processed_fields = 0

        for parent_field, details in rules.items():
            for field_name, field_details in details["fields"].items():
                full_field_name = f"{parent_field}.{field_name}"
                logger.info(f"Processing field {processed_fields + 1}/{total_fields}: {full_field_name}")
                
                prompt = self._generate_prompt(
                    field_name, field_details["data_type"],
                    field_details["mandatory_field"], field_details["primary_key"],
                    field_details.get("business_rules", "")
                )
                
                max_retries = 3; response_text = None; temp_test_cases = None
                for attempt in range(max_retries):
                    try:
                        response_text = llm.generate_test_cases_with_llm(
                            llm_client_instance, prompt, self.config.get("max_output_tokens", 1500) # Increased tokens
                        )
                        if response_text:
                            temp_test_cases = self._parse_llm_response(response_text, field_details["data_type"])
                            if temp_test_cases:
                                all_test_cases[full_field_name] = temp_test_cases
                                logger.info(f"Successfully generated {len(temp_test_cases)} test cases for {full_field_name}")
                                break # Success
                            else:
                                logger.warning(f"Attempt {attempt + 1} for {full_field_name}: Failed to parse valid test cases from LLM response.")
                        else:
                             logger.warning(f"Attempt {attempt + 1} for {full_field_name}: LLM returned empty response.")
                    except Exception as e:
                        logger.error(f"Attempt {attempt + 1} for {full_field_name} failed with LLM error: {str(e)}")
                    if attempt == max_retries - 1 and not temp_test_cases:
                        logger.error(f"Failed to generate test cases for {full_field_name} after {max_retries} attempts.")
                processed_fields += 1
        
        self._save_test_cases(all_test_cases, output_file_config_key)
        if total_fields > 0: self._generate_summary(all_test_cases, self.config[output_file_config_key])
        else: logger.warning("No fields processed, skipping summary.")

    def _save_test_cases(self, test_cases: Dict[str, List[Dict[str, Any]]], json_output_file_config_key: str) -> None:
        json_filepath_or_name = self.config[json_output_file_config_key]
        base_json_filename = os.path.basename(json_filepath_or_name)
        base_csv_filename = os.path.splitext(base_json_filename)[0] + ".csv"

        if self.config.get("use_blob_storage"):
            logger.info("Attempting to save test cases (JSON and CSV) to Azure Blob Storage.")
            try:
                blob_manager = BlobStorageManager(config_path="config/settings.yaml")
                
                # Save JSON to Blob
                if blob_manager.upload_json_data(test_cases, base_json_filename, indent=2):
                    blob_path = f"{blob_manager.folder}/{base_json_filename}" if blob_manager.folder else base_json_filename
                    logger.info(f"Successfully saved test cases to JSON in Azure Blob Storage: {blob_manager.container_name}/{blob_path}")
                else:
                    logger.error(f"Failed to save JSON test cases to Azure Blob Storage: {base_json_filename}")

                # Save CSV to Blob
                if not test_cases:
                    logger.warning("Test cases dictionary is empty, skipping CSV generation for blob.")
                else:
                    output_csv_string = StringIO()
                    writer = csv.writer(output_csv_string)
                    headers = ['SchemaName', 'FieldName', 'Test Case', 'Description', 'Expected Result', 'Input']
                    writer.writerow(headers)
                    for full_field_name, cases_list in test_cases.items():
                        parts = full_field_name.split('.', 1)
                        schema = parts[0] if len(parts) > 0 else full_field_name
                        field_name_part = parts[1] if len(parts) > 1 else ""
                        for case_dict in cases_list:
                            input_val = case_dict.get('input')
                            csv_input = "NULL" if input_val is None else str(input_val)
                            row = [schema, field_name_part, case_dict.get('test_case', ''),
                                   case_dict.get('description', ''), case_dict.get('expected_result', ''), csv_input]
                            writer.writerow(row)
                    
                    csv_data = output_csv_string.getvalue()
                    output_csv_string.close()

                    if blob_manager.upload_data(csv_data, base_csv_filename):
                        blob_path_csv = f"{blob_manager.folder}/{base_csv_filename}" if blob_manager.folder else base_csv_filename
                        logger.info(f"Successfully saved test cases to CSV in Azure Blob Storage: {blob_manager.container_name}/{blob_path_csv}")
                    else:
                        logger.error(f"Failed to save CSV test cases to Azure Blob Storage: {base_csv_filename}")
            except Exception as e:
                logger.error(f"Error during saving test cases to blob storage: {e}", exc_info=True)

        else: # Save locally
            logger.info("Attempting to save test cases (JSON and CSV) to local file system.")
            # JSON local save (existing logic)
            json_output_dir = os.path.dirname(json_filepath_or_name)
            if json_output_dir: os.makedirs(json_output_dir, exist_ok=True)
            if os.path.exists(json_filepath_or_name):
                json_backup_file = f"{json_filepath_or_name}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
                try: os.rename(json_filepath_or_name, json_backup_file); logger.info(f"Created JSON backup: {json_backup_file}")
                except OSError as e: logger.error(f"Failed to create JSON backup for {json_filepath_or_name}: {e}")
            try:
                with open(json_filepath_or_name, "w", encoding='utf-8') as f:
                    json.dump(test_cases, f, indent=2, ensure_ascii=False)
                logger.info(f"Successfully saved test cases to JSON: {json_filepath_or_name}")
            except Exception as e:
                logger.error(f"Failed to save test cases to JSON file {json_filepath_or_name}: {str(e)}", exc_info=True)
                raise # Re-raise for JSON, as it's primary

            # CSV local save (existing logic)
            if not test_cases:
                logger.warning("Test cases dictionary is empty, skipping local CSV generation.")
                return
            csv_output_file = os.path.splitext(json_filepath_or_name)[0] + ".csv"
            csv_output_dir = os.path.dirname(csv_output_file)
            if csv_output_dir: os.makedirs(csv_output_dir, exist_ok=True)
            if os.path.exists(csv_output_file):
                csv_backup_file = f"{csv_output_file}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
                try: os.rename(csv_output_file, csv_backup_file); logger.info(f"Created CSV backup: {csv_backup_file}")
                except OSError as e: logger.error(f"Failed to create CSV backup for {csv_output_file}: {e}")
            try:
                with open(csv_output_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    headers = ['SchemaName', 'FieldName', 'Test Case', 'Description', 'Expected Result', 'Input']
                    writer.writerow(headers)
                    for full_field_name, cases_list in test_cases.items():
                        parts = full_field_name.split('.', 1)
                        schema = parts[0] if len(parts) > 0 else full_field_name
                        field_name_part = parts[1] if len(parts) > 1 else ""
                        for case_dict in cases_list:
                            input_val = case_dict.get('input')
                            csv_input = "NULL" if input_val is None else str(input_val)
                            row = [schema, field_name_part, case_dict.get('test_case', ''),
                                   case_dict.get('description', ''), case_dict.get('expected_result', ''), csv_input]
                            writer.writerow(row)
                logger.info(f"Successfully saved test cases to CSV: {csv_output_file}")
            except Exception as e:
                logger.error(f"Failed to save test cases to CSV file {csv_output_file}: {str(e)}", exc_info=True)
             
    def _generate_summary(self, test_cases: Dict[str, List[Dict[str, Any]]], output_file_ref: str) -> None:
        total_fields_with_cases = len(test_cases)
        total_test_cases_generated = sum(len(cases) for cases in test_cases.values())
        avg_cases = total_test_cases_generated / total_fields_with_cases if total_fields_with_cases > 0 else 0
        
        location = "Azure Blob Storage" if self.config.get("use_blob_storage") else "Local File System"
        base_output_filename = os.path.basename(output_file_ref)

        summary = (
            f"\nTest Case Generation Summary\n"
            f"{'=' * 30}\n"
            f"Total fields processed for test case generation: {total_fields_with_cases}\n" # This reflects fields for which cases were actually stored
            f"Total test cases generated: {total_test_cases_generated}\n"
            f"Average test cases per field: {avg_cases:.1f}\n"
            f"Output files (JSON and CSV) base name: {os.path.splitext(base_output_filename)[0]}\n"
            f"Saved to: {location}\n"
        )
        if self.config.get("use_blob_storage"):
            blob_manager = BlobStorageManager(config_path="config/settings.yaml") # To get folder/container
            folder_path = blob_manager.folder if blob_manager.folder else "[root]"
            summary += f"Blob Location: {blob_manager.container_name}/{folder_path}/\n"
        else:
            summary += f"Local Path Base: {os.path.dirname(output_file_ref)}/\n"
        summary += f"{'=' * 30}"
        logger.info(summary)

def main(config_from_app): # Renamed to avoid confusion with module-level 'config'
    try:
        # Pass the config from app.py to the generator
        generator = TestCaseGenerator(config=config_from_app)
        llm_client = llm.initialize_llm(config_from_app) # Use app's config for LLM
        
        generator.generate_test_cases(
            # Use keys from the config_from_app for file paths/names
            "processed_rules_file", 
            "generated_test_cases_file",
            llm_client
        )
    except Exception as e:
        logger.error(f"Test case generation application failed: {str(e)}", exc_info=True)
        # Re-raise so app.py can catch it if needed, or for more visibility
        raise 

if __name__ == "__main__":
    # This block is for standalone execution/testing of this script.
    # It assumes config/settings.yaml is accessible.
    # The logger is configured at the top of the file for this case.
    
    # Path relative to script location for standalone run.
    # If script is in src/, and config is in ../config/, path would be "../config/settings.yaml"
    # For now, assumes "config/settings.yaml" from CWD.
    cfg_path = "config/settings.yaml" 
    
    temp_config = TestCaseGenerator()._load_config(cfg_path) # Temp load for main
    if temp_config:
        main(temp_config)
    else:
        logger.error(f"Failed to load configuration from {cfg_path} for standalone run.")
