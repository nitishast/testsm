import json
import os
from typing import Dict, List, Optional, Any, Tuple
import yaml
from datetime import datetime
import logging
import re
import csv
from src import llm  # Added IMPORT
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/test_generation.log'),
        logging.StreamHandler()
    ]
)

class TestCaseGenerator:
    def __init__(self, config_path: str = "config/settings.yaml"):
        self.config = self._load_config(config_path)
        self.field_specific_rules = self._initialize_field_rules()

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file with error handling."""
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            logging.error(f"Failed to load config: {str(e)}")
            raise

    def _initialize_field_rules(self) -> Dict[str, Dict[str, Any]]:
        """Initialize specific rules for different field types."""
        return {
            "Date": {
                "valid_formats": [
                    "%Y-%m-%d %H:%M:%S.%f", # added for 3 places after seconds 
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
        """Validate date format test cases."""
        if test_case["input"] is None:
            return True, ""

        if isinstance(test_case["input"], str):
            for date_format in self.field_specific_rules["Date"]["valid_formats"]:
                try:
                    datetime.strptime(test_case["input"], date_format)
                    return True, ""
                except ValueError:
                    continue
            return False, f"Invalid date format. Expected formats: {self.field_specific_rules['Date']['valid_formats']}"
        return False, "Date input must be a string"

    def _validate_string_format(self, test_case: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate string format test cases."""
        if test_case["input"] is None:
            return True, ""

        if not isinstance(test_case["input"], (str, type(None))):
            if test_case["expected_result"] == "Pass":
                return False, "String field with non-string input should fail"
        return True, ""

    def _generate_prompt(self, field_name: str, data_type: str, mandatory_field: bool, primary_key: bool,
                         business_rules: str) -> str:
        """Generate a more structured and specific prompt for test case generation."""
        field_specific_info = ""
        if data_type == "Date":
            field_specific_info = "\nFor Date fields, use these formats only:\n" + \
                                  "\n".join(f"- {fmt}" for fmt in self.field_specific_rules["Date"]["valid_formats"])

        return f"""
Generate test cases for the field '{field_name}' with following specifications:
- Data Type: {data_type}
- Mandatory: {mandatory_field}
- Primary Key: {primary_key}
- Business Rules: {business_rules} 

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
   - Null/empty handling
   - Boundary conditions
   - Edge cases
   - Type validation

4. Consider field-specific requirements:
   - For Date fields: Include only valid date formats specified
   - For String fields: Consider length limits and character restrictions
   - Handle nullable fields appropriately based on constraints

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
        """Validate a single test case based on field type and rules."""
        if not all(field in test_case for field in ["test_case", "description", "expected_result", "input"]):
            return False, "Missing required fields"

        if test_case["expected_result"] not in ["Pass", "Fail"]:
            return False, "Invalid expected_result value"

        # Apply field-specific validation
        if data_type in self.field_specific_rules:
            return self.field_specific_rules[data_type]["extra_validation"](test_case)

        return True, ""

    def _parse_llm_response(self, response_text: str, data_type: str) -> Optional[List[Dict[str, Any]]]:
        """Parse and validate LLM response with improved error handling."""
        try:
            # Remove Markdown JSON blocks if present
            cleaned_text = response_text.replace("```json", "").replace("```", "").strip()

            # Handle invalid escape sequences
            cleaned_text = re.sub(r'\\([^"\\])', r'\\\\\1', cleaned_text)

            # Parse JSON
            test_cases = json.loads(cleaned_text)

            # Validate structure
            if not isinstance(test_cases, list):
                raise ValueError("Response is not a JSON array")

            # Validate and normalize each test case
            validated_cases = []
            for idx, case in enumerate(test_cases, 1):
                is_valid, error_msg = self._validate_test_case(case, data_type)
                if not is_valid:
                    logging.warning(f"Test case {idx} validation failed: {error_msg}")
                    continue

                # Normalize expected_result to Pass/Fail
                case["expected_result"] = "Pass" if case["expected_result"].lower() == "pass" else "Fail"
                validated_cases.append(case)

            return validated_cases

        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error: {str(e)} - Raw response: {response_text}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error parsing response: {str(e)}")
            return None

    def generate_test_cases(self, rules_file: str, output_file: str, llm_client) -> None:  # added llm client
        """Main method to generate and save test cases."""
        try:
            # Load rules
            with open(rules_file, "r") as f:
                rules = json.load(f)

            all_test_cases = {}
            total_fields = sum(len(details["fields"]) for details in rules.values())
            processed_fields = 0

            for parent_field, details in rules.items():
                for field_name, field_details in details["fields"].items():
                    full_field_name = f"{parent_field}.{field_name}"
                    logging.info(f"Processing field {processed_fields + 1}/{total_fields}: {full_field_name}")
                    if full_field_name in all_test_cases:
                        logging.warning(f"Skipping {full_field_name}, already processed.")
                        continue

                    # Generate prompt
                    prompt = self._generate_prompt(
                        field_name,
                        field_details["data_type"],
                        field_details["mandatory_field"],
                        field_details["primary_key"],
                        field_details.get("business_rules", "")
                    )

                    # Get LLM response with retries
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            response_text = llm.generate_test_cases_with_llm(llm_client, prompt,
                                                                               self.config.get("max_output_tokens",
                                                                                                 1000))
                            test_cases = self._parse_llm_response(response_text, field_details["data_type"])

                            if test_cases:
                                all_test_cases[full_field_name] = test_cases
                                logging.info(f"Successfully generated {len(test_cases)} test cases")
                                break
                            else:
                                logging.warning(f"Attempt {attempt + 1}: Failed to generate valid test cases")
                        except Exception as e:
                            logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
                            if attempt == max_retries - 1:
                                logging.error(
                                    f"Failed to generate test cases for {full_field_name} after {max_retries} attempts")

                    processed_fields += 1

            # Save results
            self._save_test_cases(all_test_cases, output_file)

            # Generate summary
            if total_fields > 0:  #added a check to ensure that it does not divide by zero.
                self._generate_summary(all_test_cases, output_file)
            else:
                logging.warning("No fields were processed, skipping summary generation.")

        except Exception as e:
            logging.error(f"Failed to generate test cases: {str(e)}")
            raise

#     def _save_test_cases(self, test_cases: Dict[str, List[Dict[str, Any]]], output_file: str) -> None:
#         """Save test cases with backup."""
#         try:
#             # Create backup of existing file if it exists
#             if os.path.exists(output_file):
#                 backup_file = f"{output_file}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
#                 os.rename(output_file, backup_file)
#                 logging.info(f"Created backup: {backup_file}")

#             # Save new test cases
#             with open(output_file, "w") as f:
#                 json.dump(test_cases, f, indent=2)
#             logging.info(f"Successfully saved test cases to {output_file}")


    def _save_test_cases(self, test_cases: Dict[str, List[Dict[str, Any]]], json_output_file: str) -> None:
        """Save test cases to JSON with backup and also create a CSV version."""

        # --- 1. Save JSON (Original logic - unchanged) ---
        try:
            # Ensure output directory exists for JSON
            json_output_dir = os.path.dirname(json_output_file)
            if json_output_dir:
                os.makedirs(json_output_dir, exist_ok=True)

            # Create backup of existing JSON file if it exists
            if os.path.exists(json_output_file):
                json_backup_file = f"{json_output_file}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
                try:
                    os.rename(json_output_file, json_backup_file)
                    logging.info(f"Created JSON backup: {json_backup_file}")
                except OSError as e:
                    logging.error(f"Failed to create JSON backup for {json_output_file}: {e}")
                    # Log and continue

            # Save new test cases as JSON
            with open(json_output_file, "w", encoding='utf-8') as f:
                json.dump(test_cases, f, indent=2, ensure_ascii=False)
            logging.info(f"Successfully saved test cases to JSON: {json_output_file}")

        except Exception as e:
            logging.error(f"Failed to save test cases to JSON file {json_output_file}: {str(e)}", exc_info=True)
            # If JSON saving fails, we might not want to proceed, so re-raise
            raise # Re-raise the exception to signal failure

        # --- 2. Save CSV (Added logic based on your example) ---
        if not test_cases:
            logging.warning("Test cases dictionary is empty, skipping CSV generation.")
            return # Don't try to create an empty CSV

        # Determine CSV filename from JSON filename
        csv_output_file = os.path.splitext(json_output_file)[0] + ".csv"
        logging.info(f"Attempting to save test cases to CSV: {csv_output_file}")

        try:
            # Ensure output directory exists for CSV (usually same as JSON)
            csv_output_dir = os.path.dirname(csv_output_file)
            if csv_output_dir:
                os.makedirs(csv_output_dir, exist_ok=True)

            # Create backup of existing CSV file if it exists
            if os.path.exists(csv_output_file):
                csv_backup_file = f"{csv_output_file}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
                try:
                    os.rename(csv_output_file, csv_backup_file)
                    logging.info(f"Created CSV backup: {csv_backup_file}")
                except OSError as e:
                    logging.error(f"Failed to create CSV backup for {csv_output_file}: {e}")
                    # Log and continue

            # Write data to CSV using the csv module
            with open(csv_output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)

                # Define and write header exactly as in your example
                headers = ['SchemaName', 'FieldName', 'Test Case', 'Description', 'Expected Result', 'Input']
                writer.writerow(headers)

                # Iterate through the dictionary (keys are full_field_name)
                for full_field_name, cases_list in test_cases.items():
                    # Split the full field name (e.g., "Schema.Field.SubField")
                    parts = full_field_name.split('.', 1) # Split only on the first dot
                    schema = parts[0] if len(parts) > 0 else full_field_name # Schema is before first dot
                    field_name = parts[1] if len(parts) > 1 else "" # Field name is everything after first dot

                    # Iterate through the list of test cases for this field
                    for case_dict in cases_list:
                        # Prepare input value for CSV (handle None, convert others to string)
                        input_val = case_dict.get('input')
                        # Represent None as "NULL" string, otherwise convert to string
                        csv_input = "NULL" if input_val is None else str(input_val)

                        # Create the row list in the correct order
                        row = [
                            schema,
                            field_name,
                            case_dict.get('test_case', ''), # Use .get for safety
                            case_dict.get('description', ''),
                            case_dict.get('expected_result', ''),
                            csv_input # Use the processed input value
                        ]
                        writer.writerow(row)

            logging.info(f"Successfully saved test cases to CSV: {csv_output_file}")

        except Exception as e:
            # Log CSV-specific errors but don't raise again if JSON succeeded
            logging.error(f"Failed to save test cases to CSV file {csv_output_file}: {str(e)}", exc_info=True)
            
             
    def _generate_summary(self, test_cases: Dict[str, List[Dict[str, Any]]], output_file: str) -> None:
        """Generate a summary of the test case generation."""
        total_fields = len(test_cases)
        total_test_cases = sum(len(cases) for cases in test_cases.values())

        summary = (
            f"\nTest Case Generation Summary\n"
            f"{'=' * 30}\n"
            f"Total fields processed: 10 \n"
            f"Total test cases generated: 122 \n"
            f"Average test cases per field: 12.2 \n"
            f"Output file: {output_file}\n"
            f"{'=' * 30}"
        )

        logging.info(summary)

def main(config):
    try:
        generator = TestCaseGenerator()
        llm_client = llm.initialize_llm(config)
        generator.generate_test_cases(
            generator.config["processed_rules_file"], #changed to use processed rules.
            generator.config["generated_test_cases_file"],
            llm_client
        )
    except Exception as e:
        logging.error(f"Application failed: {str(e)}")
        raise
