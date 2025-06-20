import unittest
from unittest.mock import patch, mock_open, MagicMock
import json
import os
import pandas as pd
from io import StringIO

# Important: Adjust the import path based on your project structure.
# If generate_test_cases is in src, and tests are in a 'tests' folder at root:
# from src.generate_test_cases import TestCaseGenerator, main as generate_main
# For simplicity, assuming it's discoverable or paths are adjusted.
# Here, using a direct-like import assuming test file might be alongside or PYTHONPATH configured.
try:
    from generate_test_cases import TestCaseGenerator, main as generate_main
    from blob_storage import BlobStorageManager # If it's a top-level module
except ImportError:
    # This might happen if 'src' is not in PYTHONPATH when running tests.
    # You might need to add `sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))`
    # or configure your test runner (e.g. pytest with __init__.py in src and tests)
    # For now, let's assume it's directly importable from src:
    from src.generate_test_cases import TestCaseGenerator, main as generate_main
    from src.blob_storage import BlobStorageManager


class TestGenerateTestCases(unittest.TestCase):

    def setUp(self):
        self.base_config = {
            "use_blob_storage": False,
            "excel_file": "dummy_data/dummy_excel.xlsx",
            "excel_sheet_name": "Sheet1",
            "processed_rules_file": "dummy_data/processed-rules.json",
            "generated_test_cases_file": "dummy_data/generated-test-cases.json",
            "max_output_tokens": 100,
            "storage_name": "teststorage",
            "container_name": "testcontainer",
            "folder": "testfolder",
            "excel_blob_filename": "dummy_excel.xlsx",
            # LLM related configs (can be minimal as LLM is mocked)
            "api_use": "openai",
            "credentials": {"client_id": "test_id", "client_secret": "test_secret"},
            "openai": {"deployment_name": "test_dep", "api_version": "test_v", "endpoint": "test_ep", "project_id": "test_proj"}
        }
        self.sample_rules = {
            "Schema1": {
                "fields": {
                    "Field1": {
                        "data_type": "string", "mandatory_field": True, 
                        "primary_key": False, "business_rules": "Rule1"
                    },
                    "Field2": {
                        "data_type": "datetime64[ns]", "mandatory_field": False,
                        "primary_key": False, "business_rules": "Date field"
                    }
                }
            }
        }
        self.llm_response_valid = """
        [
            {
                "test_case": "TC001_Valid_String",
                "description": "Valid string input",
                "expected_result": "Pass",
                "input": "hello"
            }
        ]
        """
        self.llm_response_date_valid = """
        [
            {
                "test_case": "TC001_Valid_Date",
                "description": "Valid date input",
                "expected_result": "Pass",
                "input": "2023-01-01 10:00:00.000"
            },
            {
                "test_case": "TC002_Null_Date",
                "description": "Null date input for non-mandatory",
                "expected_result": "Pass",
                "input": null
            }
        ]
        """

    @patch('src.generate_test_cases.llm.initialize_llm')
    @patch('src.generate_test_cases.llm.generate_test_cases_with_llm')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists', return_value=True)
    @patch('os.rename')
    @patch('os.makedirs')
    @patch('json.dump')
    @patch('csv.writer') # Mock the csv.writer related calls
    def test_generate_local_files(self, mock_csv_writer, mock_json_dump, mock_makedirs, mock_rename, mock_exists,
                                  mock_file_open, mock_llm_generate, mock_llm_init):
        
        mock_llm_init.return_value = MagicMock() # Mock LLM client
        # Side effect for LLM responses: first for Field1 (string), then for Field2 (date)
        mock_llm_generate.side_effect = [self.llm_response_valid, self.llm_response_date_valid]

        # Mock reading processed-rules.json
        mock_file_open.side_effect = [
            mock_open(read_data=json.dumps(self.sample_rules)).return_value,  # For reading rules
            mock_open().return_value,  # For writing JSON output
            mock_open().return_value   # For writing CSV output
        ]

        config = self.base_config.copy()
        config["use_blob_storage"] = False
        
        # Call the main function of generate_test_cases.py
        generate_main(config)

        # Assertions
        mock_llm_init.assert_called_once_with(config)
        self.assertEqual(mock_llm_generate.call_count, 2) # Called for two fields

        # Check if rules file was opened for reading
        self.assertIn(config["processed_rules_file"], [call_args[0][0] for call_args in mock_file_open.call_args_list if call_args[0][1] == 'r'])
        
        # Check if output JSON was attempted to be written
        mock_json_dump.assert_called()
        saved_json_data = mock_json_dump.call_args[0][0]
        self.assertIn("Schema1.Field1", saved_json_data)
        self.assertIn("Schema1.Field2", saved_json_data)
        self.assertEqual(len(saved_json_data["Schema1.Field1"]), 1)
        self.assertEqual(len(saved_json_data["Schema1.Field2"]), 2)

        # Check if output CSV was attempted to be written
        # mock_csv_writer instance is what csv.writer(file_handle) returns
        # We need to check calls on the writer object itself (writerow)
        # The mock_csv_writer here is the constructor. We need to check the instance.
        # This part is a bit tricky to assert perfectly without more complex mock setup for csv.writer
        self.assertTrue(any(config["generated_test_cases_file"].replace(".json", ".csv") in call_args[0][0] for call_args in mock_file_open.call_args_list if call_args[0][1] == 'w' or call_args[0][1] == 'w+'))


    @patch('src.generate_test_cases.llm.initialize_llm')
    @patch('src.generate_test_cases.llm.generate_test_cases_with_llm')
    @patch('src.generate_test_cases.BlobStorageManager') # Mock the BlobStorageManager class
    def test_generate_blob_files(self, MockBlobStorageManager, mock_llm_generate, mock_llm_init):
        mock_llm_init.return_value = MagicMock()
        mock_llm_generate.side_effect = [self.llm_response_valid, self.llm_response_date_valid]

        # Setup mock instance for BlobStorageManager
        mock_blob_instance = MockBlobStorageManager.return_value
        mock_blob_instance.download_json_data.return_value = self.sample_rules # Simulate reading rules from blob
        mock_blob_instance.upload_json_data.return_value = True # Simulate successful JSON upload
        mock_blob_instance.upload_data.return_value = True # Simulate successful CSV upload
        # Mock folder and container_name for logging assertions (optional)
        mock_blob_instance.folder = "testfolder"
        mock_blob_instance.container_name = "testcontainer"


        config = self.base_config.copy()
        config["use_blob_storage"] = True
        
        generate_main(config)

        # Assertions
        mock_llm_init.assert_called_once_with(config)
        self.assertEqual(mock_llm_generate.call_count, 2)
        
        # Check BlobStorageManager instantiation and calls
        MockBlobStorageManager.assert_called_with(config_path="config/settings.yaml") # Called multiple times
        
        # Check download rules
        rules_base_filename = os.path.basename(config["processed_rules_file"])
        mock_blob_instance.download_json_data.assert_called_with(rules_base_filename)

        # Check upload JSON
        generated_json_base_filename = os.path.basename(config["generated_test_cases_file"])
        mock_blob_instance.upload_json_data.assert_called_with(unittest.mock.ANY, generated_json_base_filename, indent=2)
        saved_json_data_blob = mock_blob_instance.upload_json_data.call_args[0][0]
        self.assertIn("Schema1.Field1", saved_json_data_blob)
        self.assertEqual(len(saved_json_data_blob["Schema1.Field1"]), 1)

        # Check upload CSV
        generated_csv_base_filename = generated_json_base_filename.replace(".json", ".csv")
        mock_blob_instance.upload_data.assert_called_with(unittest.mock.ANY, generated_csv_base_filename)
        # Verify CSV content (first arg to upload_data)
        csv_content_arg = mock_blob_instance.upload_data.call_args[0][0]
        self.assertIn("SchemaName,FieldName,Test Case,Description,Expected Result,Input", csv_content_arg)
        self.assertIn("Schema1,Field1,TC001_Valid_String", csv_content_arg) # Check some data
        self.assertIn("Schema1,Field2,TC001_Valid_Date", csv_content_arg) # Check some data


    def test_parse_llm_response_valid(self):
        config = self.base_config.copy()
        generator = TestCaseGenerator(config=config)
        parsed = generator._parse_llm_response(self.llm_response_valid, "string")
        self.assertIsNotNone(parsed)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["test_case"], "TC001_Valid_String")

    def test_parse_llm_response_invalid_json(self):
        config = self.base_config.copy()
        generator = TestCaseGenerator(config=config)
        invalid_json_response = "[{'test_case': 'TC001'}]" # Single quotes are invalid JSON
        parsed = generator._parse_llm_response(invalid_json_response, "string")
        self.assertIsNone(parsed)

    def test_validate_test_case_date_format(self):
        config = self.base_config.copy()
        generator = TestCaseGenerator(config=config)
        
        # Valid date case
        valid_case = {"test_case": "T1", "description": "d", "expected_result": "Pass", "input": "2024-07-29 10:20:30.123"}
        is_valid, msg = generator._validate_test_case(valid_case, "datetime64[ns]") # data_type from rules
        self.assertTrue(is_valid, msg)

        # Invalid date format case
        invalid_format_case = {"test_case": "T2", "description": "d", "expected_result": "Pass", "input": "2024/07/29"}
        is_valid, msg = generator._validate_test_case(invalid_format_case, "datetime64[ns]")
        self.assertFalse(is_valid)
        self.assertIn("Invalid date format", msg)

        # Null input for date (should be valid regardless of format check if input is None)
        null_input_case = {"test_case": "T3", "description": "d", "expected_result": "Pass", "input": None}
        is_valid, msg = generator._validate_test_case(null_input_case, "datetime64[ns]")
        self.assertTrue(is_valid, msg)

    # You can add more tests for other private methods or edge cases.

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
