import os
import yaml
from src import parse_excel, enrich_rules , generate_test_cases , add_keys
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def load_config(config_path="config/settings.yaml"):
    """Loads configuration from a YAML file."""
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Error: Config file not found at {config_path}")
        return None
    except yaml.YAMLError as e:
        logging.error(f"Error parsing config file: {e}")
        return None


def main():
    """Main function to orchestrate the test automation process."""
    config = load_config()
    if config is None:
        exit()

    # # 1. Parse Excel and Extract Rules
    rules = parse_excel.parse_excel(config)
    if rules:
        parse_excel.save_rules(rules, config.get("processed_rules_file"))
    else:
        logging.error("Error: Failed to parse Excel and extract rules.")
        return

    # 2. Enrich Rules with Constraints
    # try:
    #     enrich_rules.enrich_rules(config)
    # except Exception as e:
    #     logging.error(f"Error during enrich_rules: {e}")
    #     return

    # # 3. Generate Test Cases
    try:
        generate_test_cases.main(config)
    except Exception as e:
        logging.error(f"Error during generate_test_cases: {e}")
        return

    # 4. Add Unique Keys
    try:
        add_keys.add_unique_keys(config["generated_test_cases_file"], config["test_case_keys_file"])
    except Exception as e:
        logging.error(f"Error during add_keys: {e}")
        return

if __name__ == "__main__":
    main()