import httpx
import openai
import logging
import asyncio
import os
import yaml
import google.generativeai as genai
from azure.identity import DefaultAzureCredential


def initialize_llm(config):
    """Initializes the LLM client based on the configuration."""
    api_use = config.get("api_use", "Gemini")  # Default to Gemini if not specified

    try:
        if api_use.lower() == "gemini":
            return _initialize_gemini(config)
        elif api_use.lower() == "openai":
            return _initialize_openai(config)
        else:
            raise ValueError(f"Unsupported API specified: {api_use}. Must be 'Gemini' or 'OpenAI'.")
    except Exception as e:
        logging.error(f"Failed to initialize LLM: {str(e)}")
        raise




def _load_credentials():
    """Load credentials from config/settings.yaml file"""
    try:
        # Assuming this function is called from the src directory
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                 "config", "settings.yaml")
        
        with open(config_path, 'r') as file:
            settings = yaml.safe_load(file)
            
        # Return the credentials section or empty dict if not found
        return settings.get('credentials', {})
    except Exception as e:
        logging.error(f"Failed to load credentials from settings.yaml: {str(e)}")
        return {}

async def _get_access_token(config):
    """Gets the access token using client credentials flow."""
    try:
        # Load credentials from settings.yaml
        credentials = _load_credentials()
        
        auth_url = config.get("auth_url", "https://api.uhg.com/oauth2/token")
        scope = config.get("scope", "https://api.uhg.com/.default")
        grant_type = "client_credentials"
        
        # Prioritize credentials from config parameter, fallback to settings.yaml
        client_id = config.get("client_id") or credentials.get("client_id")
        client_secret = config.get("client_secret") or credentials.get("client_secret")
        
        if not client_id or not client_secret:
            raise ValueError("Client ID or Client Secret not found in config or settings.yaml")
        
        async with httpx.AsyncClient() as client:
            body = {
                "grant_type": grant_type,
                "scope": scope,
                "client_id": client_id,
                "client_secret": client_secret
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            resp = await client.post(auth_url, headers=headers, data=body, timeout=60)
            
            if resp.status_code != 200:
                raise ValueError(f"Authentication failed: {resp.text}")
            
            return resp.json()["access_token"]
    except Exception as e:
        logging.error(f"Failed to get access token: {str(e)}")
        raise

def _initialize_openai(config):
    """Initializes the OpenAI client with HTTPX authentication."""
    try:
        # Run the async function to get the token
        access_token = asyncio.run(_get_access_token(config))
        
        if not access_token:
            raise ValueError("Failed to obtain access token")

        # Store deployment name in client config for later use
        deployment_name = config.get("deployment_name", "gpt-4o_2024-05-13")
        
        # Define the Azure OpenAI endpoint and API version
        shared_quota_endpoint = config.get("azure_openai_endpoint", 
                                          "https://api.uhg.com/api/cloud/api-management/ai-gateway/1.0")
        azure_openai_api_version = config.get("openai_api_version", "2025-01-01-preview")

        # Initialize OpenAI client with Azure configuration
        oai_client = openai.AzureOpenAI(
            azure_endpoint=shared_quota_endpoint,
            api_version=azure_openai_api_version,
            azure_deployment=deployment_name,
            azure_ad_token=access_token,
            default_headers={
                "projectId": config.get("project_id", "0bef8880-4e98-413c-bc0b-41c280fd1b2a")
            }
        )
        
        # Store deployment name in the client object for later use
        oai_client.deployment_name = deployment_name
        return oai_client
    except Exception as e:
        logging.error(f"Failed to initialize OpenAI client: {str(e)}")
        raise
        
        
def generate_test_cases_with_llm(llm_client, prompt, max_output_tokens=1000):
    """Generates test cases using the appropriate LLM client."""
    try:
        if isinstance(llm_client, genai.GenerativeModel):
            response = llm_client.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    max_output_tokens=max_output_tokens
                )
            )
            if hasattr(response, 'text'):
                return response.text
            else:
                logging.error("Error: LLM Response missing 'text' attribute.")
                return None
        elif isinstance(llm_client, openai.AzureOpenAI):
            response = llm_client.chat.completions.create(
                model=llm_client.deployment_name,  # Using the stored deployment name
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_output_tokens
            )
            return response.choices[0].message.content
        else:
            raise ValueError("Unsupported LLM client type.")

    except Exception as e:
        logging.error(f"Exception in generate_test_cases_with_llm: {e}")
        return None