import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY=os.getenv("OPENAI_API_KEY")

AZURE_OPENAI_API_KEY=os.getenv("AZURE_OPENAI_API_KEY")
AZURE_API_VERSION=os.getenv("AZURE_API_VERSION")
AZURE_OPENAI_ENDPOINT=os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT_NAME=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")

AZURE_AI_HUB_ENDPOINT=os.getenv("AZURE_AI_HUB_ENDPOINT")
AZURE_INFERENCE_CREDENTIAL=os.getenv("AZURE_INFERENCE_CREDENTIAL")

KAFKA_USERNAME=os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD=os.getenv("KAFKA_PASSWORD")
KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL=os.getenv("SCHEMA_REGISTRY_URL")

config = {
            "openai": {
                "api_key": OPENAI_API_KEY
            },
            "azure_ai_hub": {
                "endpoint": AZURE_AI_HUB_ENDPOINT,
                "credential": AZURE_INFERENCE_CREDENTIAL
            },
            "azure_openai": {
                "api_key": AZURE_OPENAI_API_KEY,
                "api_version": AZURE_API_VERSION,
                "azure_endpoint": AZURE_OPENAI_ENDPOINT,
                "deployment_name": AZURE_OPENAI_DEPLOYMENT_NAME
            },
            "kafka": {
                "sasl.username": KAFKA_USERNAME,
                "sasl.password": KAFKA_PASSWORD,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "session.timeout.ms": "50000"
            },
            "schema_registry": {
                "url": SCHEMA_REGISTRY_URL,
                "basic.auth.credentials.source": "",
                "basic.auth.user.info": f"{KAFKA_USERNAME}:{KAFKA_PASSWORD}"
            }
        }
