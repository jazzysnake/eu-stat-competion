import asyncio
import logging

import site_finder
import genai_utils
import valkey_utils
from llm_conversation_store import ConversationStore

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    try:
        logging.info("Initializing clients...")
        gen_client = genai_utils.GenaiClient.new(model=genai_utils.PRO)
        valkey_client = valkey_utils.ValkeyClient.new()
        convo_store = ConversationStore(valkey_client)
        logging.info("Clients initialized successfully.")

        company = 'ADECCO GROUP AG'
        logging.info(f"Starting site finding for: {company}")
        result = await site_finder.find_site(gen_client, convo_store, company)
        logging.info(f"Site finding completed for: {company}")
        print("\n--- Site Finding Result ---")
        print(result.model_dump_json(indent=2))
        print("-------------------------\n")

        valkey_client.close()

    except (valkey_utils.ConfigurationError, valkey_utils.ConnectionError) as db_err:
        logging.error(f"Database connection/configuration error: {db_err}", exc_info=True)
        print(f"Error: Failed to connect to or configure the database. Please check settings/logs. Details: {db_err}")
    except ValueError as env_err:
        logging.error(f"Environment configuration error: {env_err}", exc_info=True)
        print(f"Error: Configuration missing (e.g., GEMINI_API_TOKEN). Details: {env_err}")
    except genai_utils.GenerationError as gen_err:
        logging.error(f"AI content generation error: {gen_err}", exc_info=True)
        print(f"Error: Failed to generate content using the AI model. Details: {gen_err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())
