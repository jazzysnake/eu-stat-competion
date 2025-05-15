import asyncio
import logging
import pandas as pd

from dotenv import load_dotenv

import crawler
import gcs_utils
import site_finder
import fin_rep_finder
import genai_utils
import valkey_utils
from valkey_stores import AnnualReportLinkStore, ConversationStore, CompanySiteStore, ModelActionStore

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    df = pd.read_csv('disco_starting_kit/discovery.csv', sep=';')
    companies = df.drop_duplicates(subset=['ID'])['NAME']
    load_dotenv(override=True)
    try:
        logging.info("Initializing clients...")
        gen_client = genai_utils.GenaiClient.new(model=genai_utils.PRO)
        valkey_client = valkey_utils.ValkeyClient.new()
        gcs_uploader = gcs_utils.GCSBatchUploader.new(5)

        convo_store = ConversationStore(valkey_client)
        site_store = CompanySiteStore(valkey_client)
        report_link_store = AnnualReportLinkStore(valkey_client)
        model_action_store = ModelActionStore(valkey_client)

        simple_crawler = crawler.Crawler()

        sf = site_finder.SiteFinder(
            gen_client,
            convo_store,
            site_store,
            companies,
            concurrent_threads=10,
        )

        finfinder = fin_rep_finder.FinRepFinder(
            simple_crawler,
            gen_client,
            site_store,
            convo_store,
            model_action_store,
            report_link_store,
            report_download_directory='./pdf_downloads/',
            concurrent_threads=10,
        )

        logging.info("Clients initialized successfully.")
        await sf.run()
        await finfinder.run()
        gcs_uploader.upload_dir('./pdf_downloads')


        report_link_store.fill_solution_csv('./disco_starting_kit/discovery.csv')
        logging.info('Closing valkey connection...')
        valkey_client.close()
        await simple_crawler.close()
        logging.info('Exiting...')

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
