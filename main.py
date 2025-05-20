import asyncio
import logging
import pandas as pd

from dotenv import load_dotenv

import crawler
import nace_classifier
import report_downloader
import report_uploader
import site_finder
import fin_rep_finder
import fin_data_extractor
import genai_utils
import valkey_utils
from valkey_stores import AnnualReportInfoStore, AnnualReportLinkStore, ConversationStore, CompanySiteStore, ModelActionStore, NaceClassificationStore

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    df = pd.read_csv('disco_starting_kit/discovery.csv', sep=';')
    companies = df.drop_duplicates(subset=['ID'])['NAME']
    load_dotenv(override=True)
    try:
        logging.info("Initializing clients...")
        gen_client = genai_utils.GenaiClient.new(model=genai_utils.PRO)
        valkey_client = valkey_utils.ValkeyClient.new()

        convo_store = ConversationStore(valkey_client)
        site_store = CompanySiteStore(valkey_client)
        report_link_store = AnnualReportLinkStore(valkey_client)
        model_action_store = ModelActionStore(valkey_client)
        report_store = AnnualReportInfoStore(valkey_client)
        nace_store = NaceClassificationStore(valkey_client)

        simple_crawler = crawler.Crawler()

        sf = site_finder.SiteFinder(
            gen_client,
            convo_store,
            site_store,
            list(companies[:5]) + ['SUMITOMO CORPORATION'],
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
        rep_dler = report_downloader.ReportDownloader(
            report_link_store=report_link_store,
            report_download_directory='./pdf_downloads/',
            concurrent_threads=10,
        )
        rep_uploader = report_uploader.ReportUploader(
            report_link_store,
            './pdf_downloads/',
            concurrent_threads=10,
        )
        data_extractor = fin_data_extractor.FinDataExtractor(
            gen_client=gen_client,
            conversation_store=convo_store,
            report_link_store=report_link_store,
            report_info_store=report_store,
            report_directory='./pdf_downloads/',
            concurrent_threads=10,
        )
        nace_class = nace_classifier.NaceClassifier(
            gen_client=gen_client,
            report_info_store=report_store,
            conversation_store=convo_store,
            nace_classification_store=nace_store,
            concurrent_threads=10,
        )

        logging.info("Clients initialized successfully.")

        await sf.run()

        await finfinder.run()

        await rep_dler.run()

        #report_link_store.fill_solution_csv('./disco_starting_kit/discovery.csv')

        rep_uploader.run()

        await data_extractor.run()

        await nace_class.run()

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
