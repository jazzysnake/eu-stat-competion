import asyncio
import logging
import pandas as pd
import typer
from dotenv import load_dotenv
from pathlib import Path
from typing import Annotated

import crawler
import nace_classifier
import report_downloader
import report_uploader
import site_finder
import fin_rep_finder
import fin_data_extractor
import data_exporter
import genai_utils
import valkey_utils
from valkey_stores import (
    AnnualReportInfoStore,
    AnnualReportLinkStore,
    ConversationStore,
    CompanySiteStore,
    ModelActionStore,
    NaceClassificationStore,
)

# Configure logging at the module level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = typer.Typer(
    help='CLI for Financial Data Processing Pipeline',
    context_settings={'help_option_names': ['-h', '--help']},
)

# --- Default values for common options ---
DEFAULT_CONCURRENCY = 10
DEFAULT_ENV_FILE = Path('.env')
DEFAULT_DISCO_CSV = Path('disco_starting_kit/discovery.csv')
DEFAULT_EXTR_CSV = Path('extra_starting_kit/extraction.csv')
DEFAULT_PDF_DIR = Path('./pdf_downloads/')
DEFAULT_OUTPUT_DIR = Path('.')
DEFAULT_DISCO_CONTAINS_REPORTS = False


# --- Helper function for initialization ---
async def initialize_services(
    concurrency: int,
    discovery_csv_path: Path,
    extraction_csv_path: Path | None,
    output_dir: Path | None,
    pdf_download_dir: Path,
    env_file: Path | None,
    discovery_contains_reports: bool = False,
):
    """Initializes all common services and clients."""
    if env_file and env_file.exists():
        load_dotenv(dotenv_path=env_file, override=True)
        logging.info(f'Loaded environment variables from: {env_file}')
    elif env_file:
        # If a specific env_file is given but not found, it's a warning.
        # load_dotenv will then try to load .env by default if it exists, or use system env vars.
        logging.warning(
            f'Specified environment file {env_file} not found. Attempting to load default .env or use system environment variables.'
        )
        load_dotenv(override=True)
    else:
        # Default behavior: try to load .env if it exists, or use system env vars.
        if DEFAULT_ENV_FILE.exists():
            load_dotenv(dotenv_path=DEFAULT_ENV_FILE, override=True)
            logging.info(f'Loaded environment variables from default: {DEFAULT_ENV_FILE}')
        else:
            load_dotenv(override=True)  # Fallback to system env vars if no .env found
            logging.info('Loaded environment variables from system or no .env file found.')

    if not discovery_csv_path.exists():
        logging.error(f'Input CSV file not found: {discovery_csv_path}')
        raise typer.BadParameter(
            f'Input CSV file not found: {discovery_csv_path}', param_hint='--input-csv'
        )

    try:
        df = pd.read_csv(discovery_csv_path, sep=';')
    except Exception as e:
        logging.error(f'Failed to read or parse CSV file {discovery_csv_path}: {e}')
        raise typer.BadParameter(
            f'Failed to read or parse CSV file {discovery_csv_path}: {e}',
            param_hint='--input-csv',
        )

    all_csv_companies = df.drop_duplicates(subset=['ID'])['NAME']
    sf_company_list = list(all_csv_companies)

    pdf_download_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir_str = str(pdf_download_dir)

    logging.info(f'Initializing clients with concurrency: {concurrency}...')
    # Error handling for client/store initialization is within the try-except block
    # of each command or the run_all_pipeline function.
    gen_client = genai_utils.GenaiClient.new(model=genai_utils.PRO)
    valkey_client = valkey_utils.ValkeyClient.new()

    convo_store = ConversationStore(valkey_client)
    site_store = CompanySiteStore(valkey_client)
    report_link_store = AnnualReportLinkStore(valkey_client)
    model_action_store = ModelActionStore(valkey_client)
    report_store = AnnualReportInfoStore(valkey_client)
    nace_store = NaceClassificationStore(valkey_client)

    simple_crawler = crawler.Crawler(request_timeout_sec=7)

    sf = site_finder.SiteFinder(
        gen_client,
        convo_store,
        site_store,
        sf_company_list,
        simple_crawler,
        concurrent_threads=concurrency,
    )
    finfinder = fin_rep_finder.FinRepFinder(
        simple_crawler,
        gen_client,
        site_store,
        convo_store,
        model_action_store,
        report_link_store,
        report_download_directory=pdf_dir_str,
        concurrent_threads=concurrency,
    )
    rep_dler = report_downloader.ReportDownloader(
        report_link_store=report_link_store,
        report_download_directory=pdf_dir_str,
        concurrent_threads=concurrency,
        report_link_csv_path=discovery_csv_path.as_posix() if discovery_contains_reports else None,
    )
    rep_uploader = report_uploader.ReportUploader(
        report_link_store,
        pdf_dir_str,
        concurrent_threads=concurrency,
    )
    data_extractor = fin_data_extractor.FinDataExtractor(
        gen_client=gen_client,
        conversation_store=convo_store,
        report_link_store=report_link_store,
        report_info_store=report_store,
        report_directory=pdf_dir_str,
        concurrent_threads=concurrency,
    )
    nace_class = nace_classifier.NaceClassifier(
        gen_client=gen_client,
        report_info_store=report_store,
        conversation_store=convo_store,
        nace_classification_store=nace_store,
        concurrent_threads=concurrency,
    )
    services = {
        'gen_client': gen_client,
        'valkey_client': valkey_client,
        'simple_crawler': simple_crawler,
        'sf': sf,
        'finfinder': finfinder,
        'rep_dler': rep_dler,
        'rep_uploader': rep_uploader,
        'data_extractor': data_extractor,
        'nace_class': nace_class,
    }

    if extraction_csv_path is not None and output_dir is not None:
        services['data_exporter'] = data_exporter.DataExporter(
            site_store=site_store,
            report_link_store=report_link_store,
            report_info_store=report_store,
            nace_store=nace_store,
            output_dir=output_dir.as_posix(),
            discovery_csv_path=discovery_csv_path.as_posix(),
            extraction_csv_path=extraction_csv_path.as_posix(),
        )
    logging.info('Clients initialized successfully.')
    return services


async def cleanup_services(services: dict | None):
    """Cleans up resources like database connections and crawlers."""
    if not services:
        return
    if 'valkey_client' in services and services['valkey_client']:
        logging.info('Closing valkey connection...')
        services['valkey_client'].close()
    if 'simple_crawler' in services and services['simple_crawler']:
        logging.info('Closing crawler connection...')
        await services['simple_crawler'].close()
    logging.info('Service cleanup complete.')


# --- Common CLI Options ---
EnvFileOption = Annotated[
    Path | None,
    typer.Option(
        '--env-file',
        help=f"Path to the .env file. If not specified, tries to load '{DEFAULT_ENV_FILE}'.",
        envvar='ENV_FILE_PATH',  # Allow setting via environment variable as well
    ),
]
ConcurrencyOption = Annotated[
    int,
    typer.Option('--concurrency', '-c', help='Number of concurrent threads for the operation.'),
]
DiscoCsvOption = Annotated[
    Path,
    typer.Option(
        '--disco-csv', help='Path to the input CSV (discovery.csv) file with company data.'
    ),
]
ExtCsvOption = Annotated[
    Path,
    typer.Option(
        '--ext-csv', help='Path to the input CSV (extraction.csv) file with company data.'
    ),
]
OutputDirectory = Annotated[
    Path,
    typer.Option(
        '--output-dir', help='Path to the directory where the solution csvs will be output'
    ),
]
PdfDirOption = Annotated[
    Path,
    typer.Option('--pdf-dir', help='Directory for downloading and accessing PDF reports.'),
]

# --- downlaod-reports CLI options
DiscoveryContainsReportsOption = Annotated[
    bool,
    typer.Option(
        '--discovery-contains-reports',
        help='The provided discovery.csv contains already found report links for downloading',
    ),
]


# --- Typer Commands ---


@app.command()
def find_sites(
    concurrency: ConcurrencyOption = DEFAULT_CONCURRENCY,
    env_file: EnvFileOption = None,  # Default handled by initialize_services if None
    discovery_csv: DiscoCsvOption = DEFAULT_DISCO_CSV,
    # pdf_dir is not directly used by site_finder but initialize_services expects it.
    # We could make initialize_services more granular or pass a dummy value if a step doesn't need it.
    # For consistency, we'll provide it.
    pdf_dir: PdfDirOption = DEFAULT_PDF_DIR,
):
    """Finds company websites based on the input CSV."""

    async def _run():
        services = None
        try:
            services = await initialize_services(
                concurrency,
                discovery_csv,
                None,
                None,
                pdf_dir,
                env_file,
            )
            logging.info('Starting site finder...')
            await services['sf'].run()
            logging.info('Site finding completed.')
        except Exception as e:
            logging.error(f'Error in find_sites: {e}', exc_info=True)
            typer.echo(f'Error during site finding: {e}', err=True)
            raise typer.Exit(code=1)
        finally:
            await cleanup_services(services)

    asyncio.run(_run())


@app.command()
def find_reports(
    concurrency: ConcurrencyOption = DEFAULT_CONCURRENCY,
    env_file: EnvFileOption = None,
    discovery_csv: DiscoCsvOption = DEFAULT_DISCO_CSV,  # Needed for company context in init
    pdf_dir: PdfDirOption = DEFAULT_PDF_DIR,
):
    """Finds financial reports for companies with known sites."""

    async def _run():
        services = None
        try:
            services = await initialize_services(
                concurrency,
                discovery_csv,
                None,
                None,
                pdf_dir,
                env_file,
            )
            logging.info('Starting financial report finder...')
            await services['finfinder'].run()
            logging.info('Financial report finding completed.')
        except Exception as e:
            logging.error(f'Error in find_reports: {e}', exc_info=True)
            typer.echo(f'Error during report finding: {e}', err=True)
            raise typer.Exit(code=1)
        finally:
            await cleanup_services(services)

    asyncio.run(_run())


@app.command()
def download_reports(
    concurrency: ConcurrencyOption = DEFAULT_CONCURRENCY,
    env_file: EnvFileOption = None,
    discovery_csv: DiscoCsvOption = DEFAULT_DISCO_CSV,  # Needed for company context in init
    pdf_dir: PdfDirOption = DEFAULT_PDF_DIR,
    discovery_contains_reports: DiscoveryContainsReportsOption = DEFAULT_DISCO_CONTAINS_REPORTS,
):
    """Downloads financial reports that have been found."""

    async def _run():
        services = None
        try:
            services = await initialize_services(
                concurrency,
                discovery_csv,
                None,
                None,
                pdf_dir,
                env_file,
                discovery_contains_reports,
            )
            logging.info('Starting report downloader...')
            await services['rep_dler'].run()
            logging.info('Report downloading completed.')
        except Exception as e:
            logging.error(f'Error in download_reports: {e}', exc_info=True)
            typer.echo(f'Error during report downloading: {e}', err=True)
            raise typer.Exit(code=1)
        finally:
            await cleanup_services(services)

    asyncio.run(_run())


@app.command()
def upload_reports(
    concurrency: ConcurrencyOption = DEFAULT_CONCURRENCY,  # Passed to ReportUploader
    env_file: EnvFileOption = None,
    discovery_csv: DiscoCsvOption = DEFAULT_DISCO_CSV,  # Needed for company context in init
    pdf_dir: PdfDirOption = DEFAULT_PDF_DIR,
):
    """
    Uploads downloaded financial reports.
    """
    services = None
    try:
        # Async initialization
        async def _init_services_async():
            nonlocal services
            services = await initialize_services(
                concurrency,
                discovery_csv,
                None,
                None,
                pdf_dir,
                env_file,
            )

        asyncio.run(_init_services_async())

        if not services:
            # This case should ideally be caught by exceptions in initialize_services
            logging.error('Service initialization failed for upload_reports.')
            raise typer.Exit(code=1)

        logging.info('Starting report uploader...')
        services['rep_uploader'].run()  # Synchronous call
        logging.info('Report uploading completed.')
    except Exception as e:
        logging.error(f'Error in upload_reports: {e}', exc_info=True)
        typer.echo(f'Error during report uploading: {e}', err=True)
        raise typer.Exit(code=1)
    finally:
        if services:
            # Async cleanup
            asyncio.run(cleanup_services(services))


@app.command()
def extract_data(
    concurrency: ConcurrencyOption = DEFAULT_CONCURRENCY,
    env_file: EnvFileOption = None,
    discovery_csv: DiscoCsvOption = DEFAULT_DISCO_CSV,  # Needed for company context in init
    pdf_dir: PdfDirOption = DEFAULT_PDF_DIR,
):
    """Extracts financial data from downloaded and processed reports."""

    async def _run():
        services = None
        try:
            services = await initialize_services(
                concurrency,
                discovery_csv,
                None,
                None,
                pdf_dir,
                env_file,
            )
            logging.info('Starting data extractor...')
            await services['data_extractor'].run()
            logging.info('Data extraction completed.')
        except Exception as e:
            logging.error(f'Error in extract_data: {e}', exc_info=True)
            typer.echo(f'Error during data extraction: {e}', err=True)
            raise typer.Exit(code=1)
        finally:
            await cleanup_services(services)

    asyncio.run(_run())


@app.command()
def classify_nace(
    concurrency: ConcurrencyOption = DEFAULT_CONCURRENCY,
    env_file: EnvFileOption = None,
    discovery_csv: DiscoCsvOption = DEFAULT_DISCO_CSV,  # Needed for company context in init
    pdf_dir: PdfDirOption = DEFAULT_PDF_DIR,  # Needed for consistency in initialize_services
):
    """Classifies companies using NACE codes based on extracted data."""

    async def _run():
        services = None
        try:
            services = await initialize_services(
                concurrency,
                discovery_csv,
                None,
                None,
                pdf_dir,
                env_file,
            )
            logging.info('Starting NACE classifier...')
            await services['nace_class'].run()
            logging.info('NACE classification completed.')
        except Exception as e:
            logging.error(f'Error in classify_nace: {e}', exc_info=True)
            typer.echo(f'Error during NACE classification: {e}', err=True)
            raise typer.Exit(code=1)
        finally:
            await cleanup_services(services)

    asyncio.run(_run())


@app.command()
def export_data(
    concurrency: ConcurrencyOption = DEFAULT_CONCURRENCY,
    env_file: EnvFileOption = None,
    discovery_csv: DiscoCsvOption = DEFAULT_DISCO_CSV,
    extraction_csv: ExtCsvOption = DEFAULT_EXTR_CSV,
    output_directory: OutputDirectory = DEFAULT_OUTPUT_DIR,
    pdf_dir: PdfDirOption = DEFAULT_PDF_DIR,
):
    """Export the discovered and extracted data"""

    async def _run():
        services = None
        try:
            services = await initialize_services(
                concurrency,
                discovery_csv,
                extraction_csv,
                output_directory,
                pdf_dir,
                env_file,
            )
            logging.info('Starting data exporter...')
            services['data_exporter'].run()
            logging.info('Data export completed.')
        except Exception as e:
            logging.error(f'Error in data export: {e}', exc_info=True)
            typer.echo(f'Error during data export: {e}', err=True)
            raise typer.Exit(code=1)
        finally:
            await cleanup_services(services)

    asyncio.run(_run())


@app.command(name='all', short_help='Runs the entire data processing pipeline.')
def run_all_pipeline(
    concurrency: ConcurrencyOption = DEFAULT_CONCURRENCY,
    env_file: EnvFileOption = None,
    discovery_csv: DiscoCsvOption = DEFAULT_DISCO_CSV,
    extraction_csv: ExtCsvOption = DEFAULT_EXTR_CSV,
    output_directory: OutputDirectory = DEFAULT_OUTPUT_DIR,
    pdf_dir: PdfDirOption = DEFAULT_PDF_DIR,
):
    """
    Runs the entire data processing pipeline:
    1. Find Sites
    2. Find Reports
    3. Download Reports
    4. Upload Reports to GCS
    5. Extract Data
    6. Classify NACE
    7. Export to CSV
    """

    async def _run_all():
        services = None
        try:
            services = await initialize_services(
                concurrency,
                discovery_csv,
                extraction_csv,
                output_directory,
                pdf_dir,
                env_file,
            )

            typer.echo(
                f'Starting full pipeline with concurrency: {concurrency}, '
                f'input CSV: {discovery_csv}, PDF directory: {pdf_dir}'
            )
            if env_file:
                typer.echo(f'Using env file: {env_file}')

            logging.info('Step 1: Finding sites...')
            await services['sf'].run()
            logging.info('Site finding completed.')
            typer.echo('✅ Site finding complete.')

            logging.info('Step 2: Finding financial reports...')
            await services['finfinder'].run()
            logging.info('Financial report finding completed.')
            typer.echo('✅ Financial report finding complete.')

            logging.info('Step 3: Downloading reports...')
            await services['rep_dler'].run()
            logging.info('Report downloading completed.')
            typer.echo('✅ Report downloading complete.')

            logging.info('Step 4: Uploading reports...')
            services['rep_uploader'].run()
            logging.info('Report uploading completed.')
            typer.echo('✅ Report uploading complete.')

            logging.info('Step 5: Extracting data...')
            await services['data_extractor'].run()
            logging.info('Data extraction completed.')
            typer.echo('✅ Data extraction complete.')

            logging.info('Step 6: Classifying NACE...')
            await services['nace_class'].run()
            logging.info('NACE classification completed.')
            typer.echo('✅ NACE classification complete.')

            logging.info('Step 7: Exporting to CSV...')
            services['data_exporter'].run()

            typer.echo('🚀 Pipeline completed successfully.')

        except (valkey_utils.ConfigurationError, valkey_utils.ConnectionError) as db_err:
            logging.error(f'Database connection/configuration error: {db_err}', exc_info=True)
            typer.echo(
                f'Error: Failed to connect to or configure the database. Details: {db_err}',
                err=True,
            )
            raise typer.Exit(code=1)
        except ValueError as config_err:  # Can be from GenAI client init or other config issues
            logging.error(f'Configuration error: {config_err}', exc_info=True)
            typer.echo(
                f'Error: Configuration issue (e.g., missing API token, invalid value). Details: {config_err}',
                err=True,
            )
            raise typer.Exit(code=1)
        except genai_utils.GenerationError as gen_err:
            logging.error(f'AI content generation error: {gen_err}', exc_info=True)
            typer.echo(
                f'Error: Failed to generate content using the AI model. Details: {gen_err}',
                err=True,
            )
            raise typer.Exit(code=1)
        except typer.BadParameter as param_err:
            logging.error(f'Invalid parameter: {param_err.message}', exc_info=True)
            typer.echo(f'Error: Invalid parameter. {param_err.message}', err=True)
            raise typer.Exit(code=1)
        except Exception as e:
            logging.error(f'An unexpected error occurred in pipeline: {e}', exc_info=True)
            typer.echo(f'An unexpected error occurred: {e}', err=True)
            raise typer.Exit(code=1)
        finally:
            await cleanup_services(services)

    asyncio.run(_run_all())


if __name__ == '__main__':
    app()
