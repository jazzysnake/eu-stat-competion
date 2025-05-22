# EU Stat Competition - Web Intelligence - MNE Group Data Discovery and Extraction

This project is a pipeline designed to discover, download, process, and extract data from annual financial reports of Multinational Enterprise (MNE) groups. It leverages web crawling, generative AI for data extraction and classification, and Google Cloud Storage for report storage.

## Features

*   Finds official company websites and investor relations pages.
*   Locates and downloads annual financial reports (PDF or HTML).
*   Uploads downloaded reports to Google Cloud Storage.
*   Extracts key financial figures and company information from reports using AI.
*   Classifies companies based on NACE codes using AI.
*   Exports the collected data into specified CSV formats.

## Installation

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd <your-repo-name>
```

### 2. Set up Python Environment

This project uses uv for managing Python versions and dependencies.

Install uv:
If you don't have uv installed, follow the instructions at uv's official documentation.

Create and Activate Virtual Environment:
The project specifies Python 3.11 (see .python-version file). uv can manage this for you.

```bash
uv sync
source .venv/bin/activate  # On Linux/macOS
# .venv\Scripts\activate    # On Windows
```

### 3. Set up Crawl4ai

This project uses crawl4ai for web crawling. crawl4ai requires a browser to be installed and configured.

Run Crawl4ai Setup:
After installing the dependencies, run the crawl4ai setup command. This will help you download and configure the necessary browser drivers.

```bash
crawl4ai-setup
```

For more detailed instructions on crawl4ai installation and setup, please refer to the official [Crawl4ai documentation](https://docs.crawl4ai.com/core/installation/)).

## Configuration

### 1. Environment Variables

The application uses environment variables for configuration, primarily for API keys and paths. Create a .env file in the root of the project directory. You can copy the structure from a .env.example if provided, or create it manually.

Example .env structure:

```
# Google AI (Gemini)
GEMINI_API_TOKEN="your_google_ai_api_key"

# Google Cloud Storage
GOOGLE_PROJECT_ID="your_gcp_project_id"
GOOGLE_CREDENTIALS_PATH="/path/to/your/gcs-service-account-key.json"
GOOGLE_STORAGE_BUCKET_NAME="your_gcs_bucket_name"

# Valkey (Redis-compatible datastore)
VALKEY_HOST="localhost"
VALKEY_PORT="6379"
VALKEY_PW="changeme" # Ensure this matches your valkey.conf or docker-compose setup
VALKEY_DB="0"
```

### 2. API Keys

Google AI (Gemini) API Key:
You need an API key from Google AI Studio (or Google Cloud Vertex AI) to use the Gemini models for data extraction and NACE classification.

Visit Google AI Studio or the Google Cloud Console.

Generate an API key.

Add this key to your .env file as GEMINI_API_TOKEN.

Google Cloud Storage (GCS) Service Account Key:
To store and retrieve financial reports from Google Cloud Storage, you need a GCS service account key.

Go to the Google Cloud Console.

Navigate to "IAM & Admin" > "Service Accounts".

Create a new service account or use an existing one. Grant it appropriate permissions for GCS (e.g., "Storage Object Admin" or more granular permissions for the target bucket).

Create a JSON key for this service account and download it.

Store the path to this JSON key file in your .env file as GOOGLE_CREDENTIALS_PATH.

Set GOOGLE_PROJECT_ID to your Google Cloud Project ID and GOOGLE_STORAGE_BUCKET_NAME to the name of the GCS bucket you want to use.

### 3. Valkey Datastore

The application uses Valkey (a fork of Redis) as a key-value store for caching intermediate results, conversation histories, and processed data. A docker-compose.yaml file is provided to easily run a Valkey instance.

Start Valkey using Docker Compose:

```bash
docker-compose up -d
```

This will start a Valkey container with persistence enabled, using the configuration from valkey.conf. The default password is "changeme" (as set in valkey.conf). Ensure your .env file's VALKEY_PW matches this.

## Usage (CLI)

The application provides a Command Line Interface (CLI) built with Typer for running different parts of the pipeline.

All commands can be run using python main.py <command> [OPTIONS].
Use python main.py --help to see all available commands and options.

### Common Options

--concurrency / -c INTEGER: Number of concurrent threads for the operation (default: 10).

--env-file FILE_PATH: Path to the .env file (e.g., .env or .llm.env).

--disco-csv FILE_PATH: Path to the input CSV (discovery.csv) file with company data (default: disco_starting_kit/discovery.csv).

--pdf-dir DIRECTORY_PATH: Directory for downloading and accessing PDF/HTML reports (default: ./pdf_downloads/).

--ext-csv FILE_PATH: Path to the input CSV template for extraction data (default: extra_starting_kit/extraction.csv). Used by export-data and all.

--output-dir DIRECTORY_PATH: Path to the directory where the solution CSVs will be output (default: .). Used by export-data and all.

### Commands

find-sites: Finds company websites based on the input CSV.

```bash
python main.py find-sites --disco-csv path/to/your/discovery.csv
```

find-reports: Finds financial reports for companies with known sites.

```bash
python main.py find-reports
```

download-reports: Downloads financial reports that have been found.

```bash
python main.py download-reports --pdf-dir ./my_reports
```
upload-reports: Uploads downloaded financial reports to Google Cloud Storage.

```bash
python main.py upload-reports --pdf-dir ./my_reports
```

extract-data: Extracts financial data from downloaded and processed reports.

```bash
python main.py extract-data
```
classify-nace: Classifies companies using NACE codes based on extracted data.

```bash
python main.py classify-nace
```
export-data: Exports the discovered and extracted data into the final CSV files.

```bash
python main.py export-data --disco-csv path/to/discovery.csv --ext-csv path/to/extraction_template.csv --output-dir ./results
```
all: Runs the entire data processing pipeline from finding sites to exporting data.
```bash
python main.py all --disco-csv path/to/discovery.csv --ext-csv path/to/extraction_template.csv --output-dir ./results --concurrency 5
```

**The pipeline generally follows these steps**:

Site Finding: Identifies official websites and investor relations pages for companies listed in the input CSV.

Financial Report Finding: Crawls the identified sites to locate links to annual financial reports.

Report Downloading: Downloads the found reports (PDF or HTML) to a local directory.

Report Uploading: Uploads the downloaded reports to Google Cloud Storage for backup and accessibility.

Data Extraction: Uses generative AI to extract key financial figures, company headquarters country, employee count, and a description of main activities from the reports.

NACE Classification: Employs generative AI to classify companies based on their main activity description into NACE codes.

Data Exporting: Populates template CSV files with all the discovered and extracted information.

## Project Structure

main.py: CLI entry point.

site_finder.py: Logic for finding company websites.

fin_rep_finder.py: Logic for locating financial report links.

report_downloader.py: Handles downloading of report files.

report_uploader.py: Handles uploading reports to GCS.

fin_data_extractor.py: Logic for extracting data from reports using GenAI.

nace_classifier.py: Logic for NACE classification using GenAI.

data_exporter.py: Handles creation of final output CSVs.

valkey_stores.py: Manages data persistence in Valkey.

genai_utils.py: Utilities for interacting with Google's GenAI models.

gcs_utils.py: Utilities for Google Cloud Storage interaction.

crawler.py: Wrapper for web crawling functionalities.

models.py: Pydantic models for data structures.

data/: Contains static data like NACE code definitions.

pdf_downloads/: Default directory for downloaded reports.
