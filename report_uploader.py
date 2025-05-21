import os
import logging

import valkey_stores

from models import AnnualReportLinkWithPaths
from gcs_utils import GCSBatchUploader

class UploadError(Exception):
    """Custom exception for errors during report uploading."""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class ReportUploader:
    """Handles uploading of annual financial reports to Google Cloud Storage."""
    def __init__(
        self,
        report_link_store: valkey_stores.AnnualReportLinkStore,
        report_download_directory: str,
        concurrent_threads: int = 1,
    ) -> None:
        """Initializes the ReportUploader.

        Args:
            report_link_store: Store for annual report links, used to get report metadata.
            report_download_directory: Directory where reports are downloaded locally.
            concurrent_threads: Number of concurrent threads for uploading files to GCS.
        """
        self.report_link_store = report_link_store
        self.report_download_directory = report_download_directory
        self.uploader = GCSBatchUploader.new(num_clients=concurrent_threads)
        self.concurrent_threads = concurrent_threads

    def run(self) -> None:
        """Uploads downloaded annual reports to Google Cloud Storage.

        It iterates through companies in the report_link_store.
        For each company, it checks if a report is available locally and
        has not already been uploaded (i.e., no GCS link exists).
        If conditions are met, the report is uploaded, and the GCS link
        is stored back in the report_link_store.
        """
        logging.info('Started report uploads')
        companies = self.report_link_store.get_companies()
        reports = []
        for c in companies:
            reports.append(self.report_link_store.get(c))

        report_info = []
        for company, report in zip(companies, reports):
            if not isinstance(report, AnnualReportLinkWithPaths):
                logging.warning(f'Skipping upload of report for company {company}, not available locally')
                continue
            if report.gcs_link is not None:
                logging.warning(f'Skipping upload for company {company} as it has a gcs link')
                continue
            if report.local_path is None:
                logging.error(f'Local path is unexpectedly missing from database for company {company}, skipping upload')
                continue
            report_info.append((
                    company,
                    report.local_path,
                    os.path.basename(report.local_path),
                    )
            )

        logging.info(f'Starting upload of {len(report_info)} documents')
        results = self.uploader.upload_blobs(
            [r[1] for r in report_info],
            [r[2] for r in report_info],
        )
        
        for report_info, result in zip(report_info, results):
            company, _, _ = report_info
            _, res = result
            if type(res) != str:
                logging.error(f'Failed to upload report of company {company}, error: {res}')
                continue
            self.report_link_store.add_gcs_link(company, res)
            

        logging.info('Finished report uploads')

