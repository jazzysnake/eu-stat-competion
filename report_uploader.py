import os
import logging

import valkey_stores

from models import AnnualReportLinkWithPaths
from gcs_utils import GCSBatchUploader

class UploadError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class ReportUploader:
    def __init__(
        self,
        report_link_store: valkey_stores.AnnualReportLinkStore,
        report_download_directory: str,
        concurrent_threads: int = 1,
    ) -> None:
        self.report_link_store = report_link_store
        self.report_download_directory = report_download_directory
        self.uploader = GCSBatchUploader.new(num_clients=concurrent_threads)
        self.concurrent_threads = concurrent_threads

    def run(self) -> None:
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
            if report.local_path is None:
                logging.error(f'Local path is unexpectedly missing from database for company {company}, skipping upload')
                continue
            report_info.append((
                    company,
                    report.local_path,
                    os.path.basename(report.local_path),
                    )
            )

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

