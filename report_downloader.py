import os
import httpx
import logging
import asyncio
import pandas as pd

import valkey_stores

from utils import batched
from models import AnnualReportLink, AnnualReportLinkWithPaths
from crawler import HTMLDownloader
from pdf_downloader import PDFDownloader


class DownloadError(Exception):
    """Custom exception for errors encountered during report downloading."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class ReportDownloader:
    """Handles downloading of annual financial reports (PDF or HTML)."""

    def __init__(
        self,
        report_link_store: valkey_stores.AnnualReportLinkStore,
        report_download_directory: str,
        concurrent_threads: int = 1,
        report_link_csv_path: str | None = None,
    ) -> None:
        """Initializes the ReportDownloader.

        Args:
            report_link_store: Store for annual report links, used to get URLs.
            report_download_directory: Directory to save downloaded reports.
            concurrent_threads: Number of concurrent threads for downloading files.
            report_link_csv_path: Path to a csv containing companies annual financial report data
                                  (expected to be in the format of discovery.csv)
        """
        self.report_link_store = report_link_store
        self.report_download_directory = report_download_directory
        self.concurrent_threads = concurrent_threads
        self.report_link_csv_path = report_link_csv_path
        if report_link_csv_path is not None:
            self.additional_reports = ReportDownloader.read_discovery_csv(report_link_csv_path)
            for c, r in self.additional_reports:
                self.report_link_store.store(c, r)
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.7',
            'priority': 'u=1, i',
            'referer': '',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Brave";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        }
        self.pdf_downloader = PDFDownloader(default_headers=headers)
        self.html_downloader = HTMLDownloader()

    async def run(self) -> None:
        """Downloads annual reports for companies in batches.

        Retrieves companies from the report_link_store and processes them
        concurrently based on the configured number of threads.
        """
        companies = self.report_link_store.get_companies()
        if len(companies) == 0:
            return

        for company_batch in batched(companies, self.concurrent_threads):
            tasks = [self.process_company(c) for c in company_batch]
            await asyncio.gather(*tasks)

    async def process_company(
        self,
        company: str,
    ) -> None:
        """Processes a single company to download its annual report.

        Skips downloading if the report link is missing, or if the report
        (identified by local_path) has already been downloaded.
        Stores the local path of the downloaded file back into the report_link_store.

        Args:
            company: The name of the company to process.
        """
        try:
            report_link = self.report_link_store.get(company)
            if report_link is None:
                logging.error(f'Report link is missing for company {company}, skipping download')
                return
            if report_link is not None:
                if (
                    isinstance(
                        report_link,
                        AnnualReportLinkWithPaths,
                    )
                    and report_link.local_path is not None
                ):
                    return
            logging.info(f'Downloading report for {company}...')
            fname = await self.download_annual_report(report_link, company)
            self.report_link_store.add_local_path(company, fname)
        except DownloadError as e:
            logging.error(e, exc_info=True)
        except Exception as e:
            logging.error(f'Unexpected error occured: {e}', exc_info=True)

    @staticmethod
    def __clean_filename(name: str) -> str:
        """Cleans a filename by replacing spaces and removing invalid characters.

        Replaces spaces with underscores and removes backslashes and forward slashes.

        Args:
            name: The original filename string.

        Returns:
            str: The cleaned filename string.
        """
        return name.replace(' ', '_').replace('\\', '').replace('/', '')

    async def download_annual_report(
        self,
        report_link: AnnualReportLink,
        company: str,
    ) -> str:
        """Downloads an annual report, determining if it's PDF or HTML.

        Constructs a filename based on company name and reference year.
        If the target is a PDF and an initial download attempt results in a 403 error,
        it retries with a spoofed browser user agent.

        Args:
            report_link: The AnnualReportLink object containing the URL and reference year.
            company: The name of the company (for logging and filename generation).

        Returns:
            str: The local path to the downloaded file.

        Raises:
            ValueError: If `report_link.link` is None.
            DownloadError: If downloading fails after all attempts or for other reasons.
                           This can be due to HTTP errors, network issues, or other exceptions
                           from the underlying downloaders.
        """
        if report_link.link is None:
            raise ValueError('Link must not be None')
        refyear = '' if report_link.refyear is None else str(report_link.refyear)
        fname = '_'.join([company, refyear])
        try:
            is_pdf = await self.pdf_downloader.is_pdf(report_link.link)
            fname = fname + '.pdf' if is_pdf else fname + '.html'
            fname = ReportDownloader.__clean_filename(fname)
            fname = os.path.join(self.report_download_directory, fname)
            if is_pdf:
                try:
                    await self.pdf_downloader.download_async(report_link.link, fname)
                except httpx.HTTPStatusError as e:
                    if '403' in str(e):
                        print(f'Retrying download with spoofed browser agent for {company}')
                        await self.pdf_downloader.download_async(
                            report_link.link,
                            fname,
                            spoof_browser_user_agent=True,
                        )

                return fname
            await self.html_downloader.download(report_link.link, fname)
            return fname
        except Exception as e:
            raise DownloadError(f'Failed to download report for company {company}') from e

    @staticmethod
    def read_discovery_csv(csv_path: str) -> list[tuple[str, AnnualReportLink]]:
        """Reads the discovery.csv and return a list of (company name, AnnualReportlink) tuples.

        Args:
            csv_path: The path to the filled out discovery csv that contains financial report info

        Returns:
            list[tuple[str, AnnualReportLink]]: The list of company name - financial report pairs
        """
        df = pd.read_csv(csv_path, sep=';')
        df = df.dropna(subset=['SRC'])
        df = df.query("TYPE == 'FIN_REP'")
        return [
            (row['NAME'], AnnualReportLink(link=row['SRC'], refyear=row['REFYEAR']))
            for _, row in df.iterrows()
        ]
