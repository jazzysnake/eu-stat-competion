import os
import httpx
import logging
import asyncio

import valkey_stores

from utils import batched
from models import AnnualReportLink, AnnualReportLinkWithPaths
from crawler import HTMLDownloader
from pdf_downloader import PDFDownloader

class DownloadError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class ReportDownloader:
    def __init__(
        self,
        report_link_store: valkey_stores.AnnualReportLinkStore,
        report_download_directory: str,
        concurrent_threads: int = 1,
    ) -> None:
        self.report_link_store = report_link_store
        self.report_download_directory = report_download_directory
        self.concurrent_threads = concurrent_threads
        self.pdf_downloader = PDFDownloader()
        self.html_downloader = HTMLDownloader()

    async def run(self) -> None:
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
        try:
            reportlink = self.report_link_store.get(company)
            if reportlink is None:
                logging.error(f'Report link is missing for company {company}, skipping download')
                return
            if reportlink is not None:
                if isinstance(
                    reportlink,
                    AnnualReportLinkWithPaths,
                ) and reportlink.local_path is not None:
                    return
            logging.info(f'Downloading report for {company}...')
            fname = await self.download_annual_report(reportlink, company)
            self.report_link_store.add_local_path(company, fname)
        except DownloadError as e:
            logging.error(e, exc_info=True)
        except Exception as e:
            logging.error(f'Unexpected error occured: {e}', exc_info=True)


    @staticmethod
    def __clean_filename(name: str) -> str:
        return name.replace(' ', '_').replace('\\', '').replace('/', '')
        
    async def download_annual_report(
        self,
        report_link: AnnualReportLink,
        company: str,
    ) -> str:
        if report_link.link is None:
            raise ValueError('Link must not be None')
        refyear = '' if report_link.refyear is None else str(report_link.refyear)
        fname = '_'.join([company,refyear])
        try:
            is_pdf = await self.pdf_downloader.is_pdf(report_link.link)
            fname = fname + '.pdf' if is_pdf else fname + ".html"
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

