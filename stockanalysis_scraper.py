import crawl4ai
import httpx
import asyncio
import logging

from datetime import datetime
from typing import Literal
from urllib.parse import quote_plus

import crawler
import genai_utils
import valkey_stores


from utils import batched
from models import (
            CompanyMatchesResponse,
            CompanyAssetInformation,
            CompanyProfileInformation,
            CompanyFinancialInformation,
            StockSearchResult,
)

class RequestLimitError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class StockAnalysisScraper:
    base_url = "https://stockanalysis.com"
    def __init__(
        self,
        companies: list[str],
        gen_client: genai_utils.GenaiClient,
        crawler: crawler.Crawler,
        conversation_store: valkey_stores.ConversationStore,
        company_profile_store: valkey_stores.CompanyProfileStore,
        company_assets_store: valkey_stores.CompanyAssetsStore,
        company_financials_store: valkey_stores.CompanyFinancialsStore,
        concurrent_threads: int = 1,
        proxy: crawl4ai.ProxyConfig | None = None,
    ) -> None:
        self.companies = companies
        p = None
        if proxy is not None:
            auth = f'{proxy.username}:{proxy.password}@' if proxy.username is not None and proxy.password is not None else ''
            p = f'http://{auth}{proxy.server.removeprefix("http://")}'
        self.http_client = httpx.AsyncClient(proxy=p)
        self.crawler = crawler
        self.gen_client = gen_client
        self.convo_store = conversation_store
        self.profile_store = company_profile_store
        self.assets_store = company_assets_store
        self.financials_store = company_financials_store
        self.concurrent_threads = concurrent_threads

    async def run(self) -> None:
        logging.info("Stock Analysis scraping started")
        for company_batch in batched(self.companies, self.concurrent_threads):
            tasks = [self.process_company(c) for c in company_batch]
            await asyncio.gather(*tasks)
        logging.info("Stock Analysis scraping stopped")

    async def process_company(
        self,
        company_name: str,
    ) -> None:
        try:
            asset_data = self.assets_store.get(company_name)
            if asset_data is not None:
                logging.info(f'Skipping scraping company {company_name}, as it is already processed')
                return
            search_res = await self.search(company_name)
            await asyncio.sleep(1)
            if len(search_res) <1:
                logging.warning(f'Could not retrieve search results for company {company_name}')
                return 
            matches = await self.match_found_classification(company_name, search_res[0])
            if not matches:
                # stockanalysis has a good query endpoint. if they have info on the company
                # the first one is almost always the correct one
                # but if they do not, they still return a list of results so
                # we abort if the first one does not match
                logging.warning(f'Skipping company {company_name}, first result did not match')
                return
                profile = await self.get_profile_info(company_name, search_res[0])
                self.profile_store.store(company_name, profile)
                fiscals = await self.get_fiscal_data(company_name, search_res[0])
                self.financials_store.store(company_name, fiscals)
                assets = await self.get_asset_data(company_name, search_res[0])
                self.assets_store.store(company_name, assets)
        except RequestLimitError as e:
            await asyncio.sleep(120)
            logging.warning(f'Likely hit request limit on sa, sleeping for 2 minutes. cause {e}')
        except Exception as e:
            logging.error(
                f'Failed to process company: {company_name}, cause: {e}', exc_info=True,
            )

    @classmethod
    def assemble_url(
        cls,
        stock_search_result: StockSearchResult,
        endpoint: Literal['balance_sheet', 'financials', 'profile'],
    ) -> str:
        if stock_search_result.exchange == 'stocks':
            company_base = f"{cls.base_url}/{stock_search_result.exchange}/{stock_search_result.symbol}"
        else:
            company_base = f"{cls.base_url}/quote/{stock_search_result.exchange}/{stock_search_result.symbol}"
        if endpoint == 'financials':
            return f"{company_base}/financials"
        if endpoint == 'balance_sheet':
            return f"{company_base}/financials/balance-sheet"
        return f"{company_base}/company"

    async def match_found_classification(
        self,
        company_name: str,
        search_res: StockSearchResult,
    ) -> bool:
        prompt = f"""Determine if the search result retrieved by an api matches the company.
        search query: {company_name}

        search result: {search_res.model_dump_json(indent=2)}
        """
        msgs = self.gen_client.get_simple_message(prompt)
        r = await self.gen_client.generate(
            msgs,
            thinking_budget=0,
            model=genai_utils.FLASH,
            response_schema=CompanyMatchesResponse,
        )
        return CompanyMatchesResponse.model_validate_json(r.text).classification_result

    async def search(
        self,
        company_name: str,
    ) -> list[StockSearchResult]:
        search = self.base_url + "/api/search?q="
        query = quote_plus(company_name)
        res = await self.http_client.get(search + query)
        if res.status_code == 429:
            # sa has agressive throttling
            await asyncio.sleep(90)
        res.raise_for_status()
        js = res.json()
        data = js.get('data')
        if data is None:
            raise ValueError('Search result contains no data')
        results = [
            StockSearchResult(
                company_name=r['n'],
                symbol=r['s'].split('/')[-1],
                exchange=r['s'].split('/')[0] if '/' in r['s'] else 'stocks',
            )
            for r in data
            if r.get('n') is not None and r.get('s') is not None
        ]
        await asyncio.sleep(1)
        return results
    
    async def get_profile_info(
        self,
        company_name: str,
        stock_search_result: StockSearchResult,
    ) -> CompanyProfileInformation:
        profile_endpoint = self.assemble_url(stock_search_result, 'profile')
        res = await self.crawler.crawl(profile_endpoint, avoid_bot_detection=True)
        if str(res.markdown).strip() == '':
            raise RequestLimitError('Failed to extract markdown information')
        prompt = f"""Extract structured information about the company {company_name} from the markdown below.
            Notes:
            - Stick to the facts presented in the markdown.
            - For the main activity description, collect the main industries and sectors the company participates in (if possible order them by priority)

        Markdown:
                {res.markdown}
        """
        msgs = self.gen_client.get_simple_message(prompt)
        self.convo_store.store(
            company_name,
            'profile_info_extraction',
            msgs,
        )
        gen_res = await self.gen_client.generate(
            msgs,
            response_schema=CompanyProfileInformation,
            model=genai_utils.FLASH,
        )
        if gen_res.candidates is None:
            raise genai_utils.GenerationError(f'Failed to generate a single candidate for profile info extraction for company {company_name}')
        msgs.append(gen_res.candidates[0].content)
        self.convo_store.store(
            company_name,
            'profile_info_extraction',
            msgs,
        )
        return CompanyProfileInformation.model_validate_json(gen_res.text)

    async def get_fiscal_data(
        self,
        company_name: str,
        stock_search_result: StockSearchResult,
    ) -> CompanyFinancialInformation:
        fiscal_endpoint = self.assemble_url(stock_search_result, 'financials')
        res = await self.crawler.crawl(fiscal_endpoint, avoid_bot_detection=True)
        if str(res.markdown).strip() == '':
            raise RequestLimitError('Failed to extract markdown information')
        print(res.markdown)
        prompt = f"""Extract the information about the company {company_name} for the latest full fiscal year from the markdown below. 
        Current date is {datetime.today()}. Pay attention to when each fiscal year's period ends and only choose the latest.
        
        markdown:
                {res.markdown}
        """
        msgs = self.gen_client.get_simple_message(prompt)
        self.convo_store.store(
            company_name,
            'fiscal_data_extraction',
            msgs,
        )
        gen_res = await self.gen_client.generate(
            msgs,
            response_schema=CompanyFinancialInformation,
            model=genai_utils.FLASH,
        )
        if gen_res.candidates is None:
            raise genai_utils.GenerationError(f'Failed to generate a single candidate for fiscal info extraction for company {company_name}')
        msgs.append(gen_res.candidates[0].content)
        self.convo_store.store(
            company_name,
            'fiscal_data_extraction',
            msgs,
        )
        return CompanyFinancialInformation.model_validate_json(gen_res.text)

    async def get_asset_data(
        self,
        company_name: str,
        stock_search_result:StockSearchResult,
    ) -> CompanyAssetInformation:
        balance_sheet_endpoint = self.assemble_url(stock_search_result, 'balance_sheet')
        res = await self.crawler.crawl(balance_sheet_endpoint, avoid_bot_detection=True)
        if str(res.markdown).strip() == '':
            raise RequestLimitError('Failed to extract markdown information')
        print(res.markdown)
        prompt = f"""Extract the information about the company {company_name} for the latest full fiscal year from the markdown below. 
        Current date is {datetime.today()}. Pay attention to when each fiscal year's period ends and only choose the latest.
        
        markdown:
                {res.markdown}
        """
        msgs = self.gen_client.get_simple_message(prompt)
        self.convo_store.store(
            company_name,
            'asset_data_extraction',
            msgs,
        )
        gen_res = await self.gen_client.generate(
            msgs,
            response_schema=CompanyAssetInformation,
            model=genai_utils.FLASH,
        )
        if gen_res.candidates is None:
            raise genai_utils.GenerationError(f'Failed to generate a single candidate for asset info extraction for company {company_name}')
        msgs.append(gen_res.candidates[0].content)
        self.convo_store.store(
            company_name,
            'asset_data_extraction',
            msgs,
        )
        return CompanyAssetInformation.model_validate_json(gen_res.text)
