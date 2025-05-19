import asyncio
import logging
import numpy as np

import crawler
import genai_utils
import valkey_stores

from utils import batched
from models import ModelActionResponse, ModelActionResponseWithMetadata, AnnualReportLink
from valkey_utils import ConfigurationError

class FinRepFinder:
    def __init__(
        self,
        crawler: crawler.Crawler,
        gen_client: genai_utils.GenaiClient,
        site_store: valkey_stores.CompanySiteStore,
        company_profile_store: valkey_stores.CompanyProfileStore,
        conversation_store: valkey_stores.ConversationStore,
        model_action_store: valkey_stores.ModelActionStore,
        annual_report_link_store: valkey_stores.AnnualReportLinkStore,
        report_download_directory: str,
        companies: list[str] | None = None,
        max_tries_per_company: int = 10,
        concurrent_threads: int = 1,
    ) -> None:
        self.crawler = crawler
        self.gen_client = gen_client
        self.site_store = site_store
        self.profile_store = company_profile_store
        self.report_download_directory = report_download_directory
        self.conversation_store = conversation_store
        self.model_action_store = model_action_store
        self.annual_report_link_store = annual_report_link_store
        self.companies = companies
        self.max_tries_per_company = max_tries_per_company
        if concurrent_threads < 1:
            raise ConfigurationError('concurrent_threads must be > 1')
        self.concurrent_threads = concurrent_threads

    async def run(self) -> None:
        companies = self.companies
        if companies is None:
            companies = self.site_store.get_companies()
            companies += self.profile_store.get_companies()
            companies = np.unique(companies).tolist()

        if len(companies) == 0:
            return
        
        for company_batch in batched(companies, self.concurrent_threads):
            tasks = [self.process_company(c) for c in company_batch]
            await asyncio.gather(*tasks)

    async def process_company(
        self,
        company: str,
    ) -> None:
        existing_report=self.annual_report_link_store.get(company) 
        if existing_report is not None and existing_report.link is not None:
            return
        res = await self.find_annual_report(company)
        if res is None or res.link is None:
            logging.warning(f'Could not find report link for {company}')
            return
        self.annual_report_link_store.store(company,res)

    async def find_annual_report(self, company: str) -> AnnualReportLink | None:
        done = self.model_action_store.get_done_action(company)
        if done is not None:
            if done.action != 'done': # aborted
                return None
            refyear = int(done.reference_year.split('-')[0]) if done.reference_year is not None else None
            return AnnualReportLink(link=done.link, refyear=refyear)

        start_urls = []
        # continue previous crawl
        current = self.model_action_store.get_current_url(company)
        if current is not None:
            start_urls.append(current)

        # get starting points
        profile = self.profile_store.get(company)
        if profile is not None and profile.website is not None:
            start_urls.append(profile.website)
        site = self.site_store.get(company)
        if site is not None:
            if site.investor_relations_page is not None:
                start_urls.append(site.investor_relations_page)
            if site.official_website_link is not None:
                start_urls.append(site.official_website_link)

        if not start_urls:
            logging.warning(f'Skipping annual report discovery for {company}, no valid starting link found')
            return None

        for url in start_urls:
            try:
                return await self.crawl_to_report(company,url)
            except ValueError as e:
                logging.error(str(e))
        return None


    def format_history_prompt(
        self,
        history: list[ModelActionResponseWithMetadata],
        urlqueue: list[str],
    ) -> str:
        h = f"""Here is your current navigation stack:
        {'->'.join(urlqueue)}
        Here are the actions you have taken so far:\n"""
        history = sorted(history, key=lambda x: x.action_ts_iso, reverse=False)
        for a in history:
            action_json = a.model_dump(mode='json', exclude={'taken_at_url'})
            h+=f'URL: {a.taken_at_url}, Action: {action_json}\n'
        return h


    def format_crawl_prompt(
        self,
        webpage_markdown: str,
        history: list[ModelActionResponseWithMetadata] | None,
        urlqueue: list[str],
    ) -> str:
        history_reminder = '' if history is None else self.format_history_prompt(history, urlqueue)
        return f"""Extract the direct link to the latest annual financial report (pdf if available, only stop at html for private companies) from the markdownified webpage below.

        If you found it. output: {{"action":"done", "annual_report":"link goes here", "reference_year":"YYYY-MM-DD"}}.

        If you did not find a direct link, but think one of the links will lead there output: {{"action":"visit", "link_to_visit":"link goes here"}}

        If you visited a link, but it lead did not lead you where you expected, you can choose to go back by outputting {{"action":"back", "note":"very brief message about what you found (2 sentences max)"}}

        If you think there is a problem or there is no chance of finding the annual report on this page, output {{"action":"abort", "error":"error message here"}}

        {history_reminder}

        webpage:\n{webpage_markdown}"""

    async def crawl_to_report(self, company:str, start_url: str) -> AnnualReportLink | None:
        res = await self.crawler.crawl(start_url)
        if not res[0].success:
            raise ValueError(f'Failed to crawl provided start_url {start_url}')
        url = start_url
        report = None
        tries = 0
        history = None
        urlqueue = self.model_action_store.get_full_url_queue(company)
        if urlqueue is None:
            urlqueue = []
        prev_actions = self.model_action_store.get_all_actions(company)
        if len(prev_actions) != 0:
            history = prev_actions
        retried = False

        while tries < self.max_tries_per_company:
            if res is None:
                res = await self.crawler.crawl(url)
                if not res[0].success:
                    if retried:
                        logging.error(f'Failed to crawl url: {url}')
                        break
                    await asyncio.sleep(1)
                    retried = True
                    continue

            prompt = self.format_crawl_prompt(res.markdown, history, urlqueue)
            content = self.gen_client.get_simple_message(prompt)
            self.conversation_store.store(company, 'report_find',content)
            generation_res = await self.gen_client.generate(
                content,
                thinking_budget=1024,
                response_schema=ModelActionResponse,
                model=genai_utils.PRO,
            )
            content.append(generation_res.candidates[0].content)
            self.conversation_store.store(company, 'report_find',content)
            response = ModelActionResponse.model_validate_json(generation_res.text)
            self.model_action_store.store(
                company,
                url,
                response,
                (response.action=='done' or response.action =='abort'),
            )
            if response.action == 'done':
                refyear = int(response.reference_year.split('-')[0]) if response.reference_year is not None else None
                report = AnnualReportLink(link=response.link, refyear=refyear)
                break
            action_with_metadata = self.model_action_store.get(company, url)
            if action_with_metadata is None:
                raise Exception('Unexpected modification to valkey data')

            if history is None:
                history = [action_with_metadata]
            else:
                history.append(action_with_metadata)

            if action_with_metadata.action == 'abort':
                break
            elif action_with_metadata.action == 'visit':
                if action_with_metadata.link_to_visit is None:
                    logging.error('TODO')
                    break
                url = action_with_metadata.link_to_visit


            urlqueue = self.model_action_store.get_full_url_queue(company)
            res = None
            tries += 1
            
        return report


