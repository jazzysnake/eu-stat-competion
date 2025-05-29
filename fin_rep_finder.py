import asyncio
import logging
import datetime as dt

import crawler
import genai_utils
import valkey_stores

from utils import batched
from models import ModelActionResponse, ModelActionResponseWithMetadata, AnnualReportLink
from valkey_utils import ConfigurationError


class CrawlAbortError(Exception):
    """Custom exception raised when the LLM decides to abort crawling for a report."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class CrawlState:
    """Represents the state of the crawling process for a single company."""

    def __init__(
        self,
        current_url: str,
        action_history: list[ModelActionResponseWithMetadata],
        url_history: list[str],
    ) -> None:
        """Initializes the crawling state.

        Args:
            current_url: The URL currently being processed or to be visited next.
            action_history: A list of actions (ModelActionResponseWithMetadata)
                            taken by the model so far.
            url_history: A list of URLs visited in sequence, forming a navigation stack.
        """
        self.current_url = current_url
        self.action_history = action_history
        self.url_history = url_history


class FinRepFinder:
    """
    Navigates company websites using an LLM to find links to annual financial reports.
    """

    def __init__(
        self,
        crawler: crawler.Crawler,
        gen_client: genai_utils.GenaiClient,
        site_store: valkey_stores.CompanySiteStore,
        conversation_store: valkey_stores.ConversationStore,
        model_action_store: valkey_stores.ModelActionStore,
        annual_report_link_store: valkey_stores.AnnualReportLinkStore,
        report_download_directory: str,
        companies: list[str] | None = None,
        max_tries_per_company: int = 10,
        concurrent_threads: int = 1,
    ) -> None:
        """Initializes the FinRepFinder.

        Args:
            crawler: An instance of the web crawler (crawler.Crawler).
            gen_client: Client for generative AI model interaction.
            site_store: Store for company website information (official site, investor page).
            conversation_store: Store for conversation history with the AI.
            model_action_store: Store for model actions taken during crawling.
            annual_report_link_store: Store for found annual report links.
            report_download_directory: Directory where reports will eventually be downloaded.
                                       (Used for context, not direct download by this class).
            companies: Optional list of specific company names to process. If None,
                       processes all companies found in the `site_store`.
            max_tries_per_company: Maximum number of web pages to visit per company
                                   in an attempt to find a report. Defaults to 10.
            concurrent_threads: Number of concurrent threads for processing companies.
                                Must be >= 1.

        Raises:
            ConfigurationError: If `concurrent_threads` is less than 1.
        """
        self.crawler = crawler
        self.gen_client = gen_client
        self.site_store = site_store
        self.report_download_directory = report_download_directory
        self.conversation_store = conversation_store
        self.model_action_store = model_action_store
        self.annual_report_link_store = annual_report_link_store
        self.companies = companies
        self.max_pages_per_company = max_tries_per_company
        if concurrent_threads < 1:
            raise ConfigurationError('concurrent_threads must be >= 1')
        self.concurrent_threads = concurrent_threads

    async def run(self) -> None:
        """Runs the financial report finding process for the specified companies.

        Processes companies in batches using concurrent threads. If no specific
        companies are provided during initialization, it retrieves them from the
        `site_store`.
        """
        companies = self.companies
        if companies is None:
            companies = self.site_store.get_companies()
        if len(companies) == 0:
            return

        for company_batch in batched(companies, self.concurrent_threads):
            tasks = [self.process_company(c) for c in company_batch]
            await asyncio.gather(*tasks)

    async def process_company(
        self,
        company: str,
    ) -> None:
        """Processes a single company to find its annual report link.

        It first checks if a report link (even if None) or a 'done' action
        is already stored for the company. If so, it skips processing.
        Otherwise, it attempts to find the report and stores the result.

        Args:
            company: The name of the company to process.
        """
        existing_report = self.annual_report_link_store.get(company)
        if existing_report is not None and existing_report.link is not None:
            return
        res = await self.find_annual_report(company)
        if res is None or res.link is None:
            logging.warning(f'Could not find report link for {company}')
            return
        self.annual_report_link_store.store(company, res)

    async def find_annual_report(self, company: str) -> AnnualReportLink | None:
        """Finds the annual report for a company by crawling its websites.

        It first checks if a 'done' action (indicating a completed attempt)
        is already stored for the company. If so, it returns the result from
        that action.
        Otherwise, it determines starting URLs (investor relations page or
        official website) and initiates the crawling process.

        Args:
            company: The name of the company.

        Returns:
            AnnualReportLink | None: The found annual report link and reference year,
                                     or None if not found or if crawling was aborted.
        """
        done = self.model_action_store.get_done_action(company)
        if done is not None:
            if done.action != 'done':  # aborted
                return None
            refyear = (
                int(done.reference_year.split('-')[0]) if done.reference_year is not None else None
            )
            return AnnualReportLink(link=done.link, refyear=refyear)

        start_urls = []

        # get starting points
        site = self.site_store.get(company)
        if site is None:
            logging.error(f'Annual report called on company with no site in db {company}')
        if site.investor_relations_page is not None:
            start_urls.append(site.investor_relations_page)
        if site.official_website_link is not None:
            start_urls.append(site.official_website_link)

        if not start_urls:
            logging.warning(
                f'Skipping annual report discovery for {company}, no valid starting link found'
            )
            return None

        for url in start_urls:
            try:
                return await self.crawl_to_report(company, url)
            except ValueError as e:
                logging.error(str(e))
        return None

    def format_history_prompt(
        self,
        history: list[ModelActionResponseWithMetadata],
        urlqueue: list[str],
    ) -> str:
        """Formats the history of model actions and visited URLs for the LLM prompt.

        Args:
            history: List of model actions (ModelActionResponseWithMetadata) taken.
            urlqueue: Current navigation stack (list of URLs visited).

        Returns:
            str: A formatted string detailing the navigation history and actions.
        """
        h = f"""Here is your current navigation stack:
        {'->'.join(urlqueue)}
        Here are the actions you have taken so far:\n"""
        history = sorted(history, key=lambda x: x.action_ts_ms, reverse=False)
        for a in history:
            action_json = a.model_dump(mode='json', exclude={'taken_at_url'})
            h += f'URL: {a.taken_at_url}, Action: {action_json}\n'
        return h

    def format_crawl_prompt(
        self,
        webpage_markdown: str,
        history: list[ModelActionResponseWithMetadata] | None,
        urlqueue: list[str],
    ) -> str:
        """Formats the main prompt for the LLM to guide its web crawling action.

        The prompt instructs the LLM on how to interpret the webpage content,
        what actions it can take ('done', 'visit', 'back', 'abort'), and
        the expected JSON format for its response. It also includes history.

        Args:
            webpage_markdown: Markdown content of the current webpage.
            history: Optional list of previous model actions and their outcomes.
            urlqueue: Current navigation stack (list of URLs visited).

        Returns:
            str: The complete prompt string for the LLM.
        """
        history_reminder = '' if history is None else self.format_history_prompt(history, urlqueue)
        return f"""Extract the direct link to the latest annual financial report (pdf if available, only stop at html for private companies) from the markdownified webpage below.

        If you found it. output: {{"action":"done", "annual_report":"link goes here", "reference_year":"YYYY-MM-DD"}}.

        If you did not find a direct link, but think one of the links will lead there output: {{"action":"visit", "link_to_visit":"link goes here"}}

        If you visited a link, but it lead did not lead you where you expected, you can choose to go back by outputting {{"action":"back", "note":"very brief message about what you found (2 sentences max)"}}

        If you think there is a problem or there is no chance of finding the annual report on this page, output {{"action":"abort", "error":"error message here"}}

        {history_reminder}

        webpage:\n{webpage_markdown}"""

    async def __handle_model_interaction(
        self,
        company: str,
        page_markdown: str,
        state: CrawlState,
    ) -> AnnualReportLink | None:
        """Manages a single interaction with the LLM for a webpage.

        This involves:
        1. Formatting the prompt with the current page's markdown and crawl history.
        2. Sending the prompt to the LLM and getting its action response.
        3. Storing the conversation and the LLM's action.
        4. Updating the `CrawlState` based on the action (e.g., changing `current_url`).
        5. Returning an `AnnualReportLink` if the LLM's action is 'done'.

        Args:
            company: The name of the company being processed.
            page_markdown: Markdown content of the current webpage.
            state: The current `CrawlState` object for this company's crawl session.

        Returns:
            AnnualReportLink | None: An `AnnualReportLink` if the 'done' action is
                                     received with a valid link. Otherwise, None.

        Raises:
            CrawlAbortError: If the LLM outputs an 'abort' action.
            ValueError: If a 'visit' action is taken without a `link_to_visit`,
                        or if a 'back' action is taken with no previous URL in history.
            genai_utils.GenerationError: If the LLM fails to generate a response or if the response is malformed.
        """
        prompt = self.format_crawl_prompt(
            page_markdown,
            state.action_history,
            state.url_history,
        )
        conversation = self.gen_client.get_simple_message(prompt)

        self.conversation_store.store(company, 'report_find', conversation)
        generation_res = await self.gen_client.generate(
            conversation,
            thinking_budget=1024,
            response_schema=ModelActionResponse,
            model=genai_utils.PRO,
        )
        conversation.append(generation_res.candidates[0].content)
        self.conversation_store.store(company, 'report_find', conversation)
        response = ModelActionResponse.model_validate_json(generation_res.text)

        state.url_history.append(state.current_url)
        action = ModelActionResponseWithMetadata(
            **response.model_dump(),
            taken_at_url=state.current_url,
            action_ts_ms=int(dt.datetime.now().timestamp() * 1000),
        )
        self.model_action_store.store(
            company,
            state.current_url,
            action,
            (action.action == 'done' or action.action == 'abort'),
        )
        state.action_history.append(action)
        if response.action == 'done':
            refyear = (
                int(response.reference_year.split('-')[0])
                if response.reference_year is not None
                else None
            )
            return AnnualReportLink(link=response.link, refyear=refyear)
        if action.action == 'abort':
            raise CrawlAbortError('LLM decided to abort crawling')
        if action.action == 'visit':
            if action.link_to_visit is None:
                raise ValueError('Visit action taken with no url to visit')
            state.current_url = action.link_to_visit
        if action.action == 'back':
            if len(state.url_history) < 2:
                raise ValueError('Back action taken with no previous url')
            state.current_url = state.url_history[-2]
        return None

    async def crawl_to_report(self, company: str, start_url: str) -> AnnualReportLink | None:
        """Crawls websites starting from a given URL to find an annual report.

        This method iteratively:
        1. Crawls the `state.current_url`.
        2. Handles interaction with the LLM via `__handle_model_interaction`.
        3. Updates `CrawlState` based on LLM's action.
        The loop continues until a report is found, LLM aborts, max pages are visited,
        or an error occurs.

        Args:
            company: The name of the company.
            start_url: The initial URL to start crawling from.

        Returns:
            AnnualReportLink | None: The found annual report link, or None if not
                                     found within limits or due to abort/error.
        """
        state = CrawlState(
            start_url,
            action_history=[],
            url_history=[],
        )
        report = None
        page_visits = 0
        retried = False
        self.model_action_store.del_all(company)

        while page_visits < self.max_pages_per_company:
            res = await self.crawler.crawl(state.current_url)
            if not res.success:
                if retried and page_visits == 0:
                    break
                if retried:
                    markdown = f'Failed to crawl {state.current_url}'
                retried = True
                continue
            markdown = res.markdown

            try:
                report = await self.__handle_model_interaction(
                    company,
                    markdown,
                    state,
                )
                if report is not None:
                    return report
            except CrawlAbortError:
                logging.info(f'LLM decided to abort crawling to report for company {company}')
                break
            res = None
            page_visits += 1

        return report
