import asyncio
import logging
import crawler
import genai_utils
import datetime as dt

from google.genai import types
from utils import batched
from valkey_stores import ConversationStore, CompanySiteStore
from models import SiteDiscoveryResponse
from valkey_utils import ConfigurationError

class SiteFinder:
    def __init__(
        self,
        gen_client: genai_utils.GenaiClient,
        conversation_store: ConversationStore,
        company_site_store: CompanySiteStore,
        company_names: list[str],
        crawler: crawler.Crawler,
        concurrent_threads: int = 1,
    ) -> None:
        self.genai_client = gen_client
        self.conversation_store = conversation_store
        self.company_site_store = company_site_store
        self.company_names = company_names
        if concurrent_threads < 1:
            raise ConfigurationError('concurrent_threads must be larger than 1')
        self.concurrent_threads = concurrent_threads
        self.crawler = crawler

    async def run(self) -> None:
        """Runs the site finding workflow for all companies.
        
        An AI model is prompted to find the official site for each company.
        The conversation with the AI as well as the attained result is saved
        in the configured stores.
        """
        logging.info(f"Site finding started")
        for company_batch in batched(self.company_names, self.concurrent_threads):
            tasks = [self.process_company(c) for c in company_batch]
            await asyncio.gather(*tasks)
        logging.info(f"Site finding finished")

    async def process_company(
        self,
        company: str,
    ) -> None:
        site = self.company_site_store.get(company)
        if site is not None and (site.official_website_link is not None or site.investor_relations_page is not None):
            return
        logging.info(f"Starting site finding for: {company}")
        try:
            res = await self.find_site(company)
            self.company_site_store.store(company, res)
        except Exception as e:
            logging.error(
                f'Failed to find site for company:{company} , cause: {e}',
                exc_info=True,
            )
        logging.info(f"Site finding completed for: {company}")


    async def find_site(
        self,
        company_name: str,
    ) -> SiteDiscoveryResponse:
        """Finds website information for a given company using a Generative AI model.

        This function prompts the AI to find the official website and investor relations
        page for the specified company.
        It utilizes the AI model's browsing capabilities and stores the conversation 
        history in the provided ConversationStore.
        The final structured response is parsed into a Pydantic model.

        Args:
            company_name: The name of the company to search for.

        Returns:
            A SiteDiscoveryResponse object containing the official website link,
            and potentially the investor relations page and financial report link.

        Raises:
            genai_utils.GenerationError: If the AI model fails to generate a response or
                                         if the response cannot be parsed into the
                                         SiteDiscoveryResponse schema.
            pydantic.ValidationError: If the final AI response does not conform to the
                                      SiteDiscoveryResponse schema.
        """
        today = dt.datetime.today()
        prompt = f"""Please find the official website of {company_name}.
                    If possible include the link to the investor relations page/subdomain as well.

                    Include full links in your answer and list the keywords you searched for.
                    Your answer should be structured like this:
                    Queries/keywords I used:
                        - your queries go here
                        ...
                    Results:
                        - the links you found go here

                    The current date is {today}."""
        contents = genai_utils.GenaiClient.get_simple_message(prompt)
        self.conversation_store.store(company_name, 'site_find', contents)
        response = await self.genai_client.generate(
            contents=contents,
            thinking_budget=1024,
            google_search=True,
        )
        contents.append(response.candidates[0].content)
        disco_res = await self.extract_link_from_convo(company_name, contents)

        if disco_res.official_website_link is None and disco_res.investor_relations_page is None:
            raise Exception('Unexpectedly failed to find any site information')
        validated = await self.validate_result(disco_res)
        if validated is not None:
            return validated
        # retry
        contents += self.genai_client.get_simple_message('The links previously retrieved by you were found to not be working anymore. Try again please, now with different queries. Use the date I provided to try to look for more recent results and do not return the same links.')

        self.conversation_store.store(company_name, 'site_find', contents)
        response = await self.genai_client.generate(
            contents=contents,
            thinking_budget=1024,
            google_search=True,
        )
        contents.append(response.candidates[0].content)
        self.conversation_store.store(company_name, 'site_find', contents)

        disco_res = await self.extract_link_from_convo(company_name, contents)

        if disco_res.official_website_link is None and disco_res.investor_relations_page is None:
            raise Exception('Unexpectedly failed to find any site information')
        validated = await self.validate_result(disco_res)
        if validated is None:
            raise Exception(f'All site results found for {company_name} are invalid')
        return validated

    async def extract_link_from_convo(self,company_name, messages: list[types.Content]) -> SiteDiscoveryResponse:
        messages = messages + genai_utils.GenaiClient.get_simple_message(
            "Provide the answer in a structured manner. Only include links present in your previous message."
        )
        response = await self.genai_client.generate(
            model=genai_utils.FLASH,
            contents=messages,
            thinking_budget=0,
            response_schema=SiteDiscoveryResponse,
        )
        messages.append(response.candidates[0].content)

        self.conversation_store.store(company_name, 'site_find', messages)
        return SiteDiscoveryResponse.model_validate_json(response.text)



    async def validate_result(self, site_response: SiteDiscoveryResponse) -> SiteDiscoveryResponse | None:
        valid_official = False
        valid_investors = False
        try:
            if site_response.official_website_link is not None:
                valid_official = await self.validate_link(site_response.official_website_link)
            if site_response.investor_relations_page is not None:
                valid_investors = await self.validate_link(site_response.investor_relations_page)
            if not valid_official and not valid_investors:
                return None
            if not valid_official:
                site_response.official_website_link = None
            if not valid_investors:
                site_response.investor_relations_page = None
        except Exception as e:
            logging.error(f'Unexpected Error occured, returning site discovery response unvalidated, error: {e}', exc_info=True)
        return site_response
        

    
    async def validate_link(self, link: str) -> bool:
        try:
            r = await self.crawler.crawl(link)
            return r.success
        except Exception:
            return False
        

