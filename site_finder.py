import asyncio
import logging
import genai_utils
import datetime as dt

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
        concurrent_threads: int = 1,
    ) -> None:
        self.genai_client = gen_client
        self.conversation_store = conversation_store
        self.company_site_store = company_site_store
        self.company_names = company_names
        if concurrent_threads < 1:
            raise ConfigurationError('concurrent_threads must be larger than 1')
        self.concurrent_threads = concurrent_threads

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
        res = await self.find_site(company)
        self.company_site_store.store(company, res)
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
                    Try to find the direct link to the latest financial report of the company.
                    If possible include the link to the investor relations page/subdomain as well.

                    Include full links in your answer. The current date is {today}."""
        contents = genai_utils.GenaiClient.get_simple_message(prompt)
        self.conversation_store.store(company_name, 'site_find', contents)
        response = await self.genai_client.generate(
            contents=contents,
            thinking_budget=1024,
            google_search=True,
        )
        contents.append(response.candidates[0].content)
        self.conversation_store.store(company_name, 'site_find', contents)
        contents = contents + genai_utils.GenaiClient.get_simple_message(
            "Provide the answer in a structured manner. Only include links present in your previous message."
        )
        response = await self.genai_client.generate(
            model=genai_utils.FLASH,
            contents=contents,
            thinking_budget=0,
            response_schema=SiteDiscoveryResponse,
        )
        contents.append(response.candidates[0].content)
        self.conversation_store.store(company_name, 'site_find', contents)
        return SiteDiscoveryResponse.model_validate_json(response.text)
