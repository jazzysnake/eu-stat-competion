import logging
import pydantic
import genai_utils
import datetime as dt
from typing import Annotated
from valkey_stores import ConversationStore, CompanySiteStore

class SiteDiscoveryResponse(pydantic.BaseModel):
    """Pydantic model for structuring the response from the site discovery process."""
    official_website_link: Annotated[
        str | None,
        pydantic.Field(description='The official homepage URL of the company')
    ]
    investor_relations_page: Annotated[
        str | None,
        pydantic.Field(description='The investor relations page or subdomain of the company)')
    ]

class SiteFinder:
    def __init__(
        self,
        gen_client: genai_utils.GenaiClient,
        conversation_store: ConversationStore,
        company_site_store: CompanySiteStore,
        company_names: list[str],
    ) -> None:
        self.genai_client = gen_client
        self.conversation_store = conversation_store
        self.company_site_store = company_site_store
        self.company_names = company_names

    async def run(self) -> None:
        """Runs the site finding workflow for all companies.
        
        An AI model is prompted to find the official site for each company.
        The conversation with the AI as well as the attained result is saved
        in the configured stores.
        """
        logging.info(f"Site finding started")
        for company in self.company_names:
            logging.info(f"Starting site finding for: {company}")
            res = await self.find_site(company)
            self.company_site_store.add(company, res)
            logging.info(f"Site finding completed for: {company}")
        logging.info(f"Site finding finished")

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
        self.conversation_store.add(company_name, 'site_find', contents)
        response = await self.genai_client.generate(
            contents=contents,
            thinking_budget=1024,
            google_search=True,
        )
        contents.append(response.candidates[0].content)
        self.conversation_store.add(company_name, 'site_find', contents)
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
        self.conversation_store.add(company_name, 'site_find', contents)
        return SiteDiscoveryResponse.model_validate_json(response.text)
