import pydantic
import genai_utils
import datetime as dt
from typing import Annotated, Literal
from llm_conversation_store import ConversationStore


class ModelActionResponse(pydantic.BaseModel):
    action: Annotated[
        Literal['done', 'visit', 'back', 'abort'],
        pydantic.Field(description='The chosen action to perform'),
    ]
    link: Annotated[
        str | None,
        pydantic.Field(description='The extracted url pointing to the requested resource, (only fill in case of action=done)'),
    ]
    reference_year: Annotated[
        str | None,
        pydantic.Field(description='The reference date of the requested resource. Supply as format YYYY-MM-DD (only fill in case of action=done)')
    ]
    error: Annotated[
        str | None,
        pydantic.Field(description='Error message to fill in case or abort action'),
    ]
    note: Annotated[
        str | None,
        pydantic.Field(description='Brief message summarizing the contents of the visited page (only fill in case of action=back'),
    ]

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


async def find_site(
    gen_client: genai_utils.GenaiClient,
    conversation_store: ConversationStore,
    company_name: str,
) -> SiteDiscoveryResponse:
    """Finds website information for a given company using a Generative AI model.

    This function prompts the AI to find the official website, investor relations
    page, and the latest financial report link for the specified company.
    It utilizes the AI model's browsing capabilities and stores the conversation 
    history in the provided ConversationStore.
    The final structured response is parsed into a Pydantic model.

    Args:
        gen_client: An initialized GenaiClient instance.
        conversation_store: An initialized ConversationStore instance to log the interaction.
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
    conversation_store.add(company_name, 'site_find', contents)
    response = await gen_client.generate(
        contents=contents,
        thinking_budget=1024,
        google_search=True,
    )
    contents.append(response.candidates[0].content)
    conversation_store.add(company_name, 'site_find', contents)
    contents = contents + genai_utils.GenaiClient.get_simple_message(
        "Provide the answer in a structured manner. Only include links present in your previous message."
    )
    response = await gen_client.generate(
        model=genai_utils.FLASH,
        contents=contents,
        thinking_budget=0,
        response_schema=SiteDiscoveryResponse,
    )
    contents.append(response.candidates[0].content)
    conversation_store.add(company_name, 'site_find', contents)
    return SiteDiscoveryResponse.model_validate_json(response.text)
