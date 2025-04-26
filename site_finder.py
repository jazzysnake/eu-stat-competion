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
    official_website_link: str
    investor_relations_page: Annotated[
        str | None,
        pydantic.Field(description='The investor relations page or subdomain of the company)')
    ]
    financial_report_link: Annotated[
        str | None,
        pydantic.Field(description='Direct link to the latest annual financial report'),
    ]


def find_site(
    gen_client: genai_utils.GenaiClient,
    conversation_store: ConversationStore,
    company_name: str,
) -> SiteDiscoveryResponse:
    today = dt.datetime.today()
    prompt = f"""Please find the official website of {company_name}.
                Try to find the direct link to the latest financial report of the company.
                If possible include the link to the investor relations page/subdomain as well.

                The current date is {today}."""
    contents = genai_utils.GenaiClient.get_simple_message(prompt)
    conversation_store.add(company_name, 'site_find', contents)
    response = gen_client.generate(
        contents=contents,
        thinking_budget=1024,
        google_search=True,
    )
    contents.append(response.candidates[0].content)
    conversation_store.add(company_name, 'site_find', contents)
    contents = contents + genai_utils.GenaiClient.get_simple_message(
        "Provide your answer in a structured manner"
    )
    response = gen_client.generate(
        model=genai_utils.FLASH,
        contents=contents,
        thinking_budget=0,
        response_schema=SiteDiscoveryResponse,
    )
    contents.append(response.candidates[0].content)
    conversation_store.add(company_name, 'site_find', contents)
    return SiteDiscoveryResponse.model_validate_json(response.text)
