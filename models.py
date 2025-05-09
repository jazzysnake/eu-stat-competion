import pydantic
from typing import Annotated, Literal


class ModelActionResponse(pydantic.BaseModel):
    action: Annotated[
        Literal['done', 'visit', 'back', 'abort'],
        pydantic.Field(description='The chosen action to perform'),
    ]
    link: Annotated[
        str | None,
        pydantic.Field(
            description='The extracted url pointing to the requested resource, (only fill in case of action=done)',
            default=None,
        ),
    ]
    link_to_visit: Annotated[
        str | None,
        pydantic.Field(
        description='The url to visit next (only fill in case of action=visit)',
        default=None,
        ),
    ]
    reference_year: Annotated[
        str | None,
        pydantic.Field(
            description='The reference date of the requested resource. Supply as format YYYY-MM-DD (only fill in case of action=done)',
            default=None,
        )
    ]
    error: Annotated[
        str | None,
        pydantic.Field(
            description='Error message to fill in case or abort action',
            default=None,
        ),
    ]
    note: Annotated[
        str | None,
        pydantic.Field(
            description='Brief message summarizing the contents of the visited page (only fill in case of action=back)',
            default=None,
    ),
    ]

class ModelActionResponseWithMetadata(ModelActionResponse):
    taken_at_url: str
    action_ts_iso: str


class AnnualReportLink(pydantic.BaseModel):
    link: Annotated[
        str | None,
        pydantic.Field(
            description='Direct link to the latest annual report pdf.',
            default=None,
        )
    ]
    refyear: Annotated[
        int | None,
        pydantic.Field(
            description='Reference year of the pdf.',
            default=None,
        )
    ]
class AnnualReportLinkWithGCS(AnnualReportLink):
    gcs_link: Annotated[
        str | None,
        pydantic.Field(
            description='Direct link to the latest annual report pdf stored in object storage.',
            default=None,
        )
    ]

class SiteDiscoveryResponse(pydantic.BaseModel):
    """Pydantic model for structuring the response from the site discovery process."""
    official_website_link: Annotated[
        str | None,
        pydantic.Field(
            description='The official homepage URL of the company',
            default=None,
        )
    ]
    investor_relations_page: Annotated[
        str | None,
        pydantic.Field(
            description='The investor relations page or subdomain of the company)',
            default=None,
        )
    ]

