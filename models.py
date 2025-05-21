import pydantic
from typing import Annotated, Literal


class ModelActionResponse(pydantic.BaseModel):
    """
    Represents an action decision made by the LLM during web crawling
    to find financial reports.
    """
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
    """
    Extends ModelActionResponse with metadata about when and where (URL)
    the action was decided by the LLM.
    """
    taken_at_url: str
    action_ts_ms: int


class AnnualReportLink(pydantic.BaseModel):
    """
    Stores the direct link to an annual financial report and its reference year.
    """
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
class AnnualReportLinkWithPaths(AnnualReportLink):
    """
    Extends AnnualReportLink with local filesystem path and GCS (Google Cloud Storage)
    link for the downloaded report file.
    """
    local_path: Annotated[
        str | None,
        pydantic.Field(
            description='Path to the locally downloaded pdf',
            default=None,
        )
    ]

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


class AnnualReportInfo(pydantic.BaseModel):
    """
    Stores extracted financial and general information from an annual report.
    """
    country_code: Annotated[
        str | None,
        pydantic.Field(
        description='The ISO 3166-1 alpha-2 country code where the company is headquartered at (2 uppercase letters).',
        pattern=r'^[A-Z]{2}$',
        default=None,
    )]
    employee_count: Annotated[
        int | None,
        pydantic.Field(
        description='The number of employees employed by the company (worldwide).',
        default=None,
    )]
    assets_value: Annotated[
        int | None,
        pydantic.Field(
        description='Total assets of the company expressed in nominal value.',
        default=None,
    )]
    net_turnover: Annotated[
        int | None,
        pydantic.Field(
        description='Net turnover for the company in the financial year',
        default=None,
        )
    ]
    currency_code_assets: Annotated[
        str | None,
        pydantic.Field(
        description='ISO 4217 currency code of the currency in which the assets_value is denominated (3 uppercase letters).',
        pattern=r'^[A-Z]{3}$',
        default=None,
    )
    ]
    currency_code_turnover: Annotated[
        str | None,
        pydantic.Field(
        description='ISO 4217 currency code of the currency in which net_turnover is denominated (3 uppercase letters).',
        pattern=r'^[A-Z]{3}$',
        default=None,
    )
    ]
    main_activity_description: Annotated[
        str | None,
        pydantic.Field(
        description='Brief description (5 sentences max) of the main activity performed by the company',
        default=None,
        )
    ]
    reference_year: Annotated[
        int | None,
        pydantic.Field(
            description='The year which the annual report is about. (Calendar year of the closing date of the financial year)',
            default=None,
        )
    ]

class Lvl1ClassificationResponse(pydantic.BaseModel):
    """Represents a Level 1 NACE classification result from the LLM."""
    classification: Annotated[
        str,
        pydantic.Field(
            description='The level 1 NACE class the company belongs to (single letter)'
        )
    ]

class Lvl2ClassificationResponse(pydantic.BaseModel):
    """Represents a Level 2 NACE classification result from the LLM."""
    classification: Annotated[
        str,
        pydantic.Field(
            description='The level 2 NACE class the company belongs to (2 digit code)',
        )
    ]
