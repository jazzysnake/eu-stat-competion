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
class AnnualReportLinkWithPaths(AnnualReportLink):
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

class Lvl1ClassificationResponse(pydantic.BaseModel):
    classification: Annotated[
        str,
        pydantic.Field(
            description='The level 1 NACE class the company belongs to (single letter)'
        )
    ]

class Lvl2ClassificationResponse(pydantic.BaseModel):
    classification: Annotated[
        str,
        pydantic.Field(
            description='The level 2 NACE class the company belongs to (2 digit code)',
        )
    ]

class StockSearchResult(pydantic.BaseModel):
    company_name: str
    symbol: str
    exchange: str

class CompanyProfileInformation(pydantic.BaseModel):
    company_name: Annotated[
        str | None,
        pydantic.Field(description='Name of the company', default=None),
    ]
    website: Annotated[
        str | None,
        pydantic.Field(description='Full URL pointing to the website of the company', default=None),
    ]
    company_headquarter_country: Annotated[
        str | None,
        pydantic.Field(
        description='The ISO 3166-1 alpha-2 country code where the company is headquartered at (2 uppercase letters).',
        pattern=r'^[A-Z]{2}$',
        default=None,
    )
    ]
    company_headquarter_city: Annotated[
        str | None,
        pydantic.Field(
            description='The city where the company is headquartered at',
            default = None,
        )
    ]
    reporting_currency: Annotated[
        str | None,
        pydantic.Field(
            description='The currency the company reports in (3 letter symbol)',
            default=None,
        )
    ]
    sic_code: Annotated[
        str | None,
        pydantic.Field(
            description='The Standard Industrial Classification (SIC) code of the company (4 digit code)',
            default=None,
    )
    ]
    employees: Annotated[
        int | None,
        pydantic.Field(
            description='The number of employees employed by the company',
            default=None,
        )
    ]
    main_activity_description: Annotated[
        str | None,
        pydantic.Field(
            description='Brief description (5 sentences max) of the main activity performed by the company.',
            default=None,
        )
    ]

class CompanyFinancialInformation(pydantic.BaseModel):
    fiscal_year: Annotated[
        int | None,
        pydantic.Field(
            description='The fiscal year the data was reported in.',
            default=None,
        ),
    ]
    revenue: Annotated[
        int | None,
        pydantic.Field(
            description='The revenue of the company for the fiscal year.',
            default=None,
        )
    ]
    unit_of_revenue: Annotated[
        Literal['Raw', 'Thousands', 'Millions', 'Billions'] | None,
        pydantic.Field(
            description='The unit in which the company reports its revenue.',
            default=None,
        )
    ]
    currency: Annotated[
        str | None,
        pydantic.Field(
            description='The 3 letter code of the currency in which the company reports revenue',
            default=None,
        )
    ]


class CompanyAssetInformation(pydantic.BaseModel):
    fiscal_year: Annotated[
        int | None,
        pydantic.Field(
            description='The fiscal year the data was reported in.',
            default=None,
        ),
    ]
    total_assets: Annotated[
        int | None,
        pydantic.Field(
            description='The total assets of the company for the fiscal year.',
            default=None,
        )
    ]
    unit_of_assets: Annotated[
        Literal['Raw', 'Thousands', 'Millions', 'Billions'] | None,
        pydantic.Field(
            description='The unit in which the company reports its revenue.',
            default=None,
        )
    ]
    currency: Annotated[
        str | None,
        pydantic.Field(
            description='The 3 letter code of the currency in which the company reports asset values',
            default=None,
        )
    ]

class CompanyMatchesResponse(pydantic.BaseModel):
    classification_result: bool

