import crawler
import pydantic
from typing import Annotated, Literal

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
