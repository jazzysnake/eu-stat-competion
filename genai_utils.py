import os
from typing import Literal, Type
from google import genai
from google.genai import types
import pydantic

FLASH = 'gemini-2.5-flash-preview-04-17'
PRO = 'gemini-2.5-pro-preview-03-25'

class GenerationError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class GenaiClient:
    HARM_CATEGORIES = (
        types.HarmCategory.HARM_CATEGORY_CIVIC_INTEGRITY,
        types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        types.HarmCategory.HARM_CATEGORY_HARASSMENT,
        types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        types.HarmCategory.HARM_CATEGORY_UNSPECIFIED,
    )
    def __init__(
        self,
        api_key:str,
        model:str=PRO,
        harm_block: types.HarmBlockThreshold = types.HarmBlockThreshold.OFF,
    ) -> None:
        self.model = model
        self.client = genai.Client(
            api_key=api_key,
        )
        safety_settings = [
            types.SafetySetting(
                category=category,
                threshold=harm_block,
            ) for category in GenaiClient.HARM_CATEGORIES
        ]
        self.safety_settings=safety_settings

    @staticmethod
    def new(model:str="gemini-2.5-pro-preview-03-25") -> 'GenaiClient':
        api_key=os.environ.get('GEMINI_API_TOKEN')
        if api_key is None:
            raise ValueError('GEMINI_API_TOKEN variable not set, failed to init client')
        return GenaiClient(api_key=api_key,model=model)

    @staticmethod
    def get_simple_message(
            msg: str,
            role: Literal['user', 'assistant'] = 'user',
    ) -> list[types.Content]:
        return [
            types.Content(
                role=role,
                parts=[
                    types.Part.from_text(text=msg),
                ],
            ),
        ]
    @staticmethod
    def get_simple_contents(
        contents: list[types.Content]
    ) -> list[dict[str, str | list[str]]]:
        return [
            {'role': c.role, 'parts': [p.text for p in c.parts]}
            for c in contents
        ]


    def generate(
        self,
        contents: list[types.Content],
        thinking_budget:int=0,
        response_schema: Type[pydantic.BaseModel] | None = None,
        google_search: bool = False,
        model: str | None = None,
    ):
        response_type = "text/plain" if response_schema is None else "application/json"
        generate_content_config = types.GenerateContentConfig(
            thinking_config = types.ThinkingConfig(
                thinking_budget=thinking_budget,
            ),
            response_mime_type=response_type,
            response_schema=response_schema,
            tools=[
                types.Tool(google_search=types.GoogleSearch())
            ] if google_search else None
        )
        try:
            response = self.client.models.generate_content(
                model=self.model if model is None else model,
                contents=contents,
                config=generate_content_config,
            )
            return response
        except Exception as e:
            raise GenerationError('Failed to generate content') from e

