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
        """Initializes the GenaiClient.

        Sets up the connection to the Google Generative AI API using the provided
        API key, model name, and safety settings.

        Args:
            api_key: The Google Generative AI API key.
            model: The name of the model to use (e.g., 'gemini-1.5-pro-preview-0514'). Defaults to PRO.
            harm_block: The safety threshold for blocking harmful content. Defaults to OFF."""
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
    def new(model:str=PRO) -> 'GenaiClient':
        """Creates a new GenaiClient instance using the API key from environment variables.

        Args:
            model: The name of the generative model to use. Defaults to the PRO model constant.

        Returns:
            A configured GenaiClient instance.

        Raises:
            ValueError: If the 'GEMINI_API_TOKEN' environment variable is not set.
        """
        api_key=os.environ.get('GEMINI_API_TOKEN')
        if api_key is None:
            raise ValueError('GEMINI_API_TOKEN variable not set, failed to init client')
        return GenaiClient(api_key=api_key,model=model)

    @staticmethod
    def get_simple_message(
        msg: str,
        role: Literal['user', 'model'] = 'user',
    ) -> list[types.Content]:
        """Creates a simple Google GenAI Content object list for a single message.

        Args:
            msg: The text content of the message.
            role: The role of the message sender ('user' or 'model'). Defaults to 'user'.

        Returns:
            A list containing a single `google.generativeai.types.Content`.
        """
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
        """Converts a list of GenAI Content objects to a list of dicts that contain the text only.

        This is useful for serialization or logging.

        Args:
            contents: A list of `google.generai.types.Content`

        Returns:
            A list of dictionaries, where each dictionary has 'role' (str) and
            'parts' (list[str]) keys representing the text parts."""
        return [
            {'role': c.role, 'parts': [p.text for p in c.parts]}
            for c in contents
        ]

    async def generate(
        self,
        contents: list[types.Content],
        thinking_budget:int=0,
        response_schema: Type[pydantic.BaseModel] | None = None,
        google_search: bool = False,
        model: str | None = None,
        temperature: float = 0,
    ) -> types.GenerateContentResponse:
        """Generates content using the Google Generative AI API.

        Args:
            contents: A list of `google.genai.types.Content` or `ContentDict` objects
                      representing the conversation history or prompt.
            thinking_budget: An optional token budget the model can use for thinking.
            response_schema: An optional Pydantic model to structure the JSON response.
                             If provided, response_mime_type is set to 'application/json'.
            google_search: Whether to enable the Browsing tool (replaces Google Search explicitly).
            model: An optional model name to override the client's default model
                   for this specific call.


        Returns:
            The `google.generai.types.GenerateContentResponse` object from the API.

        Raises:
            GenerationError: If the content generation API call fails.
        """
        response_type = "text/plain" if response_schema is None else "application/json"
        generate_content_config = types.GenerateContentConfig(
            thinking_config = types.ThinkingConfig(
                thinking_budget=thinking_budget,
            ),
            response_mime_type=response_type,
            response_schema=response_schema,
            temperature=temperature,
            tools=[
                types.Tool(google_search=types.GoogleSearch())
            ] if google_search else None
        )
        try:
            response = await self.client.aio.models.generate_content(
                model=self.model if model is None else model,
                contents=contents,
                config=generate_content_config,
            )
            return response
        except Exception as e:
            raise GenerationError('Failed to generate content') from e

