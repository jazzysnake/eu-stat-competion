import json
from typing import Literal

from google.genai import types
from genai_utils import GenaiClient
import valkey_utils

class ConversationStore:
    def __init__(
        self,
        client: valkey_utils.ValkeyClient,
        ) -> None:
        self.client = client

    def add(
        self,
        company_name:str,
        action: Literal['site_find'],
        conversation_contents: list[types.Content],
    ) -> None:
        k = ConversationStore.__create_key(company_name, action)
        simple_contents = GenaiClient.get_simple_contents(conversation_contents)

        mapping = {f'message:{i}':json.dumps(c) for i,c in enumerate(simple_contents)}
        self.client.client.hset(k, mapping=mapping)

    @staticmethod
    def __create_key(company_name: str, action: str) -> str:
        return f'conversation:{action}:{company_name}'


