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
        """Initializes the ConversationStore with a Valkey client instance.

        Args:
            client: An initialized ValkeyClient instance for database interaction.
        """
        self.client = client

    def add(
        self,
        company_name:str,
        action: Literal['site_find'],
        conversation_contents: list[types.Content],
    ) -> None:
        """Adds a conversation history to the Valkey store.

        The conversation is stored as a hash, with each message serialized as JSON.
        The key is generated based on the company name and action.

        Args:
            company_name: The name of the company the conversation relates to.
            action: The specific action or context of the conversation (e.g., 'site_find').
            conversation_contents: The list of `google.genai.types.Content` or `ContentDict`
                                   objects representing the conversation.
        """
        k = ConversationStore.__create_key(company_name, action)
        simple_contents = GenaiClient.get_simple_contents(conversation_contents)

        mapping = {f'message:{i}':json.dumps(c) for i,c in enumerate(simple_contents)}
        self.client.client.hset(k, mapping=mapping)

    @staticmethod
    def __create_key(company_name: str, action: str) -> str:
        """Creates a standardized Valkey key for storing conversation data.

        Normalizes the company name for consistency.

        Args:
            company_name: The name of the company.
            action: The action associated with the conversation.

        Returns:
            A formatted string to be used as a Valkey key (e.g., 'conversation:site_find:adecco_group_ag')."""
        return f'conversation:{action}:{company_name}'


