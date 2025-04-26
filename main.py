import site_finder
import genai_utils
import valkey_utils
from llm_conversation_store import ConversationStore

def main():
    gen_client = genai_utils.GenaiClient.new()
    valkeyClient = valkey_utils.ValkeyClient.new()
    convoStore = ConversationStore(valkeyClient)
    res = site_finder.find_site(gen_client, convoStore, 'ADECCO GROUP AG')
    print(res)


if __name__ == "__main__":
    main()
