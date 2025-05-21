import json
import pandas as pd
import datetime as dt

from models import AnnualReportLink, AnnualReportLinkWithPaths, ModelActionResponseWithMetadata, SiteDiscoveryResponse, AnnualReportInfo

import valkey_utils

from typing import Literal

from google.genai import types
from genai_utils import GenaiClient

class ConversationStore:
    """
    Stores and retrieves conversation histories with AI models in Valkey.
    Conversations are typically related to a specific company and action.
    """
    def __init__(
        self,
        client: valkey_utils.ValkeyClient,
        ) -> None:
        """Initializes the ConversationStore with a Valkey client instance.

        Args:
            client: An initialized ValkeyClient instance for database interaction.
        """
        self.client = client

    def store(
        self,
        company_name:str,
        action: Literal['site_find', 'report_find', 'info_extract', 'nace_classify'],
        conversation_contents: list[types.Content],
    ) -> None:
        """Adds or updates a conversation history in the Valkey store.

        The conversation is stored as a Valkey hash, with each message
        (prompt/response pair or individual messages) serialized as JSON.
        The key is generated based on the company name and action.

        Args:
            company_name: The name of the company the conversation relates to.
            action: The specific action or context of the conversation (e.g., 'site_find').
            conversation_contents: The list of `google.genai.types.Content` or
                                   compatible dictionaries representing the conversation messages.
        """
        k = ConversationStore.__create_key(company_name, action)
        simple_contents = GenaiClient.get_simple_contents(conversation_contents)

        mapping = {f'message:{i}':json.dumps(c) for i,c in enumerate(simple_contents)}
        self.client.client.hset(k, mapping=mapping)

    @staticmethod
    def __create_key(company_name: str, action: str) -> str:
        """Creates a standardized Valkey key for storing conversation data.

        Args:
            company_name: The name of the company.
            action: The action or context associated with the conversation.

        Returns:
            A formatted string to be used as a Valkey key
            (e.g., 'conversation:site_find:adecco_group_ag').
        """
        return f'conversation:{action}:{company_name}'

class CompanySiteStore:
    """
    Stores and retrieves company website information (official site, investor relations page)
    in Valkey.
    """
    def __init__(
        self,
        client: valkey_utils.ValkeyClient,
        ) -> None:
        """Initializes the CompanySiteStore with a Valkey client instance.

        Args:
            client: An initialized ValkeyClient instance for database interaction.
        """
        self.client = client

    def store(
        self,
        company_name:str,
        site_discovery_result: SiteDiscoveryResponse,
    ) -> None:
        """Adds or updates a site discovery result to the Valkey store.

        The result (official website, investor relations page) is stored as a Valkey hash.
        The key is generated based on the company name.

        Args:
            company_name: The name of the company this data belongs to.
            site_discovery_result: The `SiteDiscoveryResponse` object containing
                                   the website links.
        """
        if all([v is None for v in site_discovery_result.model_dump().values()]):
            return
        k = CompanySiteStore.__create_key(company_name)

        self.client.client.hset(k, mapping=site_discovery_result.model_dump(exclude_none=True))

    def get_companies(self) -> list[str]:
        """Retrieves a list of all company names for which site discovery data is stored.

        Returns:
            list[str]: A list of company names.
        """
        keys = self.client.client.keys('site_discovery:*')
        return [k.split(':')[-1] for k in keys]

    def get(self, company: str) -> SiteDiscoveryResponse| None:
        """Retrieves the stored site discovery information for a specific company.

        Args:
            company: The name of the company.

        Returns:
            SiteDiscoveryResponse | None: The site information as a `SiteDiscoveryResponse`
                                          object, or None if not found or if the record is empty.
        """
        res = self.client.client.hgetall(CompanySiteStore.__create_key(company))
        if res is None:
            return
        return SiteDiscoveryResponse(
            official_website_link=res.get('official_website_link'),
            investor_relations_page=res.get('investor_relations_page'),
        )


    @staticmethod
    def __create_key(company_name: str) -> str:
        """Creates a standardized Valkey key for storing site discovery data.

        Args:
            company_name: The name of the company. Use '*' for pattern matching.

        Returns:
            A formatted string to be used as a Valkey key
        """
        return f'site_discovery:{company_name}'

class ModelActionStore:
    """
    Stores and manages sequences of actions taken by an AI model during tasks
    like web crawling (e.g., finding financial reports). It tracks the actions,
    the URL navigation queue, and whether a task is considered "done".
    """
    def __init__(
        self,
        valkey_client: valkey_utils.ValkeyClient,
    ) -> None:
        """Initializes the ModelActionStore.

        Args:
            valkey_client: An initialized ValkeyClient instance.
        """
        self.valkey_client = valkey_client

    def store(
        self,
        company_name: str,
        url: str,
        model_action: ModelActionResponseWithMetadata,
        mark_done: bool = False,
    ) -> None:
        """Stores a model's action related to a specific company and URL.

        - The `model_action` itself is stored in a hash keyed by company and URL.
        - If the action is 'visit', the target URL is added to a sorted set (`urlqueue`)
          representing the navigation history/path, scored by timestamp.
        - If the action is 'back', the most recent URL is removed from the `urlqueue`.
        - If `mark_done` is True (typically for 'done' or 'abort' actions), a separate key
          is set to point to this action's key, marking the completion of the task for the company.

        Args:
            company_name: The name of the company.
            url: The URL of the page the model was viewing when it decided on this action.
            model_action: The `ModelActionResponseWithMetadata` object describing the action.
            mark_done: If True, marks this action as the final one for the company's
                       current crawling task. Defaults to False.
        """
        k = ModelActionStore.__create_key(company_name, url)
        self.valkey_client.client.hset(
            k,
            mapping=model_action.model_dump(exclude_none=True),
        )

        urlq_k = ModelActionStore.__create_urlqueue_key(company_name)
        if model_action.action == 'visit':
            self.valkey_client.client.zadd(urlq_k, mapping={url:model_action.action_ts_ms})
        elif model_action.action == 'back':
            current_url = self.get_current_url(company_name)
            if current_url is not None:
                self.valkey_client.client.zrem(urlq_k, current_url)

        if not mark_done:
            return
        ck = ModelActionStore.__create_done_key(company_name)
        self.valkey_client.client.set(ck, k)

    def get(self,company: str, url:str) -> ModelActionResponseWithMetadata | None:
        """Retrieves a specific model action for a company, URL, and timestamp.

        Args:
            company: The name of the company.
            url: The URL associated with the action.
            timestamp_ms: The timestamp (in ms) when the action was recorded, used to uniquely identify it.

        Returns:
            ModelActionResponseWithMetadata | None: The action object, or None if not found.
        """
        k = ModelActionStore.__create_key(company, url)
        res = self.valkey_client.client.hgetall(k)
        if res is None:
            return res
        return ModelActionResponseWithMetadata.model_validate(res)

    def get_all_actions(
        self,
        company: str
    ) -> list[ModelActionResponseWithMetadata]:
        """Retrieves all stored model actions for a specific company, sorted by timestamp.

        Args:
            company: The name of the company.

        Returns:
            list[ModelActionResponseWithMetadata]: A list of all actions for the company,
                                                  chronologically sorted.
        """
        ks = self.valkey_client.client.keys(ModelActionStore.__create_key(company, '*'))
        p = self.valkey_client.client.pipeline()
        for k in ks:
            p.hgetall(k)
        res = p.execute()
        return [
            ModelActionResponseWithMetadata.model_validate(r)
            for r in res
            if r is not None
        ]

    def del_all(
        self,
        company: str,
    ) -> None:
        """Deletes all model actions, URL queue, and 'done' marker for a specific company.

        Useful for resetting the crawling state for a company before a new attempt.

        Args:
            company: The name of the company.
        """
        ks = self.valkey_client.client.keys(ModelActionStore.__create_key(company, '*'))
        urlk = ModelActionStore.__create_urlqueue_key(company)
        donek = ModelActionStore.__create_done_key(company)
        p = self.valkey_client.client.pipeline()
        for k in ks:
            p.delete(k)
        p.zremrangebyrank(urlk, 0, -1)
        p.delete(donek)
        p.execute()


    def get_current_url(self, company:str) -> str | None:
        """Retrieves the most recent URL from the navigation stack (URL queue) for a company.

        The URL queue is a sorted set scored by timestamp. This gets the URL with the highest score.

        Args:
            company: The name of the company.

        Returns:
            str | None: The current (most recent) URL from the navigation path,
                        or None if the queue is empty.
        """
        urlq_k = ModelActionStore.__create_urlqueue_key(company)
        r = self.valkey_client.client.zrevrange(urlq_k, 0,0,False)
        if not r:
            return None
        return r[0]

    def get_full_url_queue(self, company:str) -> None | list[str]:
        """Retrieves the entire navigation stack (URL queue) for a company, ordered by visit time.

        Args:
            company: The name of the company.

        Returns:
            list[str] | None: A list of visited URLs in chronological order,
                               or None if the queue is empty.
        """
        urlq_k = ModelActionStore.__create_urlqueue_key(company)
        r = self.valkey_client.client.zrange(urlq_k, 0,-1,False)
        if r is None:
            return None
        return r


    def get_done_action(self, company: str) -> ModelActionResponseWithMetadata | None:
        """Retrieves the action that was marked as 'done' or 'abort' for a company.

        This checks the 'done_crawling' marker, which stores the key of the final action.

        Args:
            company: The name of the company.

        Returns:
            ModelActionResponseWithMetadata | None: The final action object, or None if
                                                    no task completion has been marked.
        """
        done_action_key = self.valkey_client.client.get(ModelActionStore.__create_done_key(company))
        if not done_action_key:
            return
        res =self.valkey_client.client.hgetall(done_action_key)
        return ModelActionResponseWithMetadata.model_validate(res)


    @staticmethod
    def __create_key(company: str,url: str) -> str:
        """Creates a Valkey key for a specific model action.
        Args:
            company: Company name.
            url_identifier: URL or a unique identifier for the URL where action was taken.
                            Use '*' for pattern matching.
            timestamp_ms: Timestamp of the action. Use '*' for pattern matching.
        Returns:
            str: Formatted Valkey key.
        """
        return f'model_action:{company}:{url}'

    @staticmethod
    def __create_urlqueue_key(company: str) -> str:
        """Creates the Valkey key for storing the URL navigation queue (sorted set) for a company.
        Args:
            company: Company name.
        Returns:
            str: Formatted Valkey key.
        """
        return f'urlqueue:{company}'

    @staticmethod
    def __create_done_key(company: str) -> str:
        """Creates the Valkey key for the 'done crawling' marker for a company.
        This marker stores the key of the final action ('done' or 'abort').
        Args:
            company: Company name.
        Returns:
            str: Formatted Valkey key.
        """
        return f'done_crawling:{company}'


class AnnualReportLinkStore:
    """
    Stores and manages links to annual financial reports, including local paths
    and GCS links once downloaded/uploaded.
    """
    def __init__(
        self,
        client: valkey_utils.ValkeyClient,
        ) -> None:

        """Initializes the AnnualReportLinkStore.

        Args:
            client: An initialized ValkeyClient instance.
        """
        self.client = client

    def store(
        self,
        company_name:str,
        annual_report_link: AnnualReportLink,
    ) -> None:
        """Stores or updates an annual report link for a company.

        Data is stored in a Valkey hash. Existing entries are overwritten.

        Args:
            company_name: The name of the company.
            annual_report_link: The `AnnualReportLink` (or `AnnualReportLinkWithPaths`)
                                object containing link, refyear, and optionally paths.
        """
        k = AnnualReportLinkStore.__create_key(company_name)
        self.client.client.hset(k, mapping=annual_report_link.model_dump(exclude_none=True))

    def add_gcs_link(
        self,
        company_name: str,
        gcs_link:str,
    ) -> None:
        """Adds or updates the GCS link for a company's stored annual report.

        Args:
            company_name: The name of the company.
            gcs_link: The Google Cloud Storage link to the report file.

        Raises:
            ValueError: If no existing report entry is found for the company to update.
        """
        k = AnnualReportLinkStore.__create_key(company_name)
        rep = self.get(company_name)
        if rep is None or rep.link is None:
            raise ValueError('Report entry does not exist in the db or is invalid')
        self.client.client.hset(k, 'gcs_link', gcs_link)

    def add_local_path(
        self,
        company_name: str,
        local_path: str,
    ) -> None:
        """Adds or updates the local file path for a company's stored annual report.

        Args:
            company_name: The name of the company.
            local_path: The local filesystem path to the downloaded report file.

        Raises:
            ValueError: If no existing report entry is found for the company to update.
        """
        rep = self.get(company_name)
        if rep is None or rep.link is None:
            raise ValueError('Report entry does not exist in the db or is invalid')
        k = AnnualReportLinkStore.__create_key(company_name)
        self.client.client.hset(k, 'local_path', local_path)

    def get(self, company_name: str) -> AnnualReportLink | AnnualReportLinkWithPaths | None:
        """Retrieves annual report link information for a company.

        Returns an `AnnualReportLinkWithPaths` if local or GCS paths are present,
        otherwise an `AnnualReportLink`.

        Args:
            company_name: The name of the company.

        Returns:
            AnnualReportLink | AnnualReportLinkWithPaths | None: The report link information,
                                                                  or None if not found.
        """
        k = AnnualReportLinkStore.__create_key(company_name)
        report = self.client.client.hgetall(k)
        if report is None:
            return 
        if report.get('gcs_link') is not None or report.get('local_path') is not None:
            return AnnualReportLinkWithPaths(
                link=report.get('link'),
                gcs_link=report.get('gcs_link'),
                local_path=report.get('local_path'),
                refyear=report.get('refyear'),
            )
        return AnnualReportLink(
                link=report.get('link'),
                refyear=report.get('refyear'),
            )

    def get_companies(self) -> list[str]:
        """Retrieves a list of all company names for which annual report links are stored.

        Returns:
            list[str]: A list of company names.
        """
        prefix = AnnualReportLinkStore.__create_key('')
        return [
            k.removeprefix(prefix) 
            for k in self.client.client.keys(AnnualReportLinkStore.__create_key('*'))]

    def fill_solution_csv(self, path_to_csv: str, separator:str =';') -> None:
        """Populates a CSV file with found annual report links and reference years.

        This method is designed to update a specific CSV format, likely for a
        competition or data submission task. It reads an existing CSV, updates
        the 'SRC' (source link) and 'REFYEAR' columns for 'FIN_REP' type entries,
        and writes the modified data back to the CSV.

        Args:
            path_to_csv: Path to the CSV file to update.
            separator: CSV delimiter. Defaults to ';'.
        """
        data = pd.read_csv(path_to_csv, sep=separator)
        data['SRC'] = data['SRC'].astype(object)
        data = data.set_index(['NAME', 'TYPE']).sort_index()
        companies = self.get_companies()
        for company in companies:
            link = self.get(company)
            if link is None or link.link is None:
                continue
            data.loc[(company, 'FIN_REP'), 'SRC'] = link.link
            if link.refyear is not None:
                data.loc[(company, 'FIN_REP'), 'REFYEAR'] = link.refyear
        data = data.reset_index().sort_values(by=['ID', 'TYPE'], ascending=[True, True])
        data[['ID', 'NAME', 'TYPE', 'SRC', 'REFYEAR']].to_csv(path_to_csv, sep=separator, na_rep='', index=False)

    @staticmethod
    def __create_key(company_name: str) -> str:
        """Creates a standardized Valkey key for storing annual report link data.
        Args:
            company_name: The name of the company. Use '*' for pattern matching.
        Returns:
            str: Formatted Valkey key (e.g., 'annual_report_link:adecco_group_ag').
        """
        return f'annual_report_link:{company_name}'

class AnnualReportInfoStore:
    """
    Stores and retrieves extracted financial and general information from annual reports
    (e.g., employee count, assets value, main activity) in Valkey.
    """
    def __init__(
        self,
        client: valkey_utils.ValkeyClient,
        ) -> None:
        """Initializes the AnnualReportInfoStore.

        Args:
            client: An initialized ValkeyClient instance.
        """
        self.client = client

    def store(
        self,
        company_name:str,
        annual_report: AnnualReportInfo,
    ) -> None:
        """Stores or updates extracted annual report information for a company.

        Data is stored in a Valkey hash.

        Args:
            company_name: The name of the company.
            annual_report_info: The `AnnualReportInfo` object containing the
                                extracted data.
        """
        k = AnnualReportInfoStore.__create_key(company_name)
        mapping = annual_report.model_dump(exclude_none=True)
        self.client.client.hset(k, mapping=mapping)

    def get(
        self,
        company_name: str,
    ) -> AnnualReportInfo | None:
        """Retrieves extracted annual report information for a company.

        Args:
            company_name: The name of the company.

        Returns:
            AnnualReportInfo | None: The report information as an `AnnualReportInfo`
                                     object, or None if not found.
        """
        k = AnnualReportInfoStore.__create_key(company_name)
        info = self.client.client.hgetall(k)
        if not info:
            return None
        return AnnualReportInfo(
            country_code=info.get('country_code'),
            employee_count=info.get('employee_count'),
            assets_value=info.get('assets_value'),
            currency_code_assets=info.get('currency_code_assets'),
            net_turnover=info.get('net_turnover'),
            currency_code_turnover=info.get('currency_code_turnover'),
            main_activity_description=info.get('main_activity_description'),
        )

    def get_companies(self) -> list[str]:
        """Retrieves a list of all company names for which annual report information is stored.

        Returns:
            list[str]: A list of company names.
        """
        prefix = AnnualReportInfoStore.__create_key('')
        return [
            k.removeprefix(prefix) 
            for k in self.client.client.keys(AnnualReportInfoStore.__create_key('*'))]



    @staticmethod
    def __create_key(company: str) -> str:
        """Creates a standardized Valkey key for storing annual report information.
        Args:
            company: The name of the company. Use '*' for pattern matching.
        Returns:
            str: Formatted Valkey key (e.g., 'annual_report_info:SomeCompany').
        """
        return f'annual_report_info:{company}'

class NaceClassificationStore:
    """
    Stores and retrieves NACE classification codes for companies in Valkey.
    The NACE code is stored as a simple string value.
    """
    def __init__(
        self,
        client: valkey_utils.ValkeyClient,
    ) -> None:
        """Initializes the NaceClassificationStore.

        Args:
            client: An initialized ValkeyClient instance.
        """
        self.client = client

    def store(
        self,
        company_name: str,
        nace_classification: str,
    ) -> None:
        """Stores the NACE classification code for a company.

        Overwrites any existing classification for the company.

        Args:
            company_name: The name of the company.
            nace_classification: The NACE code string (e.g., "C2620").
        """
        k = NaceClassificationStore.__create_key(company_name)
        self.client.client.set(k, nace_classification)

    def get(self,company_name: str) -> str | None:
        """Retrieves the NACE classification code for a company.

        Args:
            company_name: The name of the company.

        Returns:
            str | None: The NACE code string, or None if not found.
        """
        return self.client.client.get(
            NaceClassificationStore.__create_key(company_name),
        )

    @staticmethod
    def __create_key(company: str) -> str:
        """Retrieves a list of all company names for which NACE classifications are stored.

        Returns:
            list[str]: A list of company names.
        """
        return f'nace_classification:{company}'
