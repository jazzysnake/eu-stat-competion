import json
import pandas as pd
import datetime as dt

from models import AnnualReportLink, AnnualReportLinkWithPaths, ModelActionResponse, ModelActionResponseWithMetadata, SiteDiscoveryResponse, AnnualReportInfo

import valkey_utils

from typing import Literal

from google.genai import types
from genai_utils import GenaiClient

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

    def store(
        self,
        company_name:str,
        action: Literal['site_find', 'report_find', 'info_extract', 'nace_classify'],
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

        Args:
            company_name: The name of the company.
            action: The action associated with the conversation.

        Returns:
            A formatted string to be used as a Valkey key (e.g., 'conversation:site_find:adecco_group_ag')."""
        return f'conversation:{action}:{company_name}'

class CompanySiteStore:
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
        """Adds a site discovery result to the Valkey store.

        The result is stored as a hash, with each message serialized as JSON.
        The key is generated based on the company name and id.

        Args:
            company_name: The name of the company data belongs to .
            site_discovery_result: The result of looking up the company's website.
        """
        if all([v is None for v in site_discovery_result.model_dump().values()]):
            return
        k = CompanySiteStore.__create_key(company_name)

        self.client.client.hset(k, mapping=site_discovery_result.model_dump(exclude_none=True))

    def get_companies(self) -> list[str]:
        keys = self.client.client.keys('site_discovery:*')
        return [k.split(':')[-1] for k in keys]

    def get(self, company: str) -> SiteDiscoveryResponse| None:
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
            company_name: The name of the company.

        Returns:
            A formatted string to be used as a Valkey key (e.g., 'site_discovery:adecco_group_ag')."""
        return f'site_discovery:{company_name}'

class ModelActionStore:
    def __init__(
        self,
        valkey_client: valkey_utils.ValkeyClient,
    ) -> None:
        self.valkey_client = valkey_client

    def store(
        self,
        company_name: str,
        url: str,
        model_action: ModelActionResponseWithMetadata,
        mark_done: bool = False,
    ) -> None:
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
        k = ModelActionStore.__create_key(company, url)
        res = self.valkey_client.client.hgetall(k)
        if res is None:
            return res
        return ModelActionResponseWithMetadata.model_validate(res)

    def get_all_actions(
        self,
        company: str
    ) -> list[ModelActionResponseWithMetadata]:
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
        urlq_k = ModelActionStore.__create_urlqueue_key(company)
        r = self.valkey_client.client.zrevrange(urlq_k, 0,0,False)
        if not r:
            return None
        return r[0]

    def get_full_url_queue(self, company:str) -> None | list[str]:
        urlq_k = ModelActionStore.__create_urlqueue_key(company)
        r = self.valkey_client.client.zrange(urlq_k, 0,-1,False)
        if r is None:
            return None
        return r


    def get_done_action(self, company: str) -> ModelActionResponseWithMetadata | None:
        done_action_key = self.valkey_client.client.get(ModelActionStore.__create_done_key(company))
        if not done_action_key:
            return
        res =self.valkey_client.client.hgetall(done_action_key)
        return ModelActionResponseWithMetadata.model_validate(res)


    @staticmethod
    def __create_key(company: str,url: str) -> str:
        return f'model_action:{company}:{url}'

    @staticmethod
    def __create_urlqueue_key(company: str) -> str:
        return f'urlqueue:{company}'

    @staticmethod
    def __create_done_key(company: str) -> str:
        return f'done_crawling:{company}'


class AnnualReportLinkStore:
    def __init__(
        self,
        client: valkey_utils.ValkeyClient,
        ) -> None:
        self.client = client

    def store(
        self,
        company_name:str,
        annual_report_link: AnnualReportLink,
    ) -> None:
        k = AnnualReportLinkStore.__create_key(company_name)
        self.client.client.hset(k, mapping=annual_report_link.model_dump(exclude_none=True))

    def add_gcs_link(
        self,
        company_name: str,
        gcs_link:str,
    ) -> None:
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
        rep = self.get(company_name)
        if rep is None or rep.link is None:
            raise ValueError('Report entry does not exist in the db or is invalid')
        k = AnnualReportLinkStore.__create_key(company_name)
        self.client.client.hset(k, 'local_path', local_path)

    def get(self, company_name: str) -> AnnualReportLink | AnnualReportLinkWithPaths | None:
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
        prefix = AnnualReportLinkStore.__create_key('')
        return [
            k.removeprefix(prefix) 
            for k in self.client.client.keys(AnnualReportLinkStore.__create_key('*'))]

    def fill_solution_csv(self, path_to_csv: str, separator:str =';') -> None:
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
        return f'annual_report_link:{company_name}'

class AnnualReportInfoStore:
    def __init__(
        self,
        client: valkey_utils.ValkeyClient,
        ) -> None:
        self.client = client

    def store(
        self,
        company_name:str,
        annual_report: AnnualReportInfo,
    ) -> None:
        k = AnnualReportInfoStore.__create_key(company_name)
        mapping = annual_report.model_dump(exclude_none=True)
        self.client.client.hset(k, mapping=mapping)

    def get(
        self,
        company_name: str,
    ) -> AnnualReportInfo | None:
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
        prefix = AnnualReportInfoStore.__create_key('')
        return [
            k.removeprefix(prefix) 
            for k in self.client.client.keys(AnnualReportInfoStore.__create_key('*'))]



    @staticmethod
    def __create_key(company: str) -> str:
        return f'annual_report_info:{company}'

class NaceClassificationStore:
    def __init__(
        self,
        client: valkey_utils.ValkeyClient,
    ) -> None:
        self.client = client

    def store(
        self,
        company_name: str,
        nace_classification: str,
    ) -> None:
        k = NaceClassificationStore.__create_key(company_name)
        self.client.client.set(k, nace_classification)

    def get(self,company_name: str) -> str | None:
        return self.client.client.get(
            NaceClassificationStore.__create_key(company_name),
        )

    @staticmethod
    def __create_key(company: str) -> str:
        return f'nace_classification:{company}'
