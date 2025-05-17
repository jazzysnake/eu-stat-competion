import json
import asyncio
import logging

import genai_utils
import models
import valkey_stores

from utils import batched

class ClassificationError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class NaceClassifier:
    def __init__(
        self,
        gen_client: genai_utils.GenaiClient,
        report_info_store: valkey_stores.AnnualReportInfoStore,
        conversation_store: valkey_stores.ConversationStore,
        nace_classification_store: valkey_stores.NaceClassificationStore,
        concurrent_threads: int = 1,
        nace_lvl1_json_path: str = './data/nace/nace2lvl1.json',
        nace_lvl2_json_path: str = './data/nace/nace2lvl2.json',
    ) -> None:
        self.gen_client = gen_client
        self.report_info_store = report_info_store
        self.conversation_store = conversation_store
        self.concurrent_threads = concurrent_threads
        self.classification_store = nace_classification_store
        with open(nace_lvl1_json_path, 'r') as f:
            self.nace_lvl1 = json.load(f)
        with open(nace_lvl2_json_path, 'r') as f:
            self.nace_lvl2 = json.load(f)


    async def run(self) -> None:
        companies = self.report_info_store.get_companies()
        if len(companies) == 0:
            return
        
        for company_batch in batched(companies, self.concurrent_threads):
            tasks = [self.process_company(c) for c in company_batch]
            await asyncio.gather(*tasks)

    async def process_company(
        self,
        company: str,
    ) -> None:
        existing_classification = self.classification_store.get(company)
        if existing_classification is not None:
            return
        info = self.report_info_store.get(company)
        if info is None:
            logging.warning(f'Could not find information for {company}, skipping nace classification')
            return
        if info.main_activity_description is None:
            logging.error(f'Could not find main activity description for {company}, skipping nace classification')
            return
        try:
            logging.info(f'Classifying nace code of {company}')
            classification = await self.classify_company(
                company,
                info.main_activity_description,
            )
            self.classification_store.store(company, classification)
        except Exception as e:
            logging.error(f'Failed to classify nace code of company {company}, cause:{e}',exc_info=True)

    async def classify_company(self, company: str, activity_description: str) -> str:
        prompt = f"""Determine the level 1 nace code based on the below description of the company:
            possible nace codes and their descriptions:
                {json.dumps(self.nace_lvl1,indent='  ')}
            company_description:
                {activity_description}
            """
        msgs = self.gen_client.get_simple_message(prompt)
        self.conversation_store.store(company,'nace_classifiy',msgs)
        response = await self.gen_client.generate(
            msgs,
            model=genai_utils.FLASH,
            response_schema=models.Lvl1ClassificationResponse,
        )
        if response.candidates is None:
            raise genai_utils.GenerationError(f'Failed to generate a single candidate for nace classification (lvl1) for {company}')
        msgs.append(response.candidates[0].content)
        self.conversation_store.store(company,'nace_classify',msgs)
        res = models.Lvl1ClassificationResponse.model_validate_json(response.text)
        next_levels = self.nace_lvl2.get(res.classification)
        if next_levels is None:
            raise ClassificationError(f'The model failed to classify the company {company}')
        
        try:
            prompt = f"""
            Now choose the level 2 classification based on the previously seen description of their activities.
            
            Possible nace codes and their descriptions:
                {json.dumps(next_levels, indent='  ')}
            """
            msgs = msgs + self.gen_client.get_simple_message(prompt)

            response = await self.gen_client.generate(
                msgs,
                model=genai_utils.FLASH,
                response_schema=models.Lvl2ClassificationResponse,
            )

            if response.candidates is None:
                raise genai_utils.GenerationError(f'Failed to generate a single candidate for nace classification (lvl2) for {company}')
            msgs.append(response.candidates[0].content)
            self.conversation_store.store(company,'nace_classify',msgs)
            res2 = models.Lvl2ClassificationResponse.model_validate_json(response.text)
            return f'{res.classification}{res2.classification}'
        except Exception as e:
            logging.warning(f'Lvl2 nace classification failed for {company} keeping lvl1, cause: {e}')
            return res.classification

