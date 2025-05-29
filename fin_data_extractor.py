import json
import logging
import asyncio
import aiofiles

from google.genai import types

import genai_utils
import models
import valkey_stores

from utils import batched

class FinDataExtractor:
    """
    Class that handles financial data extraction from annual reports.
    It uses a generative AI model to parse report files (PDF or HTML)
    and extract key financial figures and company information.
    """
    def __init__(
        self,
        gen_client: genai_utils.GenaiClient,
        conversation_store: valkey_stores.ConversationStore,
        report_link_store: valkey_stores.AnnualReportLinkStore,
        report_info_store: valkey_stores.AnnualReportInfoStore,
        report_directory: str,
        concurrent_threads: int = 1,
    ) -> None:
        """Initializes the FinDataExtractor.

        Args:
            gen_client: Client for generative AI model interaction.
            conversation_store: Store for conversation history with the AI.
            report_link_store: Store for annual report links and local paths.
            report_info_store: Store for storing the extracted financial data.
            report_directory: Directory where report files are stored locally.
            concurrent_threads: Number of concurrent threads for processing companies.
        """
        self.gen_client = gen_client
        self.report_link_store = report_link_store
        self.conversation_store = conversation_store
        self.report_directory = report_directory
        self.report_info_store = report_info_store
        self.concurrent_threads = concurrent_threads


    async def run(self) -> None:
        """
        Runs the data extraction pipeline for all companies that have reports in db.
        It processes companies in batches concurrently.
        """
        companies = self.report_link_store.get_companies()

        for company_batch in batched(companies, self.concurrent_threads):
            tasks = [self.process_company(c) for c in company_batch]
            await asyncio.gather(*tasks)

    async def process_company(self, company: str) -> None:
        """
        Runs the data extraction pipeline for a single company.

        Checks if data has already been extracted. If not, it retrieves the
        local path of the company's annual report, reads the file, and
        then calls the AI model to extract financial data. The extracted
        data is then stored.

        Args:
            company: The name of the company to process.
        """
        extracted_data = self.report_info_store.get(company)
        if extracted_data is not None:
            return
        link = self.report_link_store.get(company)
        if link is None or link.link is None:
            logging.warning(f'Annual report link is missing for company {company}')
            return
        attached_report = None
        if isinstance(link, models.AnnualReportLinkWithPaths) and link.local_path is not None:
            try:
                buffer = None
                async with aiofiles.open(link.local_path, 'rb') as f:
                    buffer = await f.read()

                mime = 'application/pdf' if link.local_path.endswith('.pdf') else 'text/html'
                attached_report = types.Part.from_bytes(data=buffer,mime_type=mime)
            except Exception as e:
                logging.error(f'Failed to read report from disk for company {company}, falling back to url context')
        try:
            if attached_report is None:
                attached_report = link.link
            report = await self.extract_data_from_report(company,attached_report)
            self.report_info_store.store(company, report)

        except Exception as e:
            logging.error(f'Failed to extract data from report for company: {company}, cause {e}')

        
    async def extract_data_from_report(
        self,
        company: str,
        report: types.Part | str,
    ) -> models.AnnualReportInfo:
        """
        Extracts financial data from a single report via an LLM.

        The method constructs a prompt for the AI, sends the report file
        and prompt, and then parses the AI's response into an
        AnnualReportInfo model. Conversation history is stored.

        Args:
            company: The name of the company.
            report: The report file content as a `google.genai.types.Part`
                         (either PDF or HTML) or link to the file.

        Returns:
            models.AnnualReportInfo: An object containing the extracted financial
                                     information.

        Raises:
            genai_utils.GenerationError: If the AI model fails to generate a response.
            pydantic.ValidationError: If the AI's response cannot be validated
                                      against the AnnualReportInfo schema.
        """
        prompt = """Extract the relevant financial data from the attached annual report according to specified in the format.
        
        Notes:
        - Make sure to extract asset values/net turnover in their most expanded integer form. (If the report specifies them in thousands or millions/billions etc, make sure to input the full value)
        - Similarly for employee count, extract the expanded integer forms.
        - Only extract information you explicitly found in the attached document, base your answer on facts.
        - Avoid repeating marketing slop when summarizing the main activity. Look at the facts and collect the main industries and sectors the company participates in (if possible order them by priority).
        """
        use_url_context = False
        if type(report) == str:
            use_url_context = True
            schema = json.dumps(models.AnnualReportInfo.model_json_schema())
            prompt += f""" IMPORTANT!
            - You can find the report at {report}, use tools to view the file content, do not answer without it.
            - Output the your answer in plain json without any formatting according to the following json schema: {schema}
            """
        #if type(report) != str:
        #    raise ValueError('report must be types.Part or str')
        #    
        msg = self.gen_client.get_simple_message(prompt)
        self.conversation_store.store(
            company,
            'info_extract',
            msg,
        )
        if not use_url_context:
            msg[0].parts.append(report)
        res = await self.gen_client.generate(
            msg,
            thinking_budget=2048,
            model=genai_utils.PRO if not use_url_context else 'gemini-2.5-pro-preview-05-06',
            response_schema=models.AnnualReportInfo if not use_url_context else None,
            url_context=use_url_context,
        )
        if res.candidates is None:
            raise genai_utils.GenerationError('Failed to generate response')
        msg.append(res.candidates[0].content)
        self.conversation_store.store(
            company,
            'info_extract',
            msg,
        )
        clean_res = res.text.removeprefix('```json\n').removesuffix('\n```')
        return models.AnnualReportInfo.model_validate_json(clean_res)


