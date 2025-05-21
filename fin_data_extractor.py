import logging
import asyncio

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
        if not isinstance(link, models.AnnualReportLinkWithPaths):
            logging.warning(f'Skipping data extraction for {company}, as report is not available locally')
            return
        if link is None or link.local_path is None:
            # keep the linter happy, previous condition makes this redundant
            logging.warning(f'Local path is missing, fallback to GCS/download is not yet implemented, skipping company {company}')
            return
        try:
            buffer = None
            with open(link.local_path, 'rb') as f:
                buffer = f.read()

            mime = 'application/pdf' if link.local_path.endswith('.pdf') else 'text/html'
            file = types.Part.from_bytes(data=buffer,mime_type=mime)
            report = await self.extract_data_from_report(company,file)
            self.report_info_store.store(company, report)

        except Exception as e:
            logging.error(f'Failed to extract data from report for company: {company}, cause {e}')

        
    async def extract_data_from_report(
        self,
        company: str,
        report_file: types.Part,
    ) -> models.AnnualReportInfo:
        """
        Extracts financial data from a single report via an LLM.

        The method constructs a prompt for the AI, sends the report file
        and prompt, and then parses the AI's response into an
        AnnualReportInfo model. Conversation history is stored.

        Args:
            company: The name of the company.
            report_file: The report file content as a `google.genai.types.Part`
                         (either PDF or HTML).

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
        msg = self.gen_client.get_simple_message(prompt)
        self.conversation_store.store(
            company,
            'info_extract',
            msg,
        )
        msg[0].parts.append(report_file)
        res = await self.gen_client.generate(
            msg,
            thinking_budget=2048,
            model=genai_utils.PRO,
            response_schema=models.AnnualReportInfo,
        )
        if res.candidates is None:
            raise genai_utils.GenerationError('Failed to generate response')
        msg.append(res.candidates[0].content)
        self.conversation_store.store(
            company,
            'info_extract',
            msg,
        )
        return models.AnnualReportInfo.model_validate_json(res.text)


