import os
import logging
import pandas as pd
import datetime as dt

import valkey_stores


class DataExporter:
    """
    Exports collected company data from Valkey stores into specified CSV formats.

    This class reads template CSV files for "discovery" (e.g., website links, report links)
    and "extraction" (e.g., financial figures, NACE codes), populates them with data
    retrieved from various Valkey stores, and saves the updated CSVs to an output directory.
    """

    def __init__(
        self,
        site_store: valkey_stores.CompanySiteStore,
        report_link_store: valkey_stores.AnnualReportLinkStore,
        report_info_store: valkey_stores.AnnualReportInfoStore,
        nace_store: valkey_stores.NaceClassificationStore,
        discovery_csv_path: str,
        extraction_csv_path: str,
        output_dir: str,
    ) -> None:
        """
        Initializes the DataExporter.

        Args:
            site_store: Valkey store for company website information.
            report_link_store: Valkey store for annual report links.
            report_info_store: Valkey store for extracted annual report information.
            nace_store: Valkey store for NACE classification codes.
            discovery_csv_path: Path to the input CSV template for discovery data.
            extraction_csv_path: Path to the input CSV template for extraction data.
            output_dir: Directory where the populated CSV files will be saved.
        """
        self.site_store = site_store
        self.report_link_store = report_link_store
        self.report_info_store = report_info_store
        self.nace_store = nace_store
        self.discovery_csv_path = discovery_csv_path
        self.extraction_csv_path = extraction_csv_path
        self.output_dir = output_dir
        self.discovery_df = pd.read_csv(discovery_csv_path, sep=';')
        self.extraction_df = pd.read_csv(extraction_csv_path, sep=';')
        self.extraction_df['VALUE'] = self.extraction_df['VALUE'].astype(object)
        self.extraction_df['SRC'] = self.extraction_df['SRC'].astype(object)
        self.extraction_df['CURRENCY'] = self.extraction_df['CURRENCY'].astype(object)

    def run(self) -> None:
        """
        Executes the data export process for both discovery and extraction data.

        Defines output file paths and calls the respective export methods.
        The output directory is created if it doesn't exist.
        """
        extraction_output = os.path.join(self.output_dir, 'extraction.csv')
        discovery_output = os.path.join(self.output_dir, 'discovery.csv')
        self.export_discovery_data(discovery_output)
        self.export_extraction_data(extraction_output)

    def export_discovery_data(self, output_path: str) -> None:
        """
        Populates and exports the discovery data CSV.

        This method fills in details like financial report links, reference years,
        and company website links into a pre-structured "discovery" CSV template.
        Website reference year is set to the current year.

        Args:
            output_path: The file path where the populated discovery CSV will be saved.
        """
        self.discovery_df = self.discovery_df.sort_values(by=['ID', 'TYPE'])
        companies = self.discovery_df.drop_duplicates(subset=['ID'])
        for idx, row in companies.iterrows():
            company: str = row['NAME']

            # fill in financial report link
            report_link = self.report_link_store.get(company)
            report_info = self.report_info_store.get(company)

            report_url = None
            report_refyear = None
            if report_link is not None:
                report_url = report_link.link
                report_refyear = report_link.refyear

            # the later value is usually the more accurate one given how the
            # competition asks for the refyear to be specified
            if report_info is not None and report_info.reference_year is not None:
                if report_refyear is not None and report_info.reference_year > report_refyear:
                    report_refyear = report_info.reference_year

            if report_url is not None and report_refyear is not None:
                self.discovery_df.loc[idx, 'SRC'] = report_url
                self.discovery_df.loc[idx, 'REFYEAR'] = report_refyear

            # fill in site data
            site = self.site_store.get(company)

            site_link = None
            if site is not None:
                site_link = (
                    site.official_website_link
                    if site.official_website_link is not None
                    else site.investor_relations_page
                )
            site_refyear = dt.datetime.today().year
            if site_link is not None:
                if not site_link.startswith('http://') or not site_link.startswith('https://'):
                    site_link = 'https://' + site_link
                self.discovery_df.loc[int(idx) + 1, 'SRC'] = site_link
                self.discovery_df.loc[int(idx) + 1, 'REFYEAR'] = site_refyear

        self.discovery_df.to_csv(output_path, sep=';', index=False)

    def export_extraction_data(self, output_path: str) -> None:
        """
        Populates and exports the extraction data CSV.

        This method fills in detailed extracted data points (e.g., turnover, assets,
        employee count, country, NACE code/activity) into a pre-structured "extraction"
        CSV template. It uses data from various Valkey stores and sets appropriate
        source links and reference years. Website source is hardcoded to 'https://google.com'.

        Args:
            output_path: The file path where the populated extraction CSV will be saved.
        """
        self.extraction_df = self.extraction_df.sort_values(by=['ID'])
        companies = self.extraction_df.drop_duplicates(subset=['ID']).set_index('ID')
        self.extraction_df = self.extraction_df.set_index(['ID', 'VARIABLE']).sort_index()
        current_year = dt.datetime.today().year
        for id, row in companies.iterrows():
            company: str = row['NAME']

            report_link = self.report_link_store.get(company)
            report_info = self.report_info_store.get(company)
            site = self.site_store.get(company)
            nace = self.nace_store.get(company)

            report_refyear = None
            report_src = None
            turnover = None
            assets = None
            website = None
            employees = None
            country = None

            if site is not None:
                website = (
                    site.official_website_link
                    if site.official_website_link is not None
                    else site.investor_relations_page
                )

            if website is not None:
                self.extraction_df.loc[(id, 'WEBSITE'), 'VALUE'] = website
                self.extraction_df.loc[(id, 'WEBSITE'), 'SRC'] = 'https://google.com'
                self.extraction_df.loc[(id, 'WEBSITE'), 'REFYEAR'] = current_year

            if report_link is None:
                logging.warning(f'Failed to fill out any data about {company}')
                continue

            report_refyear = report_link.refyear
            report_src = report_link.link

            if report_info is None:
                continue

            if report_info.reference_year is not None:
                if report_refyear is None or (
                    report_refyear is not None and report_info.reference_year > report_refyear
                ):
                    report_refyear = report_info.reference_year

            if report_refyear is None:
                # no points for data with no reference year
                continue

            turnover = report_info.net_turnover
            turnover_curr = report_info.currency_code_turnover
            assets = report_info.assets_value
            assets_curr = report_info.currency_code_assets
            employees = report_info.employee_count
            country = report_info.country_code

            if country is not None:
                self.extraction_df.loc[(id, 'COUNTRY'), 'VALUE'] = country
                self.extraction_df.loc[(id, 'COUNTRY'), 'REFYEAR'] = report_refyear
                self.extraction_df.loc[(id, 'COUNTRY'), 'SRC'] = report_src

            if turnover is not None and turnover_curr is not None:
                self.extraction_df.loc[(id, 'TURNOVER'), 'VALUE'] = turnover
                self.extraction_df.loc[(id, 'TURNOVER'), 'REFYEAR'] = report_refyear
                self.extraction_df.loc[(id, 'TURNOVER'), 'SRC'] = report_src
                self.extraction_df.loc[(id, 'TURNOVER'), 'CURRENCY'] = turnover_curr

            if assets is not None and assets_curr is not None:
                self.extraction_df.loc[(id, 'ASSETS'), 'VALUE'] = assets
                self.extraction_df.loc[(id, 'ASSETS'), 'REFYEAR'] = report_refyear
                self.extraction_df.loc[(id, 'ASSETS'), 'SRC'] = report_src
                self.extraction_df.loc[(id, 'ASSETS'), 'CURRENCY'] = assets_curr

            if employees is not None:
                self.extraction_df.loc[(id, 'EMPLOYEES'), 'VALUE'] = employees
                self.extraction_df.loc[(id, 'EMPLOYEES'), 'REFYEAR'] = report_refyear
                self.extraction_df.loc[(id, 'EMPLOYEES'), 'SRC'] = report_src

            if nace is not None:
                self.extraction_df.loc[(id, 'ACTIVITY'), 'VALUE'] = nace
                self.extraction_df.loc[(id, 'ACTIVITY'), 'REFYEAR'] = report_refyear
                self.extraction_df.loc[(id, 'ACTIVITY'), 'SRC'] = report_src

        self.extraction_df = self.extraction_df.reset_index()
        self.extraction_df.to_csv(output_path, sep=';', index=False)
