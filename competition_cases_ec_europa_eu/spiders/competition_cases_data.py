import json
import os
from typing import Union

import pandas as pd
import scrapy
from requests_toolbelt import MultipartEncoder
from scrapy import Spider
from scrapy.cmdline import execute
from twisted.internet.defer import Deferred


class CompetitionCasesDataSpider(scrapy.Spider):
    name = "competition_cases_data"

    def __init__(self):
        super().__init__()
        self.data_list = []
        self.headers = None
        self.count = 0

    def start_requests(self):
        multipart_data = MultipartEncoder(
            fields={
                'query': ('blob',
                          '{"bool":{"must":[{"exists":{"field":"caseNumber"}},{"term":{"caseInstrument":"AT"}},{"term":{"metadataType":"METADATA_CASE"}}]}}',
                          'application/json'),
                'sort': ('blob',
                         '[{"field":"caseLastDecisionDate","order":"DESC"},{"field":"metadataReference","order":"DESC"}]',
                         'application/json'),
                'displayFields': ('blob',
                                  '["metadataType","metadataReference","caseNumber","caseNumberPart","caseInstrument","caseType","caseTitle","caseOriginalTitle","caseSectors","caseInitiationDate","caseLegislationSector","caseDg","caseAidCategory","caseMeasureStartDate","caseMeasureEndDate","caseMemberState","caseAidInstruments","caseRegions","casePrimaryLaws","caseSecondaryLaws","caseObjectivesStr","caseCourtCases","caseExpenditures","caseLinks","caseRegulation","caseNotificationDate","caseSimplified","caseDeadlineDate","caseEvents","caseInvestigationPhase","caseCompanies","caseCartel","caseLegalBasis","caseLastDecisionDate","caseDesignations","caseCorePlatformServices","caseConcernedObligations","casePressReleases","caseOfficialJournalPublications","caseOrigin","caseTimelineEvents","caseOfficialJournalPublicationsPublishedDates","casePressReleasesPublicationDates"]',
                                  'application/json'),
            }
        )
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'No-Cache',
            'Connection': 'keep-alive',
            'Origin': 'https://competition-cases.ec.europa.eu',
            'Pragma': 'no-cache',
            'Referer': 'https://competition-cases.ec.europa.eu/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': multipart_data.content_type,  # Set content type
        }
        yield scrapy.Request(
            url='https://api.tech.ec.europa.eu/search-api/prod/rest/search?text=*&pageNumber=1&pageSize=1000&apiKey=CS_PROD_ODSE_PROD',
            method='POST',
            headers=headers,
            body=multipart_data.to_string(),  # Convert MultipartEncoder to string
            callback=self.parse,
            cb_kwargs={'page': 1},
            dont_filter=True
        )

    def parse(self, response, **kwargs):
        page = kwargs['page']
        json_data = json.loads(response.text)
        results = json_data['results']
        if results:
            for data in results:
                data_dict = {}
                data_dict['url'] = "https://competition-cases.ec.europa.eu/cases/" + data['reference']
                data_dict['case_no'] = data['reference']
                data_dict['case_title'] = ''.join(data['metadata']['caseTitle'])
                data_dict['last_decision_date'] = ''.join(data['metadata'].get('caseLastDecisionDate', 'N/A'))
                data_dict['case_companies'] = ' | '.join(data['metadata']['caseCompanies'])

                query_json = {
                    "bool": {
                        "must": [
                            {"terms": {"caseNumber": [data_dict['case_no']]}}
                        ]
                    }
                }

                # Construct 'sort' field as JSON
                sort_json = [{"field": "metadataReference", "order": "DESC"}]

                # Define multipart data with dynamic fields
                multipart_data = MultipartEncoder(
                    fields={
                        'query': ('blob', json.dumps(query_json), 'application/json'),
                        'sort': ('blob', json.dumps(sort_json), 'application/json'),
                    }
                )
                headers = {
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'No-Cache',
                    'Connection': 'keep-alive',
                    'Content-Type': multipart_data.content_type,  # Use generated Content-Type with boundary
                    'Origin': 'https://competition-cases.ec.europa.eu',
                    'Pragma': 'no-cache',
                    'Referer': 'https://competition-cases.ec.europa.eu/',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-site',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                    'X-Requested-With': 'XMLHttpRequest',
                    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                }

                # Send request
                yield scrapy.Request(
                    url='https://api.tech.ec.europa.eu/search-api/prod/rest/search?text=*&pageNumber=1&pageSize=100&apiKey=CS_PROD_ODSE_PROD',
                    method="POST",
                    headers=headers,
                    body=multipart_data.to_string(),
                    callback=self.parse_other_info,
                    cb_kwargs={'data_dict': data_dict}
                )

            multipart_data = MultipartEncoder(
                fields={
                    'query': ('blob',
                              '{"bool":{"must":[{"exists":{"field":"caseNumber"}},{"term":{"caseInstrument":"AT"}},{"term":{"metadataType":"METADATA_CASE"}}]}}',
                              'application/json'),
                    'sort': ('blob',
                             '[{"field":"caseLastDecisionDate","order":"DESC"},{"field":"metadataReference","order":"DESC"}]',
                             'application/json'),
                    'displayFields': ('blob',
                                      '["metadataType","metadataReference","caseNumber","caseNumberPart","caseInstrument","caseType","caseTitle","caseOriginalTitle","caseSectors","caseInitiationDate","caseLegislationSector","caseDg","caseAidCategory","caseMeasureStartDate","caseMeasureEndDate","caseMemberState","caseAidInstruments","caseRegions","casePrimaryLaws","caseSecondaryLaws","caseObjectivesStr","caseCourtCases","caseExpenditures","caseLinks","caseRegulation","caseNotificationDate","caseSimplified","caseDeadlineDate","caseEvents","caseInvestigationPhase","caseCompanies","caseCartel","caseLegalBasis","caseLastDecisionDate","caseDesignations","caseCorePlatformServices","caseConcernedObligations","casePressReleases","caseOfficialJournalPublications","caseOrigin","caseTimelineEvents","caseOfficialJournalPublicationsPublishedDates","casePressReleasesPublicationDates"]',
                                      'application/json'),
                }
            )
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'No-Cache',
                'Connection': 'keep-alive',
                'Origin': 'https://competition-cases.ec.europa.eu',
                'Pragma': 'no-cache',
                'Referer': 'https://competition-cases.ec.europa.eu/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest',
                'Content-Type': multipart_data.content_type,  # Set content type
            }
            yield scrapy.Request(
                url=f'https://api.tech.ec.europa.eu/search-api/prod/rest/search?text=*&pageNumber={page + 1}&pageSize=1000&apiKey=CS_PROD_ODSE_PROD',
                method='POST',
                headers=headers,
                body=multipart_data.to_string(),  # Convert MultipartEncoder to string
                callback=self.parse,
                cb_kwargs={'page': page + 1},
                dont_filter=True
            )

    def parse_other_info(self, response, **kwargs):
        data_dict = kwargs['data_dict']
        json_data = json.loads(response.text)
        results = json_data['results']
        if results:
            attachment_links = []
            for data in results:
                attachment = ' '.join(data['metadata'].get('attachmentLink', ''))
                if attachment:
                    attachment = 'https://ec.europa.eu/competition/antitrust/' + attachment if attachment.endswith(
                        '.pdf') else attachment
                    attachment_links.append(attachment)
            data_dict['attachment_links'] = ' | '.join(attachment_links)
        self.data_list.append(data_dict)

    def close(self, spider: Spider, reason: str):
        df = pd.DataFrame(self.data_list)
        df = df.replace(r'^\s*$', None, regex=True)
        df.fillna('N/A', inplace=True)
        df.insert(0, 'id', range(1, len(df) + 1))
        os.makedirs('../output', exist_ok=True)
        df.to_excel('../output/competition_cases_ec_europa_eu.xlsx', index=False)


if __name__ == '__main__':
    execute(f'scrapy crawl {CompetitionCasesDataSpider.name}'.split())
