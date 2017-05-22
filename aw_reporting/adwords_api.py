import logging
from time import sleep

import yaml
from googleads import adwords, oauth2

logger = logging.getLogger(__name__)
API_VERSION = 'v201609'


def load_settings():
    with open('aw_reporting/google_ads.yaml', 'r') as f:
        conf = yaml.load(f)
    return conf.get('adwords', {})


def get_client(**kwargs):
    api_settings = load_settings()
    api_settings.update(kwargs)
    logger.debug('Start client, settings:', api_settings)
    oauth2_client = oauth2.GoogleRefreshTokenClient(
                                    api_settings.get('client_id'),
                                    api_settings.get('client_secret'),
                                    api_settings.get('refresh_token'))

    try_num = 0
    while True:
        try:
            client_obj = adwords.AdWordsClient(
                api_settings.get('developer_token'),
                oauth2_client,
                user_agent=api_settings.get('user_agent'),
                client_customer_id=api_settings.get('client_customer_id'),
            )
        except Exception as e:
            logger.error("Error: %s" % str(e))
            if try_num < 5:
                try_num += 1
                seconds = try_num ** 4
                logger.info('Sleep for %d seconds' % seconds)
                sleep(seconds)
            else:
                raise
        else:
            return client_obj


def optimize_keyword(query):
    service_client = get_client()
    offset = 0
    page_size = 1000

    targeting_idea_service = service_client.GetService(
        'TargetingIdeaService', version=API_VERSION)
    selector = {
        'searchParameters': [
            {
                'xsi_type': 'RelatedToQuerySearchParameter',
                'queries': list(set(query))
            },
            {
                'xsi_type': 'LocationSearchParameter',
                'locations': [
                    {'id': '2840'}
                ]
            },
            {
                #  Language setting (optional).
                #  The ID can be found in the documentation:
                #  https://developers.google.com/adwords/api/docs/appendix/languagecodes
                'xsi_type': 'LanguageSearchParameter',
                'languages': [{'id': '1000'}]
            },
            {
                # Network search parameter (optional)
                'xsi_type': 'NetworkSearchParameter',
                'networkSetting': {
                    'targetGoogleSearch': True,
                    'targetSearchNetwork': False,
                    'targetContentNetwork': False,
                    'targetPartnerSearchNetwork': False,
                }
            },
        ],
        'ideaType': 'KEYWORD',
        'requestType': 'IDEAS',

        'requestedAttributeTypes': ['KEYWORD_TEXT', 'SEARCH_VOLUME',
                                    'CATEGORY_PRODUCTS_AND_SERVICES',
                                    'AVERAGE_CPC', 'COMPETITION',
                                    'TARGETED_MONTHLY_SEARCHES'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(page_size)
        }
    }
    result_data = []
    more_pages = True

    while more_pages:
        page = targeting_idea_service.get(selector)
        total_count = int(page['totalNumEntries'])

        if 'entries' in page:
            for result in page['entries']:
                keyword_data = {}
                for attribute in result['data']:
                    value = getattr(attribute["value"], "value", None)
                    key = attribute["key"]
                    if key == "CATEGORY_PRODUCTS_AND_SERVICES":
                        keyword_data["interests"] = value or []
                    elif key == "AVERAGE_CPC":
                        v = value.microAmount if value else None
                        keyword_data["average_cpc"] = v / 1000000 \
                            if v else v
                    elif key == "TARGETED_MONTHLY_SEARCHES":
                        keyword_data["monthly_searches"] = sorted([
                           dict(
                               label="%s-%02d" % (v.year, int(v.month)),
                               value=v.count
                           ) for v in value
                           if hasattr(v, 'count')
                        ], key=lambda i: i['label'])
                    elif value:
                        keyword_data[key.lower()] = value

                result_data.append(keyword_data)
        offset += page_size
        selector['paging']['startIndex'] = str(offset)
        more_pages = offset < total_count

    return result_data
