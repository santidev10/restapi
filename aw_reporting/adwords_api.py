import logging
from time import sleep

import yaml
from googleads import adwords, oauth2

logger = logging.getLogger(__name__)
API_VERSION = 'v201702'


def load_settings():
    with open('aw_reporting/google_ads.yaml', 'r') as f:
        conf = yaml.load(f)
    return conf.get('adwords', {})


def load_web_app_settings():
    with open('aw_reporting/ad_words_web.yaml', 'r') as f:
        conf = yaml.load(f)
    return conf


def get_customers(refresh_token, **kwargs):
    aw_client = get_client(
        client_customer_id=None,
        refresh_token=refresh_token,
        **kwargs
    )
    customer_service = aw_client.GetService(
        'CustomerService', version=API_VERSION
    )
    return customer_service.getCustomers()


def _get_client(developer_token, client_id, client_secret, user_agent,
                refresh_token, client_customer_id=None):
    oauth2_client = oauth2.GoogleRefreshTokenClient(
        client_id, client_secret, refresh_token
    )
    client_obj = adwords.AdWordsClient(
        developer_token,
        oauth2_client,
        user_agent=user_agent,
        client_customer_id=client_customer_id,
    )
    return client_obj


def get_web_app_client(**kwargs):
    api_settings = load_web_app_settings()
    api_settings.update(kwargs)
    return _get_client(**api_settings)


def get_client(**kwargs):
    api_settings = load_settings()
    api_settings.update(kwargs)
    return _get_client(**api_settings)


def optimize_keyword(query, client=None, request_type='IDEAS'):
    service_client = client or get_client()
    request_type = request_type
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
        'requestType': request_type,

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


def get_all_customers(client, page_size=1000, limit=None):
    # Initialize appropriate service.
    managed_customer_service = client.GetService(
        'ManagedCustomerService',
        version=API_VERSION
    )

    offset = 0
    selector = {
        'fields': [
            'CustomerId', 'Name', 'CurrencyCode', 'DateTimeZone',
            'CanManageClients',
        ],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(page_size)
        }
    }
    more_pages = True
    customers = []

    while more_pages:
        page = managed_customer_service.get(selector)
        if 'entries' in page and page['entries']:
            customers += page['entries']

        offset += page_size
        selector['paging']['startIndex'] = str(offset)
        more_pages = offset < int(page['totalNumEntries'])

        if limit and limit >= offset:
            break

    return customers
