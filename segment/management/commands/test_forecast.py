from django.db.models import F
from django.conf import settings
from django.core.management import BaseCommand

from aw_reporting.adwords_api import get_web_app_client
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager



BATCH_SIZE = 1000

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        client = get_web_app_client(
            refresh_token="1/dFSYu09IZl43oA8pPOLE_NbkSDgO-Wm5LwA_dlkQoWsNoYWpKb856YvPe91IqL9t",
            client_customer_id="3386233102",
        )
        traffic_estimator_service = client.GetService("TrafficEstimatorService", "v201809")
        keywords = [
            {'text': 'new house', 'matchType': 'BROAD'},
            {'text': 'bug removal', 'matchType': 'PHRASE'},
            {'text': 'home tools', 'matchType': 'EXACT'}
        ]
        keyword_estimate_requests = []
        for keyword in keywords:
            keyword_estimate_requests.append({
                'keyword': {
                    'xsi_type': 'Keyword',
                    'matchType': keyword['matchType'],
                    'text': keyword['text']
                }
            })

        # Create ad group estimate requests.
        adgroup_estimate_requests = [{
            'keywordEstimateRequests': keyword_estimate_requests,
            'maxCpc': {
                'xsi_type': 'Money',
                'microAmount': '1000000'
            }
        }]

        # Create campaign estimate requests.
        campaign_estimate_requests = [{
            'campaignId': 9556645600,
            'adGroupEstimateRequests': adgroup_estimate_requests,
            'criteria': [

            ],
        }]

        # Create the selector.
        selector = {
            'campaignEstimateRequests': campaign_estimate_requests,
        }

        # Optional: Request a list of campaign-level estimates segmented by
        # platform.
        # selector['platformEstimateRequested'] = True

        # Get traffic estimates.
        estimates = traffic_estimator_service.get(selector)
        pass