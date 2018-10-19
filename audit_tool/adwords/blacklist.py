import logging
from time import sleep

from django.conf import settings

from utils.utils import chunks_generator
from .base import AdwordsBase

logger = logging.getLogger(__name__)


SLEEP_TIME = 5
RETRIES_COUNT = 5


def safe_run(method, *args, **kwargs):
    for i in range(RETRIES_COUNT):
        try:
            return method(*args, **kwargs)
        except Exception as e:
            logger.error("Error on try {} of {}: ".format(i+1, RETRIES_COUNT))
            logger.error(e)
            if i < RETRIES_COUNT-1:
                logger.error("Sleeping for {} sec".format(SLEEP_TIME))
                sleep(SLEEP_TIME)


class AdwordsBlackList(AdwordsBase):
    API_VERSION = "v201809"
    PAGE_SIZE = 500

    PERMITTED_ACCOUNTS_IDS = settings.AUDIT_TOOL_BLACKLIST_PERMITTED_ACCOUNTS
    SHARED_SET_NAME = "Blacklist - Audit Tool"

    def __init__(self, *args, **kwargs):
        accounts = kwargs.pop("accounts")
        assert accounts is not None
        kwargs["accounts"] = [dmo for dmo in accounts if dmo.account_id in self.PERMITTED_ACCOUNTS_IDS]

        super().__init__(*args, **kwargs)

    def _create_shared_set(self, service):
        shared_set = {
            "name": self.SHARED_SET_NAME,
            "type": "NEGATIVE_PLACEMENTS",
        }

        operations = [{
            'operator': 'ADD',
            'operand': shared_set
        }]

        result = safe_run(service.mutate, operations)
        return result["value"][0]["sharedSetId"]

    def _get_shared_set_id(self, service):
        selector = {
            "fields": ["SharedSetId", "Name", "Type", "Status"],
            "predicates": [
                {
                    "field": "Type",
                    "operator": "EQUALS",
                    "values": "NEGATIVE_PLACEMENTS",
                },
                {
                    "field": "Name",
                    "operator": "EQUALS",
                    "values": self.SHARED_SET_NAME,
                },

            ],
            "paging": {
                "startIndex": 0,
                "numberResults": self.PAGE_SIZE,
            }
        }
        page = safe_run(service.get, selector)
        entries = page["entries"] if "entries" in page else []
        for entry in entries:
            if entry["name"] == self.SHARED_SET_NAME and entry["status"] == "ENABLED":
                return entry["sharedSetId"]
        return None

    def _remove_shared_set(self, service, shared_set_id):
        operations = [
            {
                "operator": "REMOVE",
                "operand": {
                    "sharedSetId": shared_set_id,
                }
            }
        ]
        safe_run(service.mutate(operations))

    def _retrieve_shared_set_video_ids(self, service, shared_set_id):
        video_ids = set()
        selector = {
            "fields": ["SharedSetId", "Id", "PlacementUrl"],
            "predicates": [
                {
                    "field": "SharedSetId",
                    "operator": "EQUALS",
                    "values": shared_set_id,
                }
            ],
            "paging": {
                "startIndex": 0,
                "numberResults": self.PAGE_SIZE,
            }
        }
        offset = 0
        page = {"totalNumEntries": 1}
        while page["totalNumEntries"] > offset:
            page = safe_run(service.get, selector)
            if "entries" in page:
                for shared_criterion in page["entries"]:
                    if shared_criterion["criterion"]["type"] == "YOUTUBE_VIDEO":
                        video_ids.add(shared_criterion["criterion"]["videoId"])
            offset += self.PAGE_SIZE
            selector['paging']['startIndex'] = offset
        return video_ids

    def _add_shared_set_video_ids(self, service, shared_set_id, video_ids):
        shared_criteria = [
            {
                "criterion": {
                    "xsi_type": "YouTubeVideo",
                    "videoId": video_id,
                },
                "negative": True,
                "sharedSetId": shared_set_id,
            } for video_id in video_ids
        ]
        operations = [
            {
                "operator": "ADD",
                "operand": criterion,
            } for criterion in shared_criteria
        ]
        safe_run(service.mutate, operations)

    def _attach_shared_set_to_campaign(self, service, shared_set_id, campaign_id):
        operations = [
            {
                'operator': 'ADD',
                'operand': {
                    'campaignId': campaign_id,
                    'sharedSetId': shared_set_id
                }
            }
        ]
        safe_run(service.mutate, operations)

    def _get_campaigns(self, service):
        offset = 0
        selector = {
            'fields': ['Id', 'Name', 'Labels', 'Status'],
            'ordering': {
                'field': 'Name',
                'sortOrder': 'ASCENDING'
            },
            'paging': {
                'startIndex': str(offset),
                'numberResults': str(self.PAGE_SIZE)
            },
        }

        more_pages = True
        campaigns_ids = set()
        while more_pages:
            page = safe_run(service.get, selector)
            if "entries" in page:
                for entry in page["entries"]:
                    campaigns_ids.add(entry["id"])
            offset += self.PAGE_SIZE
            selector['paging']['startIndex'] = str(offset)
            more_pages = offset < int(page['totalNumEntries'])
        return campaigns_ids

    def upload_master_blacklist(self):
        from ..models import VideoAudit
        queryset = VideoAudit.objects.values_list("video_id", flat=True).distinct()
        videos_ids = set(queryset)

        for account in self.accounts:
            client = account.client

            # get campaigns
            campaign_service = client.GetService("CampaignService")
            campaigns_ids = self._get_campaigns(campaign_service)
            logger.info("Found {} campaign(s) for account: {}".format(len(campaigns_ids), account.account_id))

            # get or create shared set
            shared_set_service = client.GetService("SharedSetService")
            shared_set_id = self._get_shared_set_id(shared_set_service) or self._create_shared_set(shared_set_service)
            shared_criterion_service = client.GetService("SharedCriterionService")

            # add all negative videos into the shared set
            for chunk in chunks_generator(videos_ids, self.PAGE_SIZE):
                self._add_shared_set_video_ids(shared_criterion_service, shared_set_id, chunk)

            # attach the shared set to every campaign
            campaign_shared_set_service = client.GetService("CampaignSharedSetService")
            for campaign_id in campaigns_ids:
                self._attach_shared_set_to_campaign(campaign_shared_set_service, shared_set_id, campaign_id)
