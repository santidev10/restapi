from .base import AdwordsBase
from utils.utils import chunks_generator

import logging

logger = logging.getLogger(__name__)


#FIXME: The following code is used only for debug purposes and must me removed before the merge --->
def run():
    from audit_tool.management.commands.daily_audit import Command
    cmd = Command()
    cmd.account_ids = AdwordsBlackList.PERMMITTED_ACCOUNTS_IDS
    accounts = cmd.load_accounts()
    bl = AdwordsBlackList(accounts=accounts)
    bl.upload_master_blacklist()
    return bl
#FIXME: <---


class AdwordsBlackList(AdwordsBase):
    API_VERSION = "v201809"
    PAGE_SIZE = 500

    PERMMITTED_ACCOUNTS_IDS = (
        "4050523811",  # FDA: General Markets FY Q2-Q4 2018 OP002689
        "5111891998",  # FDA GM Smokeless USA Q2-Q4 2018 OP002690
        "7561321550",  # Jelly Belly Sports Beans Nice & Company US Q2 - Q4 '18 OP002664
    )

    SHARED_SET_NAME = "Blacklist - Audit Tool"

    def __init__(self, *args, **kwargs):
        accounts = kwargs.pop("accounts")
        assert accounts is not None
        kwargs["accounts"] = [dmo for dmo in accounts if dmo.account_id in self.PERMMITTED_ACCOUNTS_IDS]

        super().__init__(*args, **kwargs)

    def _create_shared_set(self, service):
        logging.info("Create Shared Set")
        shared_set = {
            "name": self.SHARED_SET_NAME,
            "type": "NEGATIVE_PLACEMENTS",
        }

        operations = [{
            'operator': 'ADD',
            'operand': shared_set
        }]

        result = service.mutate(operations)
        return result["value"][0]["sharedSetId"]

    def _get_shared_set_id(self, service):
        logging.info("Get Shared Set ID")
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
        page = service.get(selector)
        entries = page["entries"] if "entries" in page else []
        for entry in entries:
            if entry["name"] == self.SHARED_SET_NAME and entry["status"] == "ENABLED":
                return entry["sharedSetId"]
        return None

    def _remove_shared_set(self, service, shared_set_id):
        logging.info("Remove Shared Set")
        operations = [
            {
                "operator": "REMOVE",
                "operand": {
                    "sharedSetId": shared_set_id,
                }
            }
        ]
        service.mutate(operations)

    def _retrieve_shared_set_video_ids(self, service, shared_set_id):
        logging.info("Retrieve Videos from Shared Set")
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
            page = service.get(selector)
            if "entries" in page:
                for shared_criterion in page["entries"]:
                    if shared_criterion["criterion"]["type"] == "YOUTUBE_VIDEO":
                        video_ids.add(shared_criterion["criterion"]["videoId"])
            offset += self.PAGE_SIZE
            selector['paging']['startIndex'] = offset
        return video_ids

    def _add_shared_set_video_ids(self, service, shared_set_id, video_ids):
        logging.info("Add Videos into Shared Set")
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
        service.mutate(operations)

    def _attach_shared_set_to_campaign(self, service, shared_set_id, campaign_id):
        logging.info("Add Shared Set to Campaign:", campaign_id)
        operations = [
            {
                'operator': 'ADD',
                'operand': {
                    'campaignId': campaign_id,
                    'sharedSetId': shared_set_id
                }
            }
        ]
        service.mutate(operations)

    def _get_campaigns(self, service):
        logging.info("Get campaigns")
        offset = 0
        selector = {
            'fields': ['Id', 'Name', 'Labels', 'Status', 'Type'],
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
            page = service.get(selector)
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
