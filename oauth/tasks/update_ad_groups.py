import logging

from oauth.models import AdGroup
from oauth.utils.client import get_client
from oauth.utils.adwords import clean_update_fields
from oauth.utils.adwords import get_adgroup_report
from oauth.utils.adwords import prepare_items
from saas import celery_app
from utils.db.functions import safe_bulk_create


logger = logging.getLogger(__name__)


ADGROUP_REPORT_FIELDS_MAPPING = dict(
    id="AdGroupId",
    campaign_id="CampaignId",
    name="AdGroupName",
)

ADGROUP_REPORT_PREDICATES = [
    {"field": "AdGroupStatus", "operator": "EQUALS", "values": ["ENABLED"]},
    {"field": "CampaignStatus", "operator": "EQUALS", "values": ["ENABLED"]},
]


@celery_app.task
def update_adgroups_task(account_id: int, refresh_token: str) -> None:
    """
    Update AdGroups within Google Ads CID
    :param account_id: int -> Account id
    :param refresh_token: str
    """
    client = get_client(client_customer_id=account_id, refresh_token=refresh_token)
    report = get_adgroup_report(client, ADGROUP_REPORT_FIELDS_MAPPING.values(), predicates=ADGROUP_REPORT_PREDICATES)
    to_update, to_create = prepare_items(report, AdGroup, ADGROUP_REPORT_FIELDS_MAPPING)
    safe_bulk_create(AdGroup, to_create, batch_size=50)
    AdGroup.objects.bulk_update(to_update, fields=clean_update_fields(ADGROUP_REPORT_FIELDS_MAPPING.keys()))
