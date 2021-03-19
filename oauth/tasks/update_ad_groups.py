from aw_reporting.adwords_reports import ad_group_performance_report

from oauth.models import OAuthAccount
from oauth.constants import OAuthType
from oauth.models import AdGroup
from oauth.utils.client import get_client
from performiq.utils.update import prepare_items
from utils.db.functions import safe_bulk_create


ADGROUP_REPORT_FIELDS_MAPPING = dict(
    id="AdGroupId",
    name="AdGroupName",
    campaign_id="CampaignId",
)


def update_ad_groups(cid, refresh_token):
    client = get_client(
        client_customer_id=cid,
        refresh_token=refresh_token
    )
    report = ad_group_performance_report(client)
    to_create, to_update = prepare_items(report, AdGroup, ADGROUP_REPORT_FIELDS_MAPPING, OAuthType.GOOGLE_ADS.value)
    safe_bulk_create(AdGroup, to_create)
    update_fields = [val for val in ADGROUP_REPORT_FIELDS_MAPPING.keys() if val not in {"id"}]
    AdGroup.objects.bulk_update(to_update, fields=update_fields)
