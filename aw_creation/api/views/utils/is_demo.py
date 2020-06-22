from aw_creation.models import AdCreation
from aw_creation.models import CampaignCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID


def is_demo(*args, **kwargs):
    str_pk = kwargs.get("pk")
    return str_pk.isnumeric() and int(str_pk) == DEMO_ACCOUNT_ID


def is_demo_ad_creation(*args, **kwargs):
    item_id = kwargs.get("pk")
    return AdCreation.objects.filter(pk=item_id,
                                     ad__ad_group__campaign__account_id=DEMO_ACCOUNT_ID) \
        .exists()


def is_demo_campaign_creation(*args, **kwargs):
    return CampaignCreation.objects.filter(pk=kwargs.get("pk"), campaign__account_id=DEMO_ACCOUNT_ID).exists()
