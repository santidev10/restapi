from performiq.models import Campaign
from performiq.models import OAuthAccount

from googleads import adwords
from aw_reporting.adwords_api import get_web_app_client
from performiq.utils.adwords_report import get_campaigns
from performiq.models.constants import OAuthType
from performiq.utils.constants import CAMPAIGN_FIELDS_MAPPING
from performiq.utils.adwords_report import get_report
from utils.db.functions import safe_bulk_create


def pull_gads_task(account_id):
    # oauth_account = OAuthAccount.objects.get(id=account_id)
    chf = "1//0d48Q1_odkiDjCgYIARAAGA0SNwF-L9IrfX-mcPp6uNFQ_2qXzLU4QoEdp8C2cvcTcaVj6OWwCCcpUSXKqtsGTym7G7QXWB1irLw"
    client = get_web_app_client(
        refresh_token=chf,
        client_customer_id=account_id,
    )
    update_campaigns(client, fields=CAMPAIGN_FIELDS_MAPPING.values())
    pass


def update_campaigns(client, fields, report_query=None):
    report_query = report_query or \
                   (adwords.ReportQueryBuilder()
                        .From("CAMPAIGN_PERFORMANCE_REPORT")
                        .Where("ServingStatus")
                        .EqualTo("SERVING")
                        .Select(*fields)
                        .Build())
    report = get_report(client, report_query, fields)
    # Only use relevant fields in report columns
    fields_mapping = {
        obj_field: report_field for obj_field, report_field in CAMPAIGN_FIELDS_MAPPING.items()
        if report_field in fields
    }
    to_update, to_create = _prepare_items(report, Campaign, fields_mapping)
    safe_bulk_create(Campaign, to_create)
    fields_mapping.pop("id")
    Campaign.objects.bulk_update(to_update, fields=fields_mapping.keys())


def _prepare_items(report, model, fields_mapping):
    """
    Prepare items to be updated or created
    :param report: iter -> Iterable that contains report rows
    :param model: Model of report being retrieved
    :param fields_mapping: dict -> Mapping of model field to report fields e.g. id: CampaignId
    :return: tuple
    """
    exists_mapping = {
        item.id: item
        for item in model.objects.filter(
            oauth_type=0,
            id__in={*[getattr(item, fields_mapping["id"]) for item in report]}
        )
    }
    to_update = []
    to_create = []
    for row in report:
        try:
            obj = exists_mapping[str(getattr(row, fields_mapping["id"]))]
            # Set each report value on existing obj
            for obj_field, report_field in fields_mapping.items():
                report_value = getattr(row, report_field)
                setattr(obj, obj_field, report_value)
            container = to_update
        except KeyError:
            default = {"oauth_type": OAuthType.GOOGLE_ADS.value}
            # Prepare obj field values
            values = {
                obj_field: getattr(row, report_field)
                for obj_field, report_field in fields_mapping.items(),
            }
            obj = model(**default, **values)
            container = to_create
        container.append(obj)
    return to_update, to_create

