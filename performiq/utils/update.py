import operator

from performiq.models import Campaign

from performiq.models.constants import OAuthType
from performiq.utils.adwords_report import get_client
from performiq.utils.adwords_report import get_report
from utils.db.functions import safe_bulk_create
from aw_reporting.models.ad_words.calculations import get_ctr
from aw_reporting.models.ad_words.calculations import get_average_cpv
from aw_reporting.models.ad_words.calculations import get_average_cpm



def update(account_id, fields, field_mapping, report_query, model):
    """

    :param client: Adwords client instantiated from get_web_app_client
    :param fields: list -> Adwords report fields to fetch
    :param field_mapping: dict -> Postgres column fields to Adwords report field for the current report
        {
            "id": "CampaignId",
            "impressions": "Impressions",
            "video_views": "VideoViews",
            ...
        }
    :param report_query: str -> Adwords Query Language query string
    :param model: Postgres model to create / update items
    :return:
    """
    client = get_client(account_id)
    report = get_report(client, report_query, fields)
    # Not all report fields will be used in field_mapping, limit mapping for
    # setting values on model objects and for bulk_update operation
    fields_mapping = {
        obj_field: report_field for obj_field, report_field in field_mapping.items()
        if report_field in fields
    }
    to_update, to_create = prepare_items(report, Campaign, fields_mapping, OAuthType.GOOGLE_ADS.value)
    safe_bulk_create(model, to_create)
    fields_mapping.pop("id")
    model.objects.bulk_update(to_update, fields=fields_mapping.keys())


def prepare_items(report, model, fields_mapping, ouath_type):
    """
    Prepare items to be updated or created
    :param report: iter -> Iterable that contains report rows
    :param model: Model of report being retrieved
    :param fields_mapping: dict -> Mapping of model field to report fields e.g. id: CampaignId
    :param ouath_type: OAuthType enum value
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
            default = {"oauth_type": ouath_type}
            # Prepare model obj instantiation values by mapping report fields to obj fields
            values = {
                obj_field: getattr(row, report_field)
                for obj_field, report_field in fields_mapping.items()
            }
            obj = model(**default, **values)
            container = to_create
        container.append(obj)
    return to_update, to_create


def _add_computed_fields(obj):
    """ Add calculated fields not returned from Adwords API """
    values = {
        "ctr": get_ctr(obj.clicks, obj.impressions) if obj.impressions > 0 else 0,
        "cpm": get_average_cpm(obj.cost, obj.impressions) if obj.impressions > 0 else 0,
        "cpv": get_average_cpv(obj.cost, obj.video_views) if obj.video_views > 0 else 0,
    }


def _safe_calculate(a, b, operation):
    """

    :param a:
    :param b:
    :param operation: add, div, prod
    :return:
    """
    try:
        if operation == "prod":
            result = a * b
        else:
            func = getattr(operator, operation)
            result = func(a, b)
    except (ZeroDivisionError, TypeError):
        result = 0
    return result
