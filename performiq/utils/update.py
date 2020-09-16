from collections import namedtuple
from types import SimpleNamespace

from aw_reporting.models.ad_words.calculations import get_ctr
from aw_reporting.models.ad_words.calculations import get_average_cpv
from aw_reporting.models.ad_words.calculations import get_average_cpm


def prepare_items(report: list, model, fields_mapping: dict, ouath_type: int, computed_fields=None) -> tuple:
    """
    Prepare items to be updated or created
        fields_mapping dict should contain all obj_keys to be updated from report
        ** fields_mapping must contain id key
        Each obj key will be set on the model obj using it's report value
        e.g. Updating Campaign impressions, video_views
            fields_mapping = {
                "impressions": "Impressions",
                "video_views": "VideoViews"
            }
    :param report: iter -> Iterable that contains report rows
    :param model: Model of report being retrieved
    :param fields_mapping: dict -> Mapping of model field to report fields e.g. id: CampaignId
    :param ouath_type: OAuthType enum value
    :param computed_fields: list -> Fields defined in _get_computed_fields for Postgres model instantiation
    :return: tuple
    """
    computed_fields = computed_fields or []
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
        row = _map_fields(row)
        try:
            obj = exists_mapping[int(getattr(row, fields_mapping["id"]))]
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

        # Set computed values for all items
        computed_values = _get_computed_fields(row, computed_fields)
        [
            setattr(obj, obj_key, value) for obj_key, value in computed_values.items()
        ]
        container.append(obj)
    return to_update, to_create


def _get_computed_fields(row: SimpleNamespace, computed_fields: list) -> dict:
    """
    Get calculated fields not returned from Adwords API
    Uses config to define function and arguments to pass to _safe_compute function
    :param computed_fields: Fields to compute and return
    """
    compute_config = {
        "ctr": (get_ctr, "Clicks", "Impressions"),
        "cpm": (get_average_cpm, "Cost", "Impressions"),
        "cpv": (get_average_cpv, "Cost", "VideoViews"),
    }
    values = {}
    for field in computed_fields:
        try:
            config = compute_config[field]
            # Map each stat field to int to pass into config function
            stat_args = [getattr(row, stat) for stat in config[1:]]
            value = _safe_compute(config[0], *stat_args)
            values[field] = value
        except AttributeError:
            pass
    return values


def _safe_compute(func, *args, **_):
    try:
        args = [int(arg) for arg in args]
        value = func(*args) or 0
    except (TypeError, ZeroDivisionError, ValueError):
        value = 0
    return value


def _map_fields(row: namedtuple) -> SimpleNamespace:
    """
    Map values returned from Adwords report for instantiating Postgres models
    :param row: namedtuple
    :return: SimpleNamespace with same keys as row
    """
    mapped_row = SimpleNamespace()
    from_percent = {"ActiveViewViewability", "VideoQuartile100Rate"}
    to_float = {"Impressions", "VideoViews"}
    for field in row._fields:
        if field == "Cost":
            value = float(row.Cost) / 1000000
        elif field in from_percent:
            value = float(getattr(row, field).strip("%"))
        elif field in to_float:
            value = float(getattr(row, field))
        else:
            value = getattr(row, field)
        setattr(mapped_row, field, value)
    return mapped_row
