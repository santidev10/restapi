from collections import namedtuple
from typing import Dict
from typing import List

from performiq.analyzers.constants import COERCE_FIELD_FUNCS


def prepare_items(report: List[namedtuple], model, fields_mapping: Dict[str, str],
                  ouath_type: int, defaults: Dict[str, int] = None) -> tuple:
    """
    Prepare items to be updated or created for PerformIQ models. Metric numerical fields returned as string values
        from the API will be coerced using COERCE_FIELD_FUNCS

        fields_mapping dict should contain all Model fields to be updated from report
        ** fields_mapping must contain id key
        Each obj key will be set on the model obj using it's report value
        e.g. Updating Campaign impressions, video_views
            fields_mapping = {
                "impressions": "Impressions",
                "video_views": "VideoViews"
            }
    :param report: iter -> Iterable that contains report rows
    :param model: Model objects to instantiate from report values
    :param fields_mapping: dict -> Mapping of model field to report fields e.g. id: CampaignId
    :param ouath_type: OAuthType enum value
    :param defaults: dict -> Additional attributes to be set on all Model objects
    :return: tuple
    """
    defaults = defaults or {}
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
            obj = exists_mapping[int(getattr(row, fields_mapping["id"]))]
            container = to_update
        except KeyError:
            default = {"oauth_type": ouath_type}
            obj = model(**default)
            container = to_create
        # Set attributes that should be set defined in defaults
        [setattr(obj, model_field, value) for model_field, value in defaults.items()]
        for model_field, report_field in fields_mapping.items():
            coercer = COERCE_FIELD_FUNCS.get(model_field)
            report_value = getattr(row, report_field, None)
            mapped_value = coercer(report_value) if coercer else report_value
            setattr(obj, model_field, mapped_value)
        container.append(obj)
    return to_update, to_create
