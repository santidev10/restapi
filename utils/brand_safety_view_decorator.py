from django.conf import settings

import brand_safety.constants as constants
from userprofile.permissions import PermissionGroupNames
from utils.elasticsearch import ElasticSearchConnector


def add_brand_safety_data(view):
    """
    Decorator to merge brand safety data for channels and video from Elasticsearch to Singledb data
    :param view: View handling request for channel / video datas
    :return: Response with merged ES and singledb data
    """
    def wrapper(*args, **kwargs):
        # Get result of view handling request first
        response = view(*args, **kwargs)
        try:
            view_name = args[0].__class__.__name__
            # Ensure decorator is being used in appropriate views
            if view_name not in constants.BRAND_SAFETY_DECORATED_VIEWS:
                return response
            view_name = view_name.lower()
            if constants.CHANNEL in view_name:
                index_name = settings.BRAND_SAFETY_CHANNEL_INDEX
            elif constants.VIDEO in view_name:
                index_name = settings.BRAND_SAFETY_VIDEO_INDEX
            else:
                return response

            user = args[1].user
            if not user.groups.filter(name=PermissionGroupNames.BRAND_SAFETY_SCORING).exists():
                return response
            if response.data.get("items"):
                if "id" in response.data.get("items")[0]:
                    _handle_list_view(response, index_name)
                else:
                    if constants.CHANNEL in view_name:
                        _handle_list_view_export(response, index_name, "channel")
                    elif constants.VIDEO in view_name:
                        _handle_list_view_export(response, index_name, "video")
            else:
                _handle_single_view(response, index_name)
        except (IndexError, AttributeError):
            pass
        return response

    return wrapper


def get_brand_safety_label(score):
    """
    Helper method to return appropriate brand safety score label
    :param score: Integer convertible value
    :return: str or None
    """
    try:
        score = int(score)
    except (ValueError, TypeError):
        return None

    if 90 <= score:
        label = constants.SAFE
    elif 80 <= score:
        label = constants.LOW_RISK
    elif 70 <= score:
        label = constants.RISKY
    else:
        label = constants.HIGH_RISK
    return label


def get_brand_safety_data(score):
    label = get_brand_safety_label(score)
    data = {
        "score": score,
        "label": label
    }
    return data


def _handle_list_view(response, index_name):
    try:
        doc_ids = [item["id"] for item in response.data["items"]]
        es_data = ElasticSearchConnector().search_by_id(
            index_name,
            doc_ids,
            settings.BRAND_SAFETY_TYPE
        )
        es_scores = {
            _id: data["overall_score"] for _id, data in es_data.items()
        }
        for item in response.data["items"]:
            score = es_scores.get(item["id"], None)
            item["brand_safety_data"] = get_brand_safety_data(score)
    except (TypeError, KeyError):
        return


def _handle_list_view_export(response, index_name, export_type):
    try:
        doc_ids = []
        for item in response.data["items"]:
            if export_type == "channel":
                doc_ids.append(item["url"].split("/")[-2])
            elif export_type == "video":
                doc_ids.append(item["url"].split("=")[1])
        es_data = ElasticSearchConnector().search_by_id(
            index_name,
            doc_ids,
            settings.BRAND_SAFETY_TYPE
        )
        es_scores = {
            _id: data["overall_score"] for _id, data in es_data.items()
        }
        for item in response.data["items"]:
            if export_type == "channel":
                score = es_scores.get(item["url"].split("/")[-2], None)
            elif export_type == "video":
                score = es_scores.get(item["url"].split("=")[1], None)
            item["brand_safety_score"] = score
    except (TypeError, KeyError):
        return


def _handle_single_view(response, index_name):
    try:
        doc_id = response.data["id"]
        es_data = ElasticSearchConnector().search_by_id(
            index_name,
            doc_id,
            settings.BRAND_SAFETY_TYPE
        )
        score = es_data["overall_score"]
        response.data["brand_safety_data"] = get_brand_safety_data(score)
    except (TypeError, KeyError):
        return
