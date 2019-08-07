from django.conf import settings

from audit_tool.models import BlacklistItem
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
            request = args[1]
            view_name = args[0].__class__.__name__
            # Ensure decorator is being used in appropriate views
            if view_name not in constants.BRAND_SAFETY_DECORATED_VIEWS:
                return response
            view_name = view_name.lower()
            if constants.CHANNEL in view_name:
                index_name = settings.BRAND_SAFETY_CHANNEL_INDEX
                blacklist_data_type = BlacklistItem.CHANNEL_ITEM
            elif constants.VIDEO in view_name:
                index_name = settings.BRAND_SAFETY_VIDEO_INDEX
                blacklist_data_type = BlacklistItem.VIDEO_ITEM
            else:
                return response
            if not request.user.groups.filter(name=PermissionGroupNames.BRAND_SAFETY_SCORING).exists():
                return response
            if response.data.get("items"):
                _handle_list_view(request, response, index_name, blacklist_data_type)
            else:
                _handle_single_view(request, response, index_name, blacklist_data_type)
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


def _handle_list_view(request, response, index_name, blacklist_data_type):
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
        blacklist_items = BlacklistItem.get(doc_ids, blacklist_data_type)
        blacklist_items_by_id = {
            item.item_id: item for item in blacklist_items
        }
        for item in response.data["items"]:
            _id = item["id"]
            score = es_scores.get(_id, None)
            item["brand_safety_data"] = get_brand_safety_data(score)
            if request.user and (request.user.is_staff or request.user.has_perm("userprofile.flag_audit")):
                try:
                    blacklist_data = blacklist_items_by_id[_id].to_dict()
                except KeyError:
                    blacklist_data = None
                item["blacklist_data"] = blacklist_data
    except (TypeError, KeyError):
        return


def _handle_single_view(request, response, index_name, blacklist_data_type):
    try:
        doc_id = response.data["id"]
        es_data = ElasticSearchConnector().search_by_id(
            index_name,
            doc_id,
            settings.BRAND_SAFETY_TYPE
        )
        score = es_data["overall_score"]
        response.data["brand_safety_data"] = get_brand_safety_data(score)
        if request.user and (request.user.is_staff or request.user.has_perm("userprofile.flag_audit")):
            try:
                blacklist_data = BlacklistItem.get(doc_id, blacklist_data_type, to_dict=True)[0]
            except IndexError:
                blacklist_data = None
            response.data["blacklist_data"] = blacklist_data
    except (TypeError, KeyError):
        return
