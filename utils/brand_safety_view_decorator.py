from django.conf import settings

from audit_tool.models import BlacklistItem
import brand_safety.constants as constants
from userprofile.permissions import PermissionGroupNames
from utils.elasticsearch import ElasticSearchConnector
from brand_safety.auditors.utils import AuditUtils
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager


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
                manager = ChannelManager(sections=Sections.BRAND_SAFETY)
            elif constants.VIDEO in view_name:
                index_name = settings.BRAND_SAFETY_VIDEO_INDEX
                blacklist_data_type = BlacklistItem.VIDEO_ITEM
                manager = VideoManager(sections=Sections.BRAND_SAFETY)
            else:
                return response
            if not request.user.groups.filter(name=PermissionGroupNames.BRAND_SAFETY_SCORING).exists():
                return response
            if response.data.get("items"):
                _handle_list_view(request, response, index_name, blacklist_data_type)
            else:
                _handle_single_view(request, response, manager, blacklist_data_type)
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


def get_brand_safety_items(doc_ids, index_name):
    return ElasticSearchConnector().search_by_id(
        index_name,
        doc_ids,
        settings.BRAND_SAFETY_TYPE
    )


def _handle_list_view(request, response, index_name, blacklist_data_type):
    try:
        doc_ids = [item.get("id") or item["main"].get("id") for item in response.data["items"]]
        es_data = get_brand_safety_items(doc_ids, index_name)
        es_scores = {
            _id: data["overall_score"] for _id, data in es_data.items()
        }
        blacklist_items = BlacklistItem.get(doc_ids, blacklist_data_type)
        blacklist_items_by_id = {
            item.item_id: item for item in blacklist_items
        }
        for item in response.data["items"]:
            item_id = item.get("id") or item["main"].get("id")
            score = es_scores.get(item_id, None)
            item["brand_safety_data"] = get_brand_safety_data(score)
            if request.user and (request.user.is_staff or request.user.has_perm("userprofile.flag_audit")):
                try:
                    blacklist_data = blacklist_items_by_id[item_id].to_dict()
                except KeyError:
                    blacklist_data = ""
                item["blacklist_data"] = blacklist_data
    except (TypeError, KeyError):
        return


def _handle_single_view(request, response, manager, blacklist_data_type):
    doc_id = response.data.get("id") or response.data["main"].get("id")
    brand_safety_data = AuditUtils.get_items([doc_id], manager)
    # Catch trying to access index and accessing brand safety attribute
    try:
        brand_safety_score = brand_safety_data[0].brand_safety.overall_score
    except (IndexError, AttributeError):
        brand_safety_score = None
    response.data["brand_safety_data"] = get_brand_safety_data(brand_safety_score)
    if request.user and (request.user.is_staff or request.user.has_perm("userprofile.flag_audit")):
        try:
            blacklist_data = BlacklistItem.get(doc_id, blacklist_data_type, to_dict=True)[0]
        except IndexError:
            blacklist_data = ""
        response.data["blacklist_data"] = blacklist_data


def add_brand_safety(items, manager):
    doc_ids = [item.meta.id for item in items]
    try:
        brand_safety_data = AuditUtils.get_items(doc_ids, manager)
        brand_safety_scores = {}
        for doc in brand_safety_data:
            try:
                brand_safety_scores[doc.main.id] = doc.brand_safety.overall_score
            except AttributeError:
                continue
        for item in items:
            score = brand_safety_scores.get(item.meta.id, None)
            item.brand_safety_data = get_brand_safety_data(score)
    except (TypeError, KeyError):
        return items
    return items
