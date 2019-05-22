import brand_safety.constants as constants
from utils.elasticsearch import ElasticSearchConnector
from userprofile.permissions import PermissionGroupNames


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
                index_name = constants.BRAND_SAFETY_CHANNEL_ES_INDEX
            elif constants.VIDEO in view_name:
                index_name = constants.BRAND_SAFETY_VIDEO_ES_INDEX
            else:
                return response

            user = args[1].user
            if not user.groups.filter(name=PermissionGroupNames.BRAND_SAFETY_SCORING).exists():
                return response
            if response.data.get("items"):
                _handle_list_view(response, index_name)
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
            constants.BRAND_SAFETY_SCORE_TYPE
        )
        es_scores = {
            _id: data["overall_score"] for _id, data in es_data.items()
        }
        for item in response.data["items"]:
            score = es_scores.get(item["id"], None)
            item["brand_safety_data"] = get_brand_safety_data(score)
    except (TypeError, KeyError):
        return


def _handle_single_view(response, index_name):
    try:
        doc_id = response.data["id"]
        es_data = ElasticSearchConnector().search_by_id(
            index_name,
            doc_id,
            constants.BRAND_SAFETY_SCORE_TYPE
        )
        score = es_data["overall_score"]
        response.data["brand_safety_data"] = get_brand_safety_data(score)
    except (TypeError, KeyError):
        return




