from elasticsearch.exceptions import ConnectionTimeout

from utils.elasticsearch import ElasticSearchConnector
import brand_safety.constants as constants


def add_brand_safety_data(view):
    """
    Decorator to merge brand safety data for channels and video from Elasticsearch to Singledb data
    :param view: View handling request for channel / video datas
    :return: Response with merged ES and singledb data
    """
    def wrapper(*args, **kwargs):
        # Get result of view handling request first
        result = view(*args, **kwargs)
        try:
            view_type = args[0].export_file_title
            if result.data.get("items"):
                indexes = {
                    "video": constants.BRAND_SAFETY_VIDEO_ES_INDEX,
                    "channel": constants.BRAND_SAFETY_CHANNEL_ES_INDEX
                }
                try:
                    index_name = indexes[view_type]
                except KeyError:
                    return result
                try:
                    doc_ids = [item["id"] for item in result.data["items"]]
                except KeyError:
                    # Unexpected SDB response
                    return result
                body = {
                    "query": {
                        "terms": {
                            "_id": doc_ids
                        }
                    }
                }
                try:
                    es_result = ElasticSearchConnector(index_name=index_name)\
                        .search(doc_type=constants.BRAND_SAFETY_SCORE_TYPE, body=body, size=10000)
                except ConnectionTimeout:
                    return result
                # Map to dictionary to merge to sdb data
                es_data = {
                    item["_id"]: item["_source"]["overall_score"] for item in es_result["hits"]["hits"]
                }
                # Set response data with new brand safety data
                for item in result.data["items"]:
                    score = es_data.get(item["id"], None)
                    item["brand_safety_data"] = {
                        "score": score,
                        "label": get_brand_safety_label(score)
                    }
        except (IndexError, AttributeError):
            pass
        return result
    return wrapper


def get_brand_safety_label(score):
    try:
        score = int(score)
    except (ValueError, TypeError):
        return None

    if 90 < score <= 100:
        label = "SAFE"
    elif 80 < score <= 90:
        label = "LOW RISK"
    elif 70 < score <= 80:
        label = "RISKY"
    else:
        label = "HIGH RISK"
    return label
