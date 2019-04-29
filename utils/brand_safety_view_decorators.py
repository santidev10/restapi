from utils.elasticsearch import ElasticSearchConnector
import brand_safety.constants as constants


def get_es_brand_safety_data_helper(item_ids, index_name, full_response=False):
    if type(item_ids) is str:
        item_ids = [item_ids]
    body = {
        "query": {
            "terms": {
                "_id": item_ids
            }
        }
    }
    try:
        es_result = ElasticSearchConnector(index_name=index_name) \
            .search(doc_type=constants.BRAND_SAFETY_SCORE_TYPE, body=body, size=10000)
    except 
    if full_response:
        return es_result
    if len(item_ids) == 1:
        try:
            es_result["hits"]["hits"][0]
        except



def add_list_brand_safety_data(view):
    """
    Decorator to merge brand safety data for list channels and video from Elasticsearch to Singledb data
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
                es_result = ElasticSearchConnector(index_name=index_name)\
                    .search(doc_type=constants.BRAND_SAFETY_SCORE_TYPE, body=body, size=10000)
                # Map to dictionary to merge to singledb data
                es_data = {
                    item["_id"]: item["_source"]["overall_score"] for item in es_result["hits"]["hits"]
                }
                # Singledb channel data contains brand_safety fields while videos do not
                for item in result.data["items"]:
                    item["brand_safety"] = es_data.get(item["id"], item.get("brand_safety", "Unavailable")) \
                        if index_name == constants.BRAND_SAFETY_CHANNEL_ES_INDEX \
                        else es_data.get(item["id"], "Unavailable")
        except (IndexError, AttributeError):
            pass
        return result
    return wrapper


def add_channel_brand_safety_data(view):
    """
    Decorator to merge brand safety data for single channel from Elasticsearch to Singledb data
    :param view: Video view
    :return: Response with merged ES and singledb data
    """
    def wrapper(*args, **kwargs):
        # Get result of view handling request first
        result = view(*args, **kwargs)
        try:
            channel_id = args[0].kwargs["pk"]
            body = {
                "query": {
                    "term": {
                        "_id": channel_id
                    }
                }
            }
            es_result = ElasticSearchConnector(index_name=constants.BRAND_SAFETY_CHANNEL_ES_INDEX)\
                .search(doc_type=constants.BRAND_SAFETY_SCORE_TYPE, body=body, size=10000)
            try:
                es_channel_brand_safety_data = es_result["hits"]["hits"][0]
            except IndexError:
                return result
            # Singledb channel data contains brand_safety fields while videos do not
            for item in result.data["items"]:
                item["brand_safety"] = es_data.get(item["id"], item.get("brand_safety", "Unavailable")) \
                    if index_name == constants.BRAND_SAFETY_CHANNEL_ES_INDEX \
                    else es_data.get(item["id"], "Unavailable")
        except (IndexError, AttributeError):
            pass
        return result
    return wrapper


def add_video_brand_safety_data(view):
    """
    Decorator to merge brand safety data for single video from Elasticsearch to Singledb data
    :param view: Video view
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
                es_result = ElasticSearchConnector(index_name=index_name)\
                    .search(doc_type=constants.BRAND_SAFETY_SCORE_TYPE, body=body, size=10000)
                # Map to dictionary to merge to singledb data
                es_data = {
                    item["_id"]: item["_source"]["overall_score"] for item in es_result["hits"]["hits"]
                }
                # Singledb channel data contains brand_safety fields while videos do not
                for item in result.data["items"]:
                    item["brand_safety"] = es_data.get(item["id"], item.get("brand_safety", "Unavailable")) \
                        if index_name == constants.BRAND_SAFETY_CHANNEL_ES_INDEX \
                        else es_data.get(item["id"], "Unavailable")
        except (IndexError, AttributeError):
            pass
        return result
    return wrapper
