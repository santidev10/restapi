from elasticsearch.exceptions import ConnectionTimeout

from utils.elasticsearch import ElasticSearchConnector
from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException
import brand_safety.constants as constants

MAX_SIZE = 10000
BRAND_SAFETY_FLAGGED_THRESHOLD = 70

def get_es_brand_safety_data_helper(doc_ids, index_name, full_response=False):
    if type(doc_ids) is str:
        doc_ids = [doc_ids]
    body = {
        "query": {
            "terms": {
                "_id": doc_ids
            }
        }
    }
    try:
        es_result = ElasticSearchConnector(index_name=index_name) \
            .search(doc_type=constants.BRAND_SAFETY_SCORE_TYPE, body=body, size=MAX_SIZE)
    except ConnectionTimeout:
        return None

    if full_response:
        return es_result
    if len(doc_ids) == 1:
        try:
            es_data = es_result["hits"]["hits"][0]["_source"]
            return es_data
        except IndexError:
            return None
    else:
        es_data = {
            item["_id"]: item["_source"] for item in es_result["hits"]["hits"]
        }
        return es_data


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
                es_data = get_es_brand_safety_data_helper(doc_ids, index_name)
                # Singledb channel data contains brand_safety fields while videos do not
                for item in result.data["items"]:
                    if es_data.get(item["id"]):
                        item["brand_safety"] = es_data[item["id"]]["overall_score"]
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
            channel_es_data = get_es_brand_safety_data_helper(channel_id, index_name=constants.BRAND_SAFETY_CHANNEL_ES_INDEX)
            # need to get all videos for this channel...
            if not channel_es_data:
                return result
            try:
                params = {
                    "fields": "video_id,title,transcript",
                    "sort": "video_id",
                    "size": MAX_SIZE,
                    "channel_id__terms": channel_id
                }
                response = SingleDatabaseApiConnector().get_video_list(params)
                sdb_video_data = {
                    video["vide_id"]: video
                    for video in response["items"]
                }
            except SingleDatabaseApiConnectorException:
                return result
            video_ids = list(sdb_video_data.keys())
            video_es_data = get_es_brand_safety_data_helper(video_ids, constants.BRAND_SAFETY_VIDEO_ES_INDEX)
            brand_safety_data = {
                "total_videos": channel_es_data["videos_scored"],
                "videos_flagged_count": 0,
                "flagged_videos": [],
            }
            for id_, data in video_es_data.items():
                # if flagged, then merge data and append flagged videos
                if data["overall_score"] <= BRAND_SAFETY_FLAGGED_THRESHOLD:
                    brand_safety_data["flagged_videos"].append(data)
                    brand_safety_data["videos_flagged_count"] += 1
            return


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
                    .search(doc_type=constants.BRAND_SAFETY_SCORE_TYPE, body=body, size=MAX_SIZE)
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
