from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.response import Response

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
            query_params = args[1].query_params
            brand_safety_type = query_params.get("brand_safety")
            if brand_safety_type is not None:
                indexes = {
                    "video": constants.BRAND_SAFETY_VIDEO_ES_INDEX,
                    "channel": constants.BRAND_SAFETY_CHANNEL_ES_INDEX
                }
                try:
                    index_name = indexes[brand_safety_type]
                except KeyError:
                    response = Response(
                        status=HTTP_400_BAD_REQUEST,
                        data="Invalid brand_safety param: {}. Must either be video or channel.".format(brand_safety_type)
                    )
                    return response
                doc_ids = [item["id"] for item in result.data["items"]]
                body = {
                    "query": {
                        "terms": {
                            "_id": doc_ids
                        }
                    }
                }
                es_result = ElasticSearchConnector(index_name=index_name)\
                    .search(doc_type=constants.BRAND_SAFETY_SCORE_TYPE, body=body)
                # Map to dictionary to merge to singledb data
                es_data = {
                    item["_source"]["_id"]: item for item in es_result["hits"]["hits"]
                }
                for item in result.data["items"]:
                    item["brand_safety_score"] = es_data.get(item["id"], "Unavailable")
        except IndexError:
            pass
        return result
    return wrapper

