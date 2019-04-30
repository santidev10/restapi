from elasticsearch.exceptions import ConnectionTimeout

from utils.elasticsearch import ElasticSearchConnector
import brand_safety.constants as constants


def brand_safety_es_helper(doc_ids, index_name):
    MAX_SIZE = 10000
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
        es_result = None
    return es_result

# indexes = {
#     constants.VIDEO_RETRIEVE_UPDATE_API_VIEW: constants.BRAND_SAFETY_VIDEO_ES_INDEX,
#     constants.CHANNEL_RETRIEVE_UPDATE_DELETE_API_VIEW: constants.BRAND_SAFETY_CHANNEL_ES_INDEX
# }
# try:
#     index_name = indexes[view_type]
# except KeyError:
#     return result
# item_id =


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

    if 90 < score:
        label = "SAFE"
    elif 80 < score <= 90:
        label = "LOW RISK"
    elif 70 < score <= 80:
        label = "RISKY"
    else:
        label = "HIGH RISK"
    return label


class BrandSafetyDataDecorator(object):
    def __init__(self, view):
        self.view = view

    def __call__(self, *args, **kwargs):
        response = self.view(*args, **kwargs)
        test = 'ehe'


    def add_brand_safety_data(self, view):
        """
        Decorator to merge brand safety data for channels and video from Elasticsearch to Singledb data
        :param view: View handling request for channel / video datas
        :return: Response with merged ES and singledb data
        """

        def wrapper(self, *args, **kwargs):
            # Get result of view handling request first
            result = view(*args, **kwargs)
            try:
                view_name = args[0].__class__.__name__.lower()
                # if constants.CHANNEL in view_name:
                #     index_name = constants.BRAND_SAFETY_CHANNEL_ES_INDEX
                # elif constants.VIDEO in view_name:
                #     index_name = constants.BRAND_SAFETY_VIDEO_ES_INDEX
                # else:
                #     return result
                #
                # if result.data.get("items"):
                #     try:
                #         doc_ids = [item["id"] for item in result.data["items"]]
                #     except KeyError:
                #         # Unexpected SDB response
                #         return result
                # else:
                #     doc_ids =
                #
                # if result.data.get("items"):
                #     try:
                #         doc_ids = [item["id"] for item in result.data["items"]]
                #     except KeyError:
                #         # Unexpected SDB response
                #         return result
                # else:
                #     try:
                #         doc_ids = result.data["id"]
                #     except KeyError:
                #         # Unexpected SDB response
                #         return result
                #
                #     es_result = brand_safety_es_helper(doc_ids, index_name)
                    # try:
                    #     es_result = ElasticSearchConnector(index_name=index_name)\
                    #         .search(doc_type=constants.BRAND_SAFETY_SCORE_TYPE, body=body, size=10000)
                    # except ConnectionTimeout:
                    #     return result
                    # # Map to dictionary to merge to sdb data
                    # es_data = {
                    #     item["_id"]: item["_source"]["overall_score"] for item in es_result["hits"]["hits"]
                    # }
                    # # Set response data with new brand safety data
                    # for item in result.data["items"]:
                    #     score = es_data.get(item["id"], None)
                    #     item["brand_safety_data"] = {
                    #         "score": score,
                    #         "label": get_brand_safety_label(score)
                    #     }
            except (IndexError, AttributeError):
                pass
            return result

        return wrapper

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
            view_name = args[0].__class__.__name__.lower()
            # if constants.CHANNEL in view_name:
            #     index_name = constants.BRAND_SAFETY_CHANNEL_ES_INDEX
            # elif constants.VIDEO in view_name:
            #     index_name = constants.BRAND_SAFETY_VIDEO_ES_INDEX
            # else:
            #     return result
            #
            # if result.data.get("items"):
            #     try:
            #         doc_ids = [item["id"] for item in result.data["items"]]
            #     except KeyError:
            #         # Unexpected SDB response
            #         return result
            # else:
            #     doc_ids =
            #
            # if result.data.get("items"):
            #     try:
            #         doc_ids = [item["id"] for item in result.data["items"]]
            #     except KeyError:
            #         # Unexpected SDB response
            #         return result
            # else:
            #     try:
            #         doc_ids = result.data["id"]
            #     except KeyError:
            #         # Unexpected SDB response
            #         return result
            #
            #     es_result = brand_safety_es_helper(doc_ids, index_name)
                # try:
                #     es_result = ElasticSearchConnector(index_name=index_name)\
                #         .search(doc_type=constants.BRAND_SAFETY_SCORE_TYPE, body=body, size=10000)
                # except ConnectionTimeout:
                #     return result
                # # Map to dictionary to merge to sdb data
                # es_data = {
                #     item["_id"]: item["_source"]["overall_score"] for item in es_result["hits"]["hits"]
                # }
                # # Set response data with new brand safety data
                # for item in result.data["items"]:
                #     score = es_data.get(item["id"], None)
                #     item["brand_safety_data"] = {
                #         "score": score,
                #         "label": get_brand_safety_label(score)
                #     }
        except (IndexError, AttributeError):
            pass
        return result

    return wrapper
