from utils.elasticsearch import ElasticSearchConnector
from utils.elasticsearch import ElasticSearchConnectorException
import brand_safety.constants as constants


def get_es_data(item_ids, index_name):
    """
    Encapsulate getting es data to catch ElasticSearchConnectorException
        On ElasticSearchConnectorException, return it to be handled by view
    :param item_ids: str or list
    :param index_name: str
    :return: dict or ElasticSearchConnectorException
    """
    try:
        es_data = ElasticSearchConnector().search_by_id(
            index_name,
            item_ids,
            constants.BRAND_SAFETY_SCORE_TYPE)
        return es_data
    except ElasticSearchConnectorException:
        return ElasticSearchConnectorException
