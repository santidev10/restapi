from django.conf import settings
import certifi

from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import parallel_bulk
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.exceptions import ImproperlyConfigured
from elasticsearch.exceptions import ElasticsearchException
from elasticsearch_dsl import connections


class ElasticSearchConnector(object):
    MAX_RETRIES = 1000
    CHUNK_SIZE = 10000
    THREAD_COUNT = 4

    def __init__(self, index_name=None):
        self.index_name = index_name

        self.client = Elasticsearch(settings.ELASTIC_SEARCH_URLS,
                                    connection_class=RequestsHttpConnection,
                                    max_retries=self.MAX_RETRIES)

        self.indices_client = IndicesClient(client=self.client)

    def push_to_index(self, data_generator):
        result = parallel_bulk(client=self.client,
                               actions=data_generator,
                               thread_count=self.THREAD_COUNT,
                               chunk_size=self.CHUNK_SIZE)
        list(result) # required for parallel_bulk

    def search(self, request_timeout=settings.ELASTIC_SEARCH_REQUEST_TIMEOUT, **kwargs):
        try:
            index = kwargs.pop("index")
        except KeyError:
            index = self.index_name
        return self.client.search(index=index, request_timeout=request_timeout, **kwargs)

    def get(self, **kwargs):
        return self.client.get(index=self.index_name, **kwargs)

    def search_by_id(self, index_name, item_ids, doc_type, full_response=False, size=10000):
        self.index_name = index_name
        body = {
            "query": {
                "terms": {
                    "_id": [item_ids] if type(item_ids) is str else item_ids
                }
            }
        }
        try:
            es_result = self.search(doc_type=doc_type, body=body, size=size)
        except ElasticsearchException:
            raise ElasticSearchConnectorException

        if full_response:
            return es_result
        if type(item_ids) is str:
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

    def scroll(self, query, index=None, doc_type=None, sort_field="overall_score", reverse=True, batches=10, size=2000):
        """
        Generator wrapper for Elasticsearch scroll api
        :param query: Elasticsearch query body
        :param index: Index to query
        :param doc_type: Query doc_type
        :param sort_field: Field to sort on
        :param reverse: reverse=True is descending, else ascending
        :param batches: Number of scroll batches to retrieve
        :param size: Size of each scroll batch
        :return: list
        """
        self.index_name = index if index is not None else self.index_name
        batch_number = 0
        doc_type = settings.BRAND_SAFETY_TYPE if doc_type is None else doc_type
        reverse = "desc" if reverse is True else "asc"
        sort = "{sort_field}:{reverse}".format(sort_field=sort_field, reverse=reverse)
        page = self.client.search(
            index=self.index_name,
            doc_type=doc_type,
            scroll="1m",
            size=size,
            body=query,
            sort=sort,
        )
        hits = page["hits"]["hits"]
        scroll_id = page["_scroll_id"]
        while hits and batch_number < batches:
            yield hits
            batch_number += 1
            page = self.client.scroll(scroll_id=scroll_id, scroll="1m")
            scroll_id = page["_scroll_id"] if page.get("_scroll_id") else scroll_id
            hits = page["hits"]["hits"]


class ElasticSearchConnectorException(Exception):
    pass


def init_es_connection():
    connections.configure(
        default={
            "hosts": settings.ELASTIC_SEARCH_URLS,
            "timeout": settings.ELASTIC_SEARCH_TIMEOUT,
            "use_ssl": settings.ELASTIC_SEARCH_USE_SSL,
            "ca_certs": certifi.where()
        }

    )


