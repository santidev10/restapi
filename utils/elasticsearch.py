from django.conf import settings

from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import parallel_bulk
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.exceptions import ImproperlyConfigured
from elasticsearch.exceptions import ElasticsearchException


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

    def search(self, **kwargs):
        try:
            index = kwargs.pop("index")
        except KeyError:
            index = self.index_name
        return self.client.search(index=index, **kwargs)

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


class ElasticSearchConnectorException(Exception):
    pass


