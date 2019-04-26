from django.conf import settings

from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import parallel_bulk


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
        return self.client.search(index=self.index_name, **kwargs)

    def get(self, **kwargs):
        return self.client.get(index=self.index_name, **kwargs)


