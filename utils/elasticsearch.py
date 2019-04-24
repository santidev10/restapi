from django.conf import settings

from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import parallel_bulk

from utils.utils import chunks_generator


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

    def delete_index(self, index_name):
        if self.indices_client.exists(index=index_name):
            self.indices_client.delete(index=index_name)

    def recreate_index(self):
        self.delete_index(self.index_name)
        self.create_index()

    def create_index(self):
        self.indices_client.create(index=self.index_name)

    def update_model_mapping(self, model):
        self.indices_client.put_mapping(index=self.index_name,
                                        doc_type=model.Meta.es_type_name,
                                        body=model.Meta.es_mapping)

    def bulk_data_generator(self, index_type, action, objects):
        for obj in objects:
            data = obj.es_repr()
            metadata = {
                '_index': self.index_name,
                '_type': index_type,
                '_op_type': action,
            }
            data.update(**metadata)
            yield data

    def push_to_index(self, index_type, objects_generator, callback=None):
        for chunk in chunks_generator(objects_generator, self.CHUNK_SIZE):
            chunk = list(chunk)
            data = self.bulk_data_generator(index_type, "index", chunk)
            result = parallel_bulk(client=self.client,
                                   actions=data,
                                   thread_count=self.THREAD_COUNT,
                                   chunk_size=self.CHUNK_SIZE)
            list(result) # required for parallel_bulk
            if callback is not None:
                callback(chunk, result)

    def search(self, **kwargs):
        return self.client.search(index=self.index_name, **kwargs)

    def get(self, **kwargs):
        return self.client.get(index=self.index_name, **kwargs)

    def get_indices(self):
        return [l[11:25] for l in ElasticSearchConnector().client.cat.indices().split('\n') if l]

