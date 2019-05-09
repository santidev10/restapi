class ElasticSearchConnectorPatcher(object):
    def push_to_index(self, *args, **kwargs):
        pass

    def get(self, *args, **kwargs):
        pass

    def search(self, *args, **kwargs):
        return {}

    def search_by_id(self, *args, **kwargs):
        return {}


def es_monkey_patch():
    import utils.elasticsearch
    utils.elasticsearch.ElasticSearchConnector = ElasticSearchConnectorPatcher


