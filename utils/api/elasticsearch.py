from elasticsearch_dsl import connections

from es_components.connections import get_es_connection_configurations

es_connection_config = get_es_connection_configurations()
es_connection_config["timeout"] = 20
connections.configure(default=es_connection_config)
