from django.conf import settings

from elasticsearch_dsl import connections

connections.create_connection(hosts=[settings.ELASTIC_SEARCH_URL], timeout=20)


