import os

import certifi
from elasticsearch_dsl import connections

ELASTIC_SEARCH_URLS = os.getenv("ELASTIC_SEARCH_URLS", "").split(",")
ELASTIC_SEARCH_TIMEOUT = int(os.getenv("ELASTIC_SEARCH_TIMEOUT", "10"))
ELASTIC_SEARCH_USE_SSL = os.getenv("ELASTIC_SEARCH_USE_SSL", "1") == "1"

connections.configure(
    default={
        "hosts": ELASTIC_SEARCH_URLS,
        "timeout": ELASTIC_SEARCH_TIMEOUT,
        "use_ssl": ELASTIC_SEARCH_USE_SSL,
        "ca_certs": certifi.where()
    }

)
