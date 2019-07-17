import os

ELASTIC_SEARCH_URLS = os.getenv("ELASTIC_SEARCH_URLS", "https://vpc-chf-elastic-prod-3z4o4k53pvrephzhaqhunzjeyu.us-east-1.es.amazonaws.com").split(",")
ELASTIC_SEARCH_TIMEOUT = int(os.getenv("ELASTIC_SEARCH_TIMEOUT", "10"))
ELASTIC_SEARCH_USE_SSL = os.getenv("ELASTIC_SEARCH_USE_SSL", "1") == "1"
