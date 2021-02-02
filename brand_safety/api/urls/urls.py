from django.conf.urls import include
from django.conf.urls import url

from .bad_words_urls import urlpatterns as bad_words_urls
from .brand_safety_urls import urlpatterns as brand_safety_urls

urlpatterns = [
    url(r"^bad_words/", include(bad_words_urls)),
    url(r"^brand_safety/", include(brand_safety_urls)),
]
