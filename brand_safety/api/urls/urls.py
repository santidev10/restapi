from django.conf.urls import include
from django.conf.urls import url

from .bad_words_urls import urlpatterns as bad_words_urls

urlpatterns = [
    url(r'^bad_words/', include(bad_words_urls)),
]
