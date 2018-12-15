from django.conf.urls import include
from django.conf.urls import url

from .bad_channels_urls import urlpatterns as bad_channels_urls
from .bad_words_urls import urlpatterns as bad_words_urls

urlpatterns = [
    url(r'^bad_words/', include(bad_words_urls)),
    url(r'^bad_channels/', include(bad_channels_urls)),
]
