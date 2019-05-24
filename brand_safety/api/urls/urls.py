from django.conf.urls import include
from django.conf.urls import url

from .bad_videos_urls import urlpatterns as bad_videos_urls
from .bad_channels_urls import urlpatterns as bad_channels_urls
from .bad_words_urls import urlpatterns as bad_words_urls
from .brand_safety_urls import urlpatterns as brand_safety_urls


urlpatterns = [
    url(r'^bad_words/', include(bad_words_urls)),
    url(r'^bad_videos/', include(bad_videos_urls)),
    url(r'^bad_channels/', include(bad_channels_urls)),
    url(r'^brand_safety/', include(brand_safety_urls)),
]
