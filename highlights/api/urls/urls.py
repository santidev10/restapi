from django.conf.urls import url

from highlights.api.views.channels import HighlightChannelsListApiView
from highlights.api.views.channels_export import HighlightChannelsExportApiView
from highlights.api.views.keywords import HighlightKeywordsListApiView
from highlights.api.views.videos import HighlightVideosListApiView
from highlights.api.views.videos_export import HighlightVideosExportApiView
from .names import HighlightsNames

urlpatterns = (
    url(r"^channels/$", HighlightChannelsListApiView.as_view(), name=HighlightsNames.CHANNELS),
    url(r"^channels/export/$", HighlightChannelsExportApiView.as_view(), name=HighlightsNames.CHANNELS_EXPORT),
    url(r"^videos/$", HighlightVideosListApiView.as_view(), name=HighlightsNames.VIDEOS),
    url(r"^videos/export/$", HighlightVideosExportApiView.as_view(), name=HighlightsNames.VIDEOS_EXPORT),
    url(r"^keywords/$", HighlightKeywordsListApiView.as_view(), name=HighlightsNames.KEYWORDS),
)
