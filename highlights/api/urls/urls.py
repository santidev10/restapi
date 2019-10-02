from django.conf.urls import url

from highlights.api.views.channels import HighlightChannelsListApiView
from highlights.api.views.channels_export import HighlightChannelsExportApiView
from highlights.api.views.keywords import HighlightKeywordsListApiView
from highlights.api.views.keywords_export import HighlightKeywordsExportApiView
from highlights.api.views.videos import HighlightVideosListApiView
from highlights.api.views.videos_export import HighlightVideosExportApiView
from .names import HighlightsNames

urlpatterns = (
    url(r"^channels/$", HighlightChannelsListApiView.as_view(), name=HighlightsNames.CHANNELS),
    url(r"^channels/export/$", HighlightChannelsExportApiView.as_view(), name=HighlightsNames.CHANNELS_PREPARE_EXPORT),
    url(r"^channels/export/(?P<export_name>.+)/$", HighlightChannelsExportApiView.as_view(),
        name=HighlightsNames.CHANNELS_EXPORT),

    url(r"^videos/$", HighlightVideosListApiView.as_view(), name=HighlightsNames.VIDEOS),
    url(r"^videos/export/$", HighlightVideosExportApiView.as_view(), name=HighlightsNames.VIDEOS_PREPARE_EXPORT),
    url(r"^videos/export/(?P<export_name>.+)/$", HighlightVideosExportApiView.as_view(),
        name=HighlightsNames.VIDEOS_EXPORT),

    url(r"^keywords/$", HighlightKeywordsListApiView.as_view(), name=HighlightsNames.KEYWORDS),
    url(r"^keywords/export/$", HighlightKeywordsExportApiView.as_view(), name=HighlightsNames.KEYWORDS_PREPARE_EXPORT),
    url(r"^keywords/export/(?P<export_name>.+)/$", HighlightKeywordsExportApiView.as_view(),
        name=HighlightsNames.KEYWORDS_EXPORT),
)
