from django.conf.urls import url

from channel.api.country_view import CountryListApiView
from singledb.api.views.augmentation import AugmentationChannelSegmentListApiView
from singledb.api.views.bad_words import BadWordCategoriesListApiView
from singledb.api.views.bad_words import BadWordHistoryListApiView
from singledb.api.views.bad_words import BadWordListApiView
from singledb.api.views.bad_words import BadWordRetrieveUpdateDeleteApiView
from singledb.api.views.highlights import HighlightChannelsListApiView
from singledb.api.views.highlights import HighlightKeywordsListApiView
from singledb.api.views.highlights import HighlightVideosListApiView

urlpatterns = [
    url(r'^countries/$', CountryListApiView.as_view(), name="countries_list"),
    url(r'^augmentation_segments/$', AugmentationChannelSegmentListApiView.as_view(), name="augmentation_segments"),
    url(r'^highlights/channels/$', HighlightChannelsListApiView.as_view(), name="highlights_channels"),
    url(r'^highlights/videos/$', HighlightVideosListApiView.as_view(), name="highlights_videos"),
    url(r'^highlights/keywords/$', HighlightKeywordsListApiView.as_view(), name="highlights_keywords"),
    url(r"^bad_words/$", BadWordListApiView.as_view(), name="bw_list"),
    url(r"^bad_words_categories/$", BadWordCategoriesListApiView.as_view(), name="bwc_list"),
    url(r"^bad_words/(?P<pk>.+)/$", BadWordRetrieveUpdateDeleteApiView.as_view(), name="bw_details"),
    url(r"^bad_words_history/$", BadWordHistoryListApiView.as_view(), name="bwh_list"),
]
