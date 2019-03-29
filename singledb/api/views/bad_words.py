# fixme: old bad words. remove it
from django.conf.urls import url
from rest_framework.permissions import IsAdminUser

from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector

__all__ = ["urls"]


class BadWordListApiView(SingledbApiView):
    permission_classes = (IsAdminUser,)
    connector_get = Connector().get_bad_words_list
    connector_post = Connector().post_bad_word


class BadWordRetrieveUpdateDeleteApiView(SingledbApiView):
    permission_classes = (IsAdminUser,)
    connector_get = Connector().get_bad_word
    connector_put = Connector().put_bad_word
    connector_delete = Connector().delete_bad_word


class BadWordHistoryListApiView(SingledbApiView):
    permission_classes = (IsAdminUser,)
    connector_get = Connector().get_bad_words_history_list


class BadWordCategoriesListApiView(SingledbApiView):
    permission_classes = (IsAdminUser,)
    connector_get = Connector().get_bad_words_categories_list


urls = [
    url(r"^bad_words/$", BadWordListApiView.as_view(), name="bw_list"),
    url(r"^bad_words_categories/$", BadWordCategoriesListApiView.as_view(), name="bwc_list"),
    url(r"^bad_words/(?P<pk>.+)/$", BadWordRetrieveUpdateDeleteApiView.as_view(), name="bw_details"),
    url(r"^bad_words_history/$", BadWordHistoryListApiView.as_view(), name="bwh_list")
]
