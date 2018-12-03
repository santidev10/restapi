from rest_framework.permissions import IsAdminUser

from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector


class BadWordRetrieveUpdateDeleteApiView(SingledbApiView):
    permission_classes = (IsAdminUser, )
    connector_get = Connector().get_bad_word
    connector_put = Connector().put_bad_word
    connector_delete = Connector().delete_bad_word


class BadWordHistoryListApiView(SingledbApiView):
    permission_classes = (IsAdminUser, )
    connector_get = Connector().get_bad_words_history_list


class BadWordCategoriesListApiView(SingledbApiView):
    permission_classes = (IsAdminUser, )
    connector_get = Connector().get_bad_words_categories_list
