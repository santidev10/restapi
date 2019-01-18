from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
import pytz
from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector
from rest_framework.status import HTTP_405_METHOD_NOT_ALLOWED
from utils.csv_export import BaseCSVStreamResponseGenerator
from utils.datetime import now_in_default_tz

class BadWordListApiView(SingledbApiView):
    permission_classes = (IsAdminUser, )
    connector_get = Connector().get_bad_words_list
    connector_post = Connector().post_bad_word


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


class BadWordListExportApiView(SingledbApiView):
    """
    View for exporting bad word list as csv
    """
    permission_classes = (IsAdminUser, )
    connector_get = Connector().get_bad_words_list

    def get(self, request, *args, **kwargs):
        if not hasattr(self, 'connector_get'):
            return Response(status=HTTP_405_METHOD_NOT_ALLOWED)

        # BaseCSVStreamResponseGenerator expects a columns name list and a header dictionary for csv column/header names
        columns = ('Name', 'Category')
        bad_word_list_headers = {
            'Name': 'Name',
            'Category': 'Category'
        }
        bad_word_list = self._connect(request, self.connector_get, **kwargs).data
        csv_generator = BadWordListCSVExport(bad_word_list, columns, bad_word_list_headers)
        return csv_generator.prepare_csv_file_response()


class BadWordListCSVExport(BaseCSVStreamResponseGenerator):
    def __init__(self, bad_word_list, columns, bad_word_list_headers):
        self.bad_word_list = sorted(bad_word_list, key=lambda x: x.get('category'))
        self.columns = columns
        self.bad_word_list_headers = bad_word_list_headers
        super().__init__(self.columns, self.bad_word_generator(), self.bad_word_list_headers)

    def bad_word_generator(self):
        for bad_word in self.bad_word_list:
            row = {
                self.bad_word_list_headers['Name']: bad_word.get('name'),
                self.bad_word_list_headers['Category']: bad_word.get('category'),
            }
            yield row

    def get_filename(self):
        now = now_in_default_tz()
        now_utc = now.astimezone(pytz.utc)
        timestamp = now_utc.strftime("%Y%m%d %H%M%S")
        return "Brand Safety Tags List-{}.csv".format(timestamp)