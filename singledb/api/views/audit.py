from rest_framework.response import Response
from rest_framework.permissions import IsAdminUser
from rest_framework.status import HTTP_405_METHOD_NOT_ALLOWED
from utils.csv_export import BaseCSVStreamResponseGenerator
from utils.datetime import now_in_default_tz
from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector
import pytz
from drf_yasg.utils import swagger_auto_schema


class AuditKeyWordsExportApiView(SingledbApiView):
    """
    Retrieves bad_words_list from singledb and exports as csv (columns = name, category)
    """
    permission_classes = (IsAdminUser,)
    connector_get = Connector().get_bad_words_list

    @swagger_auto_schema(
        operation_description='Exports csv of all audited keywords (name, category)',
        responses={200: 'Brand Safety Tags List-{current_date}.csv'})
    def get(self, request, *args, **kwargs):
        if not hasattr(self, 'connector_get'):
            return Response(status=HTTP_405_METHOD_NOT_ALLOWED)

        # BaseCSVStreamResponseGenerator expects a column names list and a header dictionary for csv columns/headers
        audit_keyword_csv_columns = ['Name', 'Category']
        audit_keyword_csv_headers = {
            'Name': 'Name',
            'Category': 'Category'
        }
        audit_keyword_list = self._connect(request, self.connector_get, **kwargs).data
        csv_generator = AuditKeyWordsCSVExport(keywords=audit_keyword_list,
                                               columns=audit_keyword_csv_columns,
                                               headers=audit_keyword_csv_headers)
        return csv_generator.prepare_csv_file_response()

    # Override methods inherited from SingledbApiView(APIView) to be able to exclude from swagger docs
    # Must use @swagger_auto_schema decorator for drf APIViews
    @swagger_auto_schema(auto_schema=None)
    def post(self, request):
        return Response(status=HTTP_405_METHOD_NOT_ALLOWED)

    @swagger_auto_schema(auto_schema=None)
    def put(self, request):
        return Response(status=HTTP_405_METHOD_NOT_ALLOWED)

    @swagger_auto_schema(auto_schema=None)
    def delete(self, request):
        return Response(status=HTTP_405_METHOD_NOT_ALLOWED)


class AuditKeyWordsCSVExport(BaseCSVStreamResponseGenerator):
    def __init__(self, *args, **kwargs):
        self.keywords = kwargs.get('keywords', [])
        self.keywords = sorted(self.keywords, key=lambda keyword: keyword.get('category', None))
        self.columns = kwargs.get('columns', ('Name', 'Category'))
        self.headers = kwargs.get('headers', {'Name': 'Name', 'Category': 'Category'})
        super().__init__(self.columns, self.keyword_generator(), self.headers)

    def keyword_generator(self):
        for keyword in self.keywords:
            row = {
                self.headers['Name']: keyword.get('name'),
                self.headers['Category']: keyword.get('category'),
            }
            yield row

    def get_filename(self):
        now = now_in_default_tz()
        now_utc = now.astimezone(pytz.utc)
        timestamp = now_utc.strftime("%Y%m%d %H%M%S")
        return "Brand Safety Tags List-{}.csv".format(timestamp)
