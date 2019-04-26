from rest_framework.response import Response
from rest_framework.permissions import IsAdminUser
from rest_framework.status import HTTP_405_METHOD_NOT_ALLOWED
from rest_framework_csv.renderers import CSVStreamingRenderer
from utils.api.file_list_api_view import FileListApiView
from utils.datetime import now_in_default_tz
from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector
import pytz
from drf_yasg.utils import swagger_auto_schema


class AuditKeyWordsExportColumn:
    NAME = "Name"
    CATEGORY = "Category"


class AuditKeyWordsCSVRendered(CSVStreamingRenderer):
    header = (AuditKeyWordsExportColumn.NAME, AuditKeyWordsExportColumn.CATEGORY)


class AuditKeyWordsExportApiView(SingledbApiView, FileListApiView):
    """
    Retrieves bad_words_list from singledb and exports as csv (columns = name, category)
    """
    permission_classes = (IsAdminUser,)
    connector_get = Connector().get_bad_words_list
    renderer_classes = (AuditKeyWordsCSVRendered,)

    def get_queryset(self, *args, **kwargs):
        print(self.request)
        audit_keyword_list = self._connect(self.request, self.connector_get, **kwargs).data
        return sorted(audit_keyword_list, key=lambda keyword: keyword.get('category', None))

    def data_generator(self, *args, **kwargs):
        queryset = self.get_queryset(*args, **kwargs)
        for keyword in queryset:
            row = {
                AuditKeyWordsExportColumn.NAME: keyword.get('name'),
                AuditKeyWordsExportColumn.CATEGORY: keyword.get('category'),
            }
            yield row

    @property
    def filename(self):
        now = now_in_default_tz()
        now_utc = now.astimezone(pytz.utc)
        timestamp = now_utc.strftime("%Y%m%d %H%M%S")
        return "Brand Safety Tags List-{}.csv".format(timestamp)

    @swagger_auto_schema(
        operation_description='Exports csv of all audited keywords (name, category)',
        responses={200: 'Brand Safety Tags List-{current_date}.csv'})
    def get(self, request, *args, **kwargs):
        if not hasattr(self, 'connector_get'):
            return Response(status=HTTP_405_METHOD_NOT_ALLOWED)

        return self.list(request, *args, **kwargs)

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
