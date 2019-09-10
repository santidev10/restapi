import json
import hashlib

from django.http import Http404
from django.http import StreamingHttpResponse
from rest_framework.status import HTTP_200_OK

from utils.es_components_api_utils import APIViewMixin

from utils.aws.s3_exporter import ReportNotFoundException
from utils.aws.s3_exporter import S3Exporter
from utils.datetime import now_in_default_tz


class S3ExportApiView(APIViewMixin):
    s3_exporter = S3Exporter
    generate_export_task = None

    def post(self, request):
        query_params = request.query_params.dict()
        export_name = self.generate_report_hash(query_params, request.user.pk)
        self.generate_export_task.delay(query_params, export_name, request.user.emails)
        return

    def get(self, request, export_name, *args, **kwargs):
        try:
            content_generator = self.s3_exporter.get_s3_export_content(export_name).iter_chunks()
        except ReportNotFoundException:
            raise Http404
        response = StreamingHttpResponse(
            content_generator,
            content_type=self.s3_exporter.export_content_type,
            status=HTTP_200_OK,
        )
        filename = self.get_filename(export_name)
        response["Content-Disposition"] = "attachment; filename={}".format(filename)
        return response

    @staticmethod
    def get_filename(name):
        return f"{name}.csv"

    @staticmethod
    def generate_report_hash(filters, user_pk):
        _filters = filters.copy()
        _filters['current_datetime'] = now_in_default_tz().date().strftime("%Y-%m-%d")
        _filters['user_id'] = user_pk
        serialzed_filters = json.dumps(_filters, sort_keys=True)
        _hash = hashlib.md5(serialzed_filters.encode()).hexdigest()
        return _hash