import json
import hashlib

from django.http import Http404
from django.http import StreamingHttpResponse
from rest_framework.status import HTTP_200_OK
from rest_framework.response import Response

from utils.es_components_api_utils import APIViewMixin

from utils.aws.s3_exporter import ReportNotFoundException
from utils.aws.s3_exporter import S3Exporter
from utils.datetime import now_in_default_tz


class S3ExportApiView(APIViewMixin):
    s3_exporter = S3Exporter
    generate_export_task = None

    def post(self, request):
        query_params = self._get_query_params(request)
        query_params.update(request.data)

        export_name = self.generate_report_hash(query_params, request.user.pk)

        if self.s3_exporter.exists(export_name):
            export_url = self._get_url_to_export(export_name)
            return Response(
                data={
                    "export_url": export_url,
                }
            )

        self.generate_export_task.delay(query_params, export_name, [request.user.email])

        return Response(
            data={
                "message": "File is in queue for preparing. After it is finished exporting, "
                           "you will receive message via email.",
                "export_name": export_name
            },
            status=HTTP_200_OK)

    # pylint: disable=unused-argument
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
    # pylint: enable=unused-argument

    def _get_query_params(self, request):
        return request.query_params.dict()

    @staticmethod
    def get_filename(name):
        return f"{name}.csv"


    def _get_url_to_export(self, export_name):
        return self.s3_exporter.generate_temporary_url(export_name)

    @staticmethod
    def generate_report_hash(filters, user_pk):
        _filters = filters.copy()
        _filters["current_datetime"] = now_in_default_tz().date().strftime("%Y-%m-%d")
        _filters["user_id"] = user_pk
        serialzed_filters = json.dumps(_filters, sort_keys=True)
        _hash = hashlib.md5(serialzed_filters.encode()).hexdigest()
        return _hash
