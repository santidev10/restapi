from django.http import Http404
from django.http import StreamingHttpResponse
from rest_framework.generics import ListAPIView
from rest_framework.status import HTTP_200_OK

from aw_reporting.csv_reports import PacingReportS3Exporter
from utils.aws.s3_exporter import ReportNotFoundException


class PacingReportExportView(ListAPIView):
    permission_classes = tuple()

    def get(self, request, report_name, *args, **kwargs):
        try:
            content_generator = PacingReportS3Exporter.get_s3_export_content(report_name).iter_chunks()
        except ReportNotFoundException:
            raise Http404
        response = StreamingHttpResponse(
            content_generator,
            content_type=PacingReportS3Exporter.export_content_type,
            status=HTTP_200_OK,
        )
        filename = self.get_filename(report_name)
        response["Content-Disposition"] = "attachment; filename={}".format(filename)
        return response

    @staticmethod
    def get_filename(report_name):
        return "PacingReport-{}.csv".format(report_name)
