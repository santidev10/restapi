import json
import hashlib

from django.conf import settings

from rest_framework.generics import ListAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.reports.tasks import export_pacing_report
from utils.datetime import now_in_default_tz


URL_PATTERN_EXPORT_PACING_REPORT = '{}/api/v1/pacing_report_export/'.format(settings.HOST)


class PacingReportCollectView(ListAPIView, PacingReportHelper):
    permission_classes = tuple()

    def get(self, request, *args, **kwargs):
        filters = request.GET

        report_name = self.generate_report_hash(filters)
        url_to_export = URL_PATTERN_EXPORT_PACING_REPORT + report_name

        export_pacing_report.delay(filters, request.user.pk, report_name, url_to_export)

        return Response(
            data={
                "message": "Report is in queue for preparing. After it is finished exporting, "
                            "you will receive message via email and You might download it using "
                            "<a href='{}'>url</a>".format(url_to_export)
            },
            status=HTTP_200_OK)

    @staticmethod
    def generate_report_hash(filters):
        _filters = filters.copy()
        _filters['current_datetime'] = now_in_default_tz().date().strftime("%Y-%m-%d")
        serialzed_filters = json.dumps(_filters, sort_keys=True)
        _hash = hashlib.md5(serialzed_filters.encode()).hexdigest()
        return _hash
