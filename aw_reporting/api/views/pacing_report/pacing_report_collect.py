import json
import hashlib

from django.core.urlresolvers import reverse
from django.conf import settings

from rest_framework.generics import ListAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.api.urls.names import Name
from aw_reporting.api.views.pacing_report.pacing_report_helper import PacingReportHelper
from aw_reporting.reports.tasks import export_pacing_report
from saas.urls.namespaces import Namespace
from utils.datetime import now_in_default_tz
from utils.celery import utils


class PacingReportCollectView(ListAPIView, PacingReportHelper):
    permission_classes = tuple()

    def get(self, request, *args, **kwargs):
        filters = request.GET
        user_pk = request.user.pk
        user_emails = request.GET.getlist("emails")

        if not user_emails:
            return Response(
                data="User emails are not defined",
                status=HTTP_400_BAD_REQUEST
            )

        report_name = self.generate_report_hash(filters, user_pk)
        url_to_export = reverse("{}:{}".format(Namespace.AW_REPORTING, Name.PacingReport.EXPORT), args=(report_name,))

        task_position = utils.get_queue_size("reports") + 1

        export_pacing_report.delay(filters, user_pk, report_name, settings.HOST + url_to_export, user_emails)

        return Response(
            data={
                "message": "Report is in queue for preparing. Task position in queue is {}. After it "
                           "is finished exporting, you will receive message via email.".format(task_position),
                "link": url_to_export
            },
            status=HTTP_200_OK)

    @staticmethod
    def generate_report_hash(filters, user_pk):
        _filters = filters.copy()
        _filters['current_datetime'] = now_in_default_tz().date().strftime("%Y-%m-%d")
        _filters['user_id'] = user_pk
        serialzed_filters = json.dumps(_filters, sort_keys=True)
        _hash = hashlib.md5(serialzed_filters.encode()).hexdigest()
        return _hash
