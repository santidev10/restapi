from rest_framework.generics import ListAPIView

from aw_reporting.api.serializers.pacing_report_opportunities_serializer import \
    PacingReportOpportunitiesSerializer
from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.api.views.pagination import PacingReportOpportunitiesPaginator
from aw_reporting.reports.pacing_report import PacingReport


class PacingReportOpportunitiesApiView(ListAPIView, PacingReportHelper):
    serializer_class = PacingReportOpportunitiesSerializer
    pagination_class = PacingReportOpportunitiesPaginator

    def get_queryset(self):
        report = PacingReport()
        opportunities = report.get_opportunities(self.request.GET, self.request.user)
        sort_by = self.request.GET.get("sort_by")
        if sort_by is not None:
            reverse = False
            if sort_by.startswith("-"):
                reverse = True
                sort_by = sort_by[1:]

            if sort_by == "account":
                sort_by = "name"

            if sort_by in (
                    "margin", "pacing", "plan_cost", "plan_cpm", "plan_cpv",
                    "plan_impressions", "plan_video_views", "cost", "cpm",
                    "cpv", "impressions", "video_views", "name",
            ):
                if sort_by == "name":
                    def sort_key(i):
                        return i[sort_by].lower()
                else:
                    def sort_key(i):
                        return -1 if i[sort_by] is None else i[sort_by]

                opportunities = list(sorted(
                    opportunities, key=sort_key, reverse=reverse,
                ))
        return opportunities
