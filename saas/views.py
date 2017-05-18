"""
Api root view
"""
from collections import OrderedDict

from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.views import APIView


class ApiRootView(APIView):
    """
    Root endpoint
    """
    permission_classes = tuple()

    def get(self, request, format=None):
        """
        Available endpoints
        """
        demo_pk = "demo"
        response = OrderedDict([
            ('Analyze/Account list', reverse(
                'aw_reporting_urls:analyze_accounts',
                request=request,
                format=format
            )),
            ('Analyze/Account campaigns', reverse(
                'aw_reporting_urls:analyze_account_campaigns',
                request=request,
                format=format,
                kwargs={"pk": demo_pk}
            )),
            ('Analyze/Account details', reverse(
                'aw_reporting_urls:analyze_details',
                request=request,
                format=format,
                kwargs={"pk": demo_pk}
            )),
            ('Analyze/Account chart', reverse(
                'aw_reporting_urls:analyze_chart',
                request=request,
                format=format,
                kwargs={"pk": demo_pk}
            )),
            ('Analyze/Account statistic items', reverse(
                'aw_reporting_urls:analyze_chart_items',
                request=request,
                format=format,
                kwargs={"pk": demo_pk, "dimension": "device"}
            )),
            ('Analyze/Account export', reverse(
                'aw_reporting_urls:analyze_export',
                request=request,
                format=format,
                kwargs={"pk": demo_pk}
            )),
            ('Analyze/Account export weekly report', reverse(
                'aw_reporting_urls:analyze_export_weekly_report',
                request=request,
                format=format,
                kwargs={"pk": demo_pk}
            )),
        ])
        return Response(response, content_type="")