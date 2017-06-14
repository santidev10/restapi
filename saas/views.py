"""
Api root view
"""
from collections import OrderedDict
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.views import APIView
from urllib.parse import urlencode


class ApiRootView(APIView):
    """
    Root endpoint
    """
    sections = (
        "create",
        "optimize",
        "analyze",
        "track",
    )

    def get(self, request, format=None):
        """
        Available sections
        """
        section = request.query_params.get("section")
        if section:
            method = getattr(self, "get_{}_section".format(section))
            return method(request, format=format)
        else:
            root_url = reverse('api_root', request=request, format=format)
            sections = [
                (s.capitalize(),
                 "{}?{}".format(root_url, urlencode(dict(section=s))))
                for s in self.sections
            ]
            return Response(OrderedDict(sections))

    @staticmethod
    def get_optimize_section(request, format=None):
        from aw_creation.models import CampaignCreation, AdGroupCreation
        demo_pk = "demo"
        campaigns = CampaignCreation.objects.filter(
            account_creation__owner=request.user
        )[:1]
        campaign_id = campaigns[0].id if campaigns else demo_pk

        ad_groups = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=request.user
        )[:1]
        ad_group_id = ad_groups[0].id if ad_groups else demo_pk

        response = OrderedDict([
            ('Options for updating/creating', reverse(
                'aw_creation_urls:optimization_options',
                request=request,
                format=format
            )),
            ('Your account creations list', reverse(
                'aw_creation_urls:optimization_account_list',
                request=request,
                format=format
            )),
            ("Account creation's details", reverse(
                'aw_creation_urls:optimization_account',
                args=(demo_pk,),
                request=request,
                format=format
            )),
            ("Account's campaigns", reverse(
                'aw_creation_urls:optimization_campaign_list',
                args=(demo_pk,),
                request=request,
                format=format
            )),
            ("Campaign's details", reverse(
                'aw_creation_urls:optimization_campaign',
                args=(campaign_id,),
                request=request,
                format=format
            )),
            ("A list of ad groups of specified campaign", reverse(
                'aw_creation_urls:optimization_ad_group_list',
                args=(campaign_id,),
                request=request,
                format=format
            )),
            ("Ad-group's details", reverse(
                'aw_creation_urls:optimization_ad_group',
                args=(ad_group_id,),
                request=request,
                format=format
            )),
        ])
        return Response(response)

    @staticmethod
    def get_analyze_section(request, format=None):
        demo_pk = "demo"
        response = OrderedDict([
            ('Account list', reverse(
                'aw_reporting_urls:analyze_accounts',
                request=request,
                format=format
            )),
            ('Account campaigns', reverse(
                'aw_reporting_urls:analyze_account_campaigns',
                request=request,
                format=format,
                kwargs={"pk": demo_pk}
            )),
            ('Account details', reverse(
                'aw_reporting_urls:analyze_details',
                request=request,
                format=format,
                kwargs={"pk": demo_pk}
            )),
            ('Account chart', reverse(
                'aw_reporting_urls:analyze_chart',
                request=request,
                format=format,
                kwargs={"pk": demo_pk}
            )),
            ('Account statistic items', reverse(
                'aw_reporting_urls:analyze_chart_items',
                request=request,
                format=format,
                kwargs={"pk": demo_pk, "dimension": "device"}
            )),
            ('Account export', reverse(
                'aw_reporting_urls:analyze_export',
                request=request,
                format=format,
                kwargs={"pk": demo_pk}
            )),
            ('Account export weekly report', reverse(
                'aw_reporting_urls:analyze_export_weekly_report',
                request=request,
                format=format,
                kwargs={"pk": demo_pk}
            )),
        ])
        return Response(response)

    @staticmethod
    def get_track_section(request, format=None):
        response = OrderedDict([
            ('Filters', reverse(
                'aw_reporting_urls:track_filters',
                request=request,
                format=format
            )),
            ('Chart data', reverse(
                'aw_reporting_urls:track_chart',
                request=request,
                format=format
            )),
            ('Table data', reverse(
                'aw_reporting_urls:track_accounts_data',
                request=request,
                format=format
            )),
        ])
        return Response(response)

    @staticmethod
    def get_create_section(request, format=None):
        response = OrderedDict([
            ('Options', reverse(
                'aw_creation_urls:creation_options',
                request=request,
                format=format
            )),
            ('Post account', reverse(
                'aw_creation_urls:creation_account',
                request=request,
                format=format
            )),
            ('List of geo-targets', reverse(
                'aw_creation_urls:geo_target_list',
                request=request,
                format=format
            )),
            ('Import document', reverse(
                'aw_creation_urls:document_to_changes',
                request=request,
                format=format,
                args=("postal_codes",),
            )),
            ('Youtube search', reverse(
                'aw_creation_urls:youtube_video_search',
                request=request,
                format=format,
                args=("My little pony",),
            )),
        ])
        return Response(response)
