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
        "advertise",
        "analyze",
        "charts",
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
    def get_advertise_section(request, format=None):
        from aw_creation.models import AccountCreation, CampaignCreation, AdGroupCreation, AdCreation
        from aw_reporting.demo.models import DemoAccount
        demo_account = DemoAccount()
        demo_campaign = demo_account.children[0]
        demo_ad_group = demo_campaign.children[0]
        demo_ad = demo_ad_group.children[0]

        account = AccountCreation.objects.filter(owner=request.user).first()
        account_id = account.id if account else demo_account.id

        campaign = CampaignCreation.objects.filter(
            account_creation__owner=request.user
        ).first()
        campaign_id = campaign.id if campaign else demo_campaign.id

        ad_group = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=request.user
        ).first()
        ad_group_id = ad_group.id if ad_group else demo_ad_group.id

        ad = AdCreation.objects.filter(
            ad_group_creation__campaign_creation__account_creation__owner=request.user
        ).first()
        ad_id = ad.id if ad else demo_ad.id

        response = OrderedDict([
            ('Options for updating/creating', reverse(
                'aw_creation_urls:creation_options',
                request=request,
                format=format
            )),
            ('Your account creations list', reverse(
                'aw_creation_urls:account_creation_list',
                request=request,
                format=format
            )),
            ("Account creation's details", reverse(
                'aw_creation_urls:account_creation_details',
                args=(account_id,),
                request=request,
                format=format
            )),
            ("Account creation full settings", reverse(
                'aw_creation_urls:account_creation_setup',
                args=(account_id,),
                request=request,
                format=format
            )),
            ("Account's campaigns", reverse(
                'aw_creation_urls:campaign_creation_list_setup',
                args=(account_id,),
                request=request,
                format=format
            )),
            ("Campaign's settings", reverse(
                'aw_creation_urls:campaign_creation_setup',
                args=(campaign_id,),
                request=request,
                format=format
            )),
            ("A list of ad groups of specified campaign", reverse(
                'aw_creation_urls:ad_group_creation_list_setup',
                args=(campaign_id,),
                request=request,
                format=format
            )),
            ("Ad-group details", reverse(
                'aw_creation_urls:ad_group_creation_setup',
                args=(ad_group_id,),
                request=request,
                format=format
            )),
            ("A list of ads of specified ad-group", reverse(
                'aw_creation_urls:ad_creation_list_setup',
                args=(ad_group_id,),
                request=request,
                format=format
            )),
            ("Ad details", reverse(
                'aw_creation_urls:ad_creation_setup',
                args=(ad_id,),
                request=request,
                format=format
            )),
        ])
        return Response(response)

    @staticmethod
    def get_analyze_section(request, format=None):
        demo_pk = "demo"
        connect_account_url = reverse(
            'aw_reporting_urls:connect_aw_account',
            request=request,
            format=format,
        )
        response = OrderedDict([
            ('Connect Your AdWords Account',
             "{}?{}".format(
                 connect_account_url,
                 urlencode(dict(redirect_url=connect_account_url))
             )),
            ('A list of connected aw accounts', reverse(
                'aw_reporting_urls:connect_aw_account_list',
                request=request,
                format=format
            )),
            ('Account list', reverse(
                'aw_reporting_urls:analyze_accounts_list',
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
    def get_charts_section(request, format=None):
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
