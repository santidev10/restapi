import csv
import re
from datetime import datetime
from io import StringIO
from django.http import StreamingHttpResponse
from django.db import transaction
from django.db.models import Min, Max, Sum, Count, Q
from rest_framework.generics import ListAPIView
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, \
    HTTP_500_INTERNAL_SERVER_ERROR
from oauth2client import client
from aw_reporting.api.serializers import AWAccountConnectionSerializer, AccountsListSerializer, CampaignListSerializer
from aw_reporting.adwords_api import load_web_app_settings, get_customers
from aw_reporting.demo import demo_view_decorator
from aw_reporting.models import DATE_FORMAT
from aw_reporting.models import AWConnection, Account, AWAccountPermission, Campaign
from aw_reporting.utils import get_google_access_token_info
from aw_reporting.tasks import upload_initial_aw_data
from aw_reporting.charts import DeliveryChart
from utils.api_paginator import CustomPageNumberPaginator


class AccountsListPaginator(CustomPageNumberPaginator):
    page_size = 20


@demo_view_decorator
class AnalyzeAccountsListApiView(ListAPIView):
    """
    Returns a list of user's accounts that were pulled from AdWords
    """

    serializer_class = AccountsListSerializer
    pagination_class = AccountsListPaginator

    def get_queryset(self, **filters):
        sort_by = self.request.query_params.get('sort_by')
        if sort_by != "name":
            sort_by = "-created_at"

        queryset = Account.user_objects(self.request.user).filter(
            can_manage_clients=False,
            # **filters
        ).order_by("name")

        return queryset

    filters = ('status', 'search', 'min_goal_units', 'max_goal_units', 'min_campaigns_count', 'max_campaigns_count',
               'is_changed', 'min_start', 'max_start', 'min_end', 'max_end')

    def get_filters(self):
        filters = {}
        query_params = self.request.query_params
        for f in self.filters:
            v = query_params.get(f)
            if v:
                filters[f] = v
        return filters

    def filter_queryset(self, queryset):
        today = datetime.now().date()
        queryset = queryset.annotate(start=Min("campaigns__start_date"),
                                     end=Max("campaigns__end_date"))

        if self.request.query_params.get('show_closed') != "1":
            queryset = queryset.filter(Q(end__gte=today) | Q(end__isnull=True))

        filters = self.get_filters()
        search = filters.get('search')
        if search:
            queryset = queryset.filter(name__icontains=search)

        status = filters.get('status')
        if status:
            if status == "Ended":
                queryset = queryset.filter(is_ended=True)
            elif status == "Paused":
                queryset = queryset.filter(is_paused=True, is_ended=False)
            elif status == "Pending":
                queryset = queryset.filter(is_approved=False, is_paused=False, is_ended=False)  # all
            else:
                queryset = queryset.annotate(camp_count=Count("account__campaigns"))
                approved_f = dict(is_approved=True, is_paused=False, is_ended=False)
                if status == "Running":
                    queryset = queryset.filter(camp_count__gt=0, **approved_f)
                elif status == "Approved":
                    queryset = queryset.filter(camp_count=0, **approved_f)
                else:
                    queryset = queryset.none()

        min_goal_units = filters.get('min_goal_units')
        max_goal_units = filters.get('max_goal_units')
        if min_goal_units or max_goal_units:
            queryset = queryset.annotate(goal_units=Sum('campaign_creations__goal_units'))
            if min_goal_units:
                queryset = queryset.filter(goal_units__gte=min_goal_units)
            if max_goal_units:
                queryset = queryset.filter(goal_units__lte=max_goal_units)

        min_campaigns_count = filters.get('min_campaigns_count')
        max_campaigns_count = filters.get('max_campaigns_count')
        if min_campaigns_count or max_campaigns_count:
            queryset = queryset.annotate(campaigns_count=Count('campaign_creations'))
            if min_campaigns_count:
                queryset = queryset.filter(campaigns_count__gte=min_campaigns_count)
            if max_campaigns_count:
                queryset = queryset.filter(campaigns_count__lte=max_campaigns_count)

        min_start = filters.get('min_start')
        max_start = filters.get('max_start')
        if min_start or max_start:

            if min_start:
                queryset = queryset.filter(start__gte=min_start)
            if max_start:
                queryset = queryset.filter(start__lte=max_start)

        min_end = filters.get('min_end')
        max_end = filters.get('max_end')
        if min_end or max_end:

            if min_end:
                queryset = queryset.filter(end__gte=min_end)
            if max_end:
                queryset = queryset.filter(end__lte=max_end)

        is_changed = filters.get('is_changed')
        if is_changed:
            queryset = queryset.filter(is_changed=int(is_changed))

        return queryset


@demo_view_decorator
class AnalyzeAccountCampaignsListApiView(ListAPIView):
    """
    Return a list of the account's campaigns/ad-groups
    We use it to build filters
    """
    serializer_class = CampaignListSerializer

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        return Campaign.objects.filter(account_id=pk)


@demo_view_decorator
class AnalyzeDetailsApiView(APIView):
    """
    Send filters to get the account's details

    Body example:
    {}
    or
    {"start": "2017-05-01", "end": "2017-06-01", "campaigns": ["1", "2"], "ad_groups": ["11", "12"]}
    """

    def get_filters(self):
        data = self.request.data
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        filters = dict(
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
            campaigns=data.get("campaigns"),
            ad_groups=data.get("ad_groups"),
        )
        return filters

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


@demo_view_decorator
class AnalyzeChartApiView(APIView):
    """
    Send filters to get data for charts

    Body example:

    {"indicator": "impressions", "dimension": "device"}
    """

    def get_filters(self):
        data = self.request.data
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        filters = dict(
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
            campaigns=data.get("campaigns"),
            ad_groups=data.get("ad_groups"),
            indicator=data.get("indicator", "average_cpv"),
            dimension=data.get("dimension"),
        )
        return filters

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


@demo_view_decorator
class AnalyzeChartItemsApiView(APIView):
    """
    Send filters to get a list of targeted items

    Body example:

    {"segmented": false}
    """

    def get_filters(self):
        data = self.request.data
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        filters = dict(
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
            campaigns=data.get("campaigns"),
            ad_groups=data.get("ad_groups"),
            segmented=data.get("segmented"),
        )
        return filters

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


@demo_view_decorator
class AnalyzeExportApiView(APIView):
    """
    Send filters to download a csv report

    Body example:

    {"campaigns": ["1", "2"]}
    """

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")

    file_name = "{title}-analyze-{timestamp}.csv"

    column_names = (
        "", "Name",  "Impressions", "Views",  "Cost", "Average cpm",
        "Average cpv", "Clicks", "Ctr(i)", "Ctr(v)", "View rate",
        "25%", "50%", "75%", "100%",
    )
    column_keys = (
        'name', 'impressions', 'video_views', 'cost', 'average_cpm',
        'average_cpv', 'clicks', 'ctr', 'ctr_v', 'video_view_rate',
        'video25rate', 'video50rate', 'video75rate', 'video100rate',
    )
    tabs = (
        'device', 'gender', 'age', 'topic', 'interest', 'remarketing',
        'keyword', 'location', 'creative', 'ad', 'channel', 'video',
    )

    def get_filters(self):
        data = self.request.data
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        filters = dict(
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
            campaigns=data.get('campaigns'),
            ad_groups=data.get('ad_groups'),
        )
        return filters

    @staticmethod
    def stream_response_generator(data_generator):
        for row in data_generator():
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(row)
            yield output.getvalue()

    def stream_response(self, item_name, generator):
        generator = self.stream_response_generator(generator)
        response = StreamingHttpResponse(generator,
                                         content_type='text/csv')
        filename = self.file_name.format(
            title=re.sub(r"\W", item_name, '-'),
            timestamp=datetime.now().strftime("%Y%m%d"),
        )
        response['Content-Disposition'] = 'attachment; ' \
                                          'filename="{}"'.format(filename)
        return response


@demo_view_decorator
class AnalyzeExportWeeklyReport(APIView):
    """
    Send filters to download weekly report

    Body example:

    {"campaigns": ["1", "2"]}
    """

    def get_filters(self):
        data = self.request.data
        filters = dict(
            campaigns=data.get('campaigns'),
            ad_groups=data.get('ad_groups'),
        )
        return filters

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


class TrackApiBase(APIView):
    indicators = (
        ('average_cpv', 'CPV'),
        ('average_cpm', 'CPM'),
        ('video_view_rate', 'View Rate'),
        ('ctr', 'CTR(i)'),
        ('ctr_v', 'CTR(v)'),
        ('impressions', 'Impressions'),
        ('video_views', 'Views'),
        ('clicks', 'Clicks'),
        ('cost', 'Costs'),
    )
    breakdowns = (
        ("daily", "Daily"),
        ("hourly", "Hourly"),
    )
    dimensions = (
        ("creative", "Creatives"),
        ("device", "Devices"),
        ("age", "Ages"),
        ("gender", "Genders"),
        ("video", "Top videos"),
        ("channel", "Top channels"),
        ("interest", "Top interests"),
        ("topic", "Top topics"),
        ("keyword", "Top keywords"),
    )

    def get_filters(self):
        data = self.request.query_params
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        filters = dict(
            account=data.get('account'),
            campaign=data.get('campaign'),
            indicator=data.get('indicator', self.indicators[0][0]),
            breakdown=data.get('breakdown'),
            dimension=data.get('dimension'),
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
        )
        return filters


@demo_view_decorator
class TrackFiltersListApiView(TrackApiBase):
    """
    Lists of the filter names and values
    """

    def get_static_filters(self):
        static_filters = dict(
            indicator=[
                dict(id=uid, name=name)
                for uid, name in self.indicators
            ],
            breakdown=[
                dict(id=uid, name=name)
                for uid, name in self.breakdowns
            ],
            dimension=[
                dict(id=uid, name=name)
                for uid, name in self.dimensions
            ],
        )
        return static_filters

    def get(self, request, *args, **kwargs):
        accounts = Account.user_objects(request.user).filter(
            can_manage_clients=False,
        ).annotate(
            start_date=Min("campaigns__start_date"),
            end_date=Max("campaigns__end_date"),
            impressions=Sum("campaigns__impressions")
        ).filter(impressions__gt=0).distinct()

        filters = dict(
            accounts=[
                dict(
                    id=account.id,
                    name=account.name,
                    start_date=account.start_date,
                    end_date=account.end_date,
                    campaigns=[
                        dict(
                            id=c.id,
                            name=c.name,
                            start_date=c.start_date,
                            end_date=c.end_date,
                        )
                        for c in account.campaigns.all()
                    ]
                )
                for account in accounts
            ],
            **self.get_static_filters()
        )
        return Response(data=filters)


@demo_view_decorator
class TrackChartApiView(TrackApiBase):
    """
    Returns data we need to build charts
    """

    def get(self, request, *args, **kwargs):
        filters = self.get_filters()
        visible_accounts = Account.user_objects(request.user).filter(
            can_manage_clients=False,
        ).values_list("id", flat=True)
        chart = DeliveryChart(visible_accounts, additional_chart=False, **filters)
        return Response(data=chart.get_response())


@demo_view_decorator
class TrackAccountsDataApiView(TrackApiBase):
    """
    Returns a list of accounts for the table below the chart
    """

    def get(self, request, *args, **kwargs):
        filters = self.get_filters()
        visible_accounts = Account.user_objects(request.user).filter(
            can_manage_clients=False,
        ).values_list("id", flat=True)
        chart = DeliveryChart(
            visible_accounts,
            additional_chart=False,
            **filters
        )
        data = chart.get_account_segmented_data()
        return Response(data=data)


class ConnectAWAccountListApiView(ListAPIView):

    serializer_class = AWAccountConnectionSerializer

    def get_queryset(self):
        qs = AWConnection.objects.filter(
            users=self.request.user).order_by("email")
        return qs


class ConnectAWAccountApiView(APIView):
    """
    The view allows to connect user's AdWords account
    GET method gives an URL to go and grant access to our app
    then send the code you will get in the query in POST request

    POST body example:
    {"code": "<INSERT YOUR CODE HERE>"}

    success POST response example:
    {"email": "your@email.com",
    "mcc_accounts": [{"id": 1234, "name": "Test Acc", "currency_code": "UAH", "timezone": "Ukraine/Kiev"}]
    }
    """

    scopes = (
        'https://www.googleapis.com/auth/adwords',
        'https://www.googleapis.com/auth/userinfo.email',
    )
    lost_perm_error = "You have already provided access to your accounts" \
                      " but we've lost it. Please, visit " \
                      "https://myaccount.google.com/permissions and " \
                      "revoke our application's permission " \
                      "then try again"
    no_mcc_error = "MCC account wasn't found. Please check that you " \
                   "really have access to at least one."

    # first step
    def get(self, request, *args, **kwargs):
        redirect_url = self.request.query_params.get("redirect_url")
        if not redirect_url:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required query param: 'redirect_url'")
            )

        flow = self.get_flow(redirect_url)
        authorize_url = flow.step1_get_authorize_url()
        return Response(dict(authorize_url=authorize_url))

    # second step
    def post(self, request, *args, **kwargs):
        # get refresh token
        redirect_url = self.request.query_params.get("redirect_url")
        if not redirect_url:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required query param: 'redirect_url'")
            )

        code = request.data.get("code")
        if not code:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required: 'code'")
            )

        flow = self.get_flow(redirect_url)
        try:
            credential = flow.step2_exchange(code)
        except client.FlowExchangeError as e:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error='Authentication has failed: %s' % e)
            )
        else:
            token_info = get_google_access_token_info(
                credential.access_token)
            if 'email' not in token_info:
                return Response(status=HTTP_400_BAD_REQUEST,
                                data=token_info)

            refresh_token = credential.refresh_token
            try:
                connection = AWConnection.objects.get(
                    email=token_info['email']
                )
            except AWConnection.DoesNotExist:
                if refresh_token:
                    connection = AWConnection.objects.create(
                        email=token_info['email'],
                        refresh_token=refresh_token,
                    )
                else:
                    return Response(
                        data=dict(error=self.lost_perm_error),
                        status=HTTP_500_INTERNAL_SERVER_ERROR,
                    )
            else:
                # update token
                if refresh_token and \
                   connection.refresh_token != refresh_token:
                    connection.revoked_access = False
                    connection.refresh_token = refresh_token
                    connection.save()

            # save this connection, even if there is no MCC accounts yet
            self.request.user.aw_connections.add(connection)

            # -- end of get refresh token
            # save mcc accounts
            customers = get_customers(
                connection.refresh_token,
                **load_web_app_settings()
            )
            mcc_accounts = list(filter(
                lambda i: i['canManageClients'] and not i['testAccount'],
                customers,
            ))
            if not mcc_accounts:
                return Response(
                    status=HTTP_400_BAD_REQUEST,
                    data=dict(error=self.no_mcc_error)
                )
            with transaction.atomic():
                for ac_data in mcc_accounts:
                    data = dict(
                        id=ac_data['customerId'],
                        name=ac_data['descriptiveName'],
                        currency_code=ac_data['currencyCode'],
                        timezone=ac_data['dateTimeZone'],
                        can_manage_clients=ac_data['canManageClients'],
                        is_test_account=ac_data['testAccount'],
                    )
                    obj, _ = Account.objects.get_or_create(
                        id=data['id'], defaults=data,
                    )
                    AWAccountPermission.objects.get_or_create(
                        aw_connection=connection, account=obj,
                    )
            upload_initial_aw_data.delay(connection.email)

            response = AWAccountConnectionSerializer(connection).data
            return Response(data=response)

    def get_flow(self, redirect_url):
        aw_settings = load_web_app_settings()
        flow = client.OAuth2WebServerFlow(
            client_id=aw_settings.get("client_id"),
            client_secret=aw_settings.get("client_secret"),
            scope=self.scopes,
            user_agent=aw_settings.get("user_agent"),
            redirect_uri=redirect_url,
        )
        return flow







