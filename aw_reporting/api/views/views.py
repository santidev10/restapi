import csv
import re
from datetime import datetime, timedelta
from io import StringIO
from django.http import StreamingHttpResponse, HttpResponse
from django.db import transaction
from django.db.models import Min, Max, Sum, Count, Q, Avg, Case, When, Value, IntegerField, FloatField, F, \
    ExpressionWrapper
from rest_framework.generics import ListAPIView
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND
from oauth2client import client
from suds import WebFault
from oauth2client.client import HttpAccessTokenRefreshError
from aw_reporting.api.serializers.campaign_list_serializer import \
    CampaignListSerializer
from aw_reporting.api.serializers.aw_account_connection_relations_serializer import \
    AWAccountConnectionRelationsSerializer
from aw_reporting.api.serializers.accounts_list_serializer import \
    AccountsListSerializer
from aw_reporting.benchmark import ChartsHandler
from aw_reporting.models import AdGroup
from aw_reporting.models import Audience
from aw_reporting.models import SUM_STATS, BASE_STATS, QUARTILE_STATS, dict_add_calculated_stats, \
    dict_quartiles_to_rates, GenderStatistic, AgeRangeStatistic, CityStatistic, Genders, \
    AgeRanges, Devices, ConcatAggregate, AWConnection, Account, AWAccountPermission, \
    Campaign, AdGroupStatistic, DATE_FORMAT, VideoCreativeStatistic, YTChannelStatistic, \
    AWConnectionToUserRelation, CONVERSIONS, dict_calculate_stats, dict_norm_base_stats, \
    all_stats_aggregate, YTVideoStatistic, base_stats_aggregate

from aw_reporting.adwords_api import load_web_app_settings, get_customers
from aw_reporting.demo import demo_view_decorator
from aw_reporting.excel_reports import AnalyzeWeeklyReport
from aw_reporting.models import Topic
from aw_reporting.utils import get_google_access_token_info
from aw_reporting.tasks import upload_initial_aw_data
from aw_reporting.charts import DeliveryChart
from aw_creation.models import AccountCreation
from singledb.connector import SingleDatabaseApiConnector, SingleDatabaseApiConnectorException
from utils.api_paginator import CustomPageNumberPaginator
import pytz
import logging

from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class AccountsListPaginator(CustomPageNumberPaginator):
    page_size = 20


@demo_view_decorator
class AnalyzeAccountsListApiView(ListAPIView):
    """
    Returns a list of user's accounts that were pulled from AdWords
    """

    serializer_class = AccountsListSerializer
    pagination_class = AccountsListPaginator

    def get_queryset(self):
        queryset = Account.user_objects(self.request.user).filter(
            can_manage_clients=False,
        ).order_by("name", "id")
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

        show_closed = self.request.query_params.get("show_closed")
        if not show_closed or not int(show_closed):
            queryset = queryset.annotate(
                statuses=ConcatAggregate("campaigns__status", distinct=True)
            ).exclude(
                ~Q(statuses__isnull=True) &
                Q(statuses__contains="ended") &
                ~Q(statuses__contains="eligible") &
                ~Q(statuses__contains="pending") &
                ~Q(statuses__contains="suspended")
            )

        filters = self.get_filters()
        search = filters.get('search')
        if search:
            queryset = queryset.filter(name__icontains=search)

        min_campaigns_count = filters.get('min_campaigns_count')
        max_campaigns_count = filters.get('max_campaigns_count')
        if min_campaigns_count or max_campaigns_count:
            queryset = queryset.annotate(campaigns_count=Count('campaigns'))
            if min_campaigns_count:
                queryset = queryset.filter(campaigns_count__gte=min_campaigns_count)
            if max_campaigns_count:
                queryset = queryset.filter(campaigns_count__lte=max_campaigns_count)

        queryset = queryset.annotate(start=Min("campaigns__start_date"),
                                     end=Max("campaigns__end_date"))

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
    serializer_class = AccountsListSerializer

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

    def post(self, request, pk, **_):
        try:
            account = Account.user_objects(request.user).get(pk=pk)
        except Account.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        data = self.serializer_class(account).data  # header data
        data['details'] = self.get_details_data(account)
        data['overview'] = self.get_overview_data(account)
        return Response(data=data)

    def get_overview_data(self, account):
        filters = self.get_filters()
        fs = dict(ad_group__campaign__account=account)
        if filters['campaigns']:
            fs["ad_group__campaign__id__in"] = filters['campaigns']
        if filters['ad_groups']:
            fs["ad_group__id__in"] = filters['ad_groups']
        if filters['start_date']:
            fs["date__gte"] = filters['start_date']
        if filters['end_date']:
            fs["date__lte"] = filters['end_date']

        data = AdGroupStatistic.objects.filter(**fs).aggregate(
            **all_stats_aggregate
        )
        dict_norm_base_stats(data)
        dict_calculate_stats(data)
        dict_quartiles_to_rates(data)
        del data['video_impressions']

        # 'age', 'gender', 'device', 'location'
        annotate = dict(v=Sum('cost'))
        gender = GenderStatistic.objects.filter(**fs).values(
            'gender_id').order_by('gender_id').annotate(**annotate)
        gender = [dict(name=Genders[i['gender_id']], value=i['v']) for i in gender]

        age = AgeRangeStatistic.objects.filter(**fs).values(
            "age_range_id").order_by("age_range_id").annotate(**annotate)
        age = [dict(name=AgeRanges[i['age_range_id']], value=i['v']) for i in age]

        device = AdGroupStatistic.objects.filter(**fs).values(
            "device_id").order_by("device_id").annotate(**annotate)
        device = [dict(name=Devices[i['device_id']], value=i['v']) for i in device]

        location = CityStatistic.objects.filter(**fs).values(
            "city_id", "city__name").annotate(**annotate).order_by('v')[:6]
        location = [dict(name=i['city__name'], value=i['v']) for i in location]

        data.update(gender=gender, age=age, device=device, location=location)

        # this and last week base stats
        week_end = now_in_default_tz().date() - timedelta(days=1)
        week_start = week_end - timedelta(days=6)
        prev_week_end = week_start - timedelta(days=1)
        prev_week_start = prev_week_end - timedelta(days=6)

        annotate = {
            "{}_{}_week".format(s, k): Sum(
                Case(
                    When(
                        date__gte=sd,
                        date__lte=ed,
                        then=s,
                    ),
                    output_field=IntegerField()
                )
            )
            for k, sd, ed in (("this", week_start, week_end),
                              ("last", prev_week_start, prev_week_end))
            for s in BASE_STATS
            }
        weeks_stats = AdGroupStatistic.objects.filter(**fs).aggregate(**annotate)
        data.update(weeks_stats)

        # top and bottom rates
        annotate = dict(
            average_cpv=ExpressionWrapper(
                Case(
                    When(
                        cost__sum__isnull=False,
                        video_views__sum__gt=0,
                        then=F("cost__sum") / F("video_views__sum"),
                    ),
                    output_field=FloatField()
                ),
                output_field=FloatField()
            ),
            ctr=ExpressionWrapper(
                Case(
                    When(
                        clicks__sum__isnull=False,
                        impressions__sum__gt=0,
                        then=F("clicks__sum") * Value(100.0) / F("impressions__sum"),
                    ),
                    output_field=FloatField()
                ),
                output_field=FloatField()
            ),
            ctr_v=ExpressionWrapper(
                Case(
                    When(
                        clicks__sum__isnull=False,
                        video_views__sum__gt=0,
                        then=F("clicks__sum") * Value(100.0) / F("video_views__sum"),
                    ),
                    output_field=FloatField()
                ),
                output_field=FloatField()
            ),
            video_view_rate=ExpressionWrapper(
                Case(
                    When(
                        video_views__sum__isnull=False,
                        impressions__sum__gt=0,
                        then=F("video_views__sum") * Value(100.0) / F("impressions__sum"),
                    ),
                    output_field=FloatField()
                ),
                output_field=FloatField()
            ),
        )
        fields = tuple(annotate.keys())
        top_bottom_stats = AdGroupStatistic.objects.filter(**fs).values("date").order_by("date").annotate(
            *[Sum(s) for s in BASE_STATS]
        ).annotate(**annotate).aggregate(
            **{"{}_{}".format(s, n): a(s)
               for s in fields
               for n, a in (("top", Max), ("bottom", Min))}
        )
        data.update(top_bottom_stats)
        return data

    @staticmethod
    def get_details_data(account):

        fs = dict(ad_group__campaign__account=account)
        data = AdGroupStatistic.objects.filter(**fs).aggregate(
            ad_network=ConcatAggregate('ad_network', distinct=True),
            average_position=Avg(
                Case(
                    When(
                        average_position__gt=0,
                        then=F('average_position'),
                    ),
                    output_field=FloatField(),
                )
            ),
            impressions=Sum("impressions"),
            **{s: Sum(s) for s in CONVERSIONS + QUARTILE_STATS}
        )
        dict_quartiles_to_rates(data)
        del data['impressions']

        annotate = dict(v=Sum('cost'))
        creative = VideoCreativeStatistic.objects.filter(**fs).values(
            "creative_id").annotate(**annotate).order_by('v')[:3]
        if creative:
            ids = [i['creative_id'] for i in creative]
            creative = []
            try:
                channel_info = SingleDatabaseApiConnector().get_videos_base_info(ids)
            except SingleDatabaseApiConnectorException as e:
                logger.critical(e)
            else:
                video_info = {i['id']: i for i in channel_info}
                for video_id in ids:
                    info = video_info.get(video_id, {})
                    creative.append(
                        dict(
                            id=video_id,
                            name=info.get("title"),
                            thumbnail=info.get('thumbnail_image_url'),
                        )
                    )
        data.update(creative=creative)

        # second section
        gender = GenderStatistic.objects.filter(**fs).values(
            'gender_id').order_by('gender_id').annotate(**annotate)
        gender = [dict(name=Genders[i['gender_id']], value=i['v']) for i in gender]

        age = AgeRangeStatistic.objects.filter(**fs).values(
            "age_range_id").order_by("age_range_id").annotate(**annotate)
        age = [dict(name=AgeRanges[i['age_range_id']], value=i['v']) for i in age]

        device = AdGroupStatistic.objects.filter(**fs).values(
            "device_id").order_by("device_id").annotate(**annotate)
        device = [dict(name=Devices[i['device_id']], value=i['v']) for i in device]
        data.update(gender=gender, age=age, device=device)

        # third section
        charts = []
        stats = AdGroupStatistic.objects.filter(
            **fs
        ).values("date").order_by("date").annotate(
            views=Sum("video_views"),
            impressions=Sum("impressions"),
        )
        if stats:
            if any(i['views'] for i in stats):
                charts.append(
                    dict(
                        label='Views',
                        trend=[
                            dict(label=i['date'], value=i['views'])
                            for i in stats
                            ]
                    )
                )

            if any(i['impressions'] for i in stats):
                charts.append(
                    dict(
                        label='Impressions',
                        trend=[
                            dict(label=i['date'], value=i['impressions'])
                            for i in stats
                            ]
                    )
                )
        data['delivery_trend'] = charts

        return data


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

    def post(self, request, pk, **_):
        try:
            item = Account.user_objects(request.user).get(pk=pk)
        except Account.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        chart = DeliveryChart([item.id], segmented_by="campaigns", **filters)
        chart_data = chart.get_response()
        return Response(data=chart_data)


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
            segmented_by=data.get("segmented"),
        )
        return filters

    def post(self, request, pk, **kwargs):
        dimension = kwargs.get('dimension')
        try:
            item = Account.user_objects(request.user).get(pk=pk)
        except Account.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        chart = DeliveryChart(
            accounts=[item.id],
            dimension=dimension,
            **filters
        )
        items = chart.get_items()
        return Response(data=items)


@demo_view_decorator
class AnalyzeExportApiView(APIView):
    """
    Send filters to download a csv report

    Body example:

    {"campaigns": ["1", "2"]}
    """

    def post(self, request, pk, **_):
        try:
            item = Account.user_objects(request.user).get(pk=pk)
        except Account.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        def data_generator():
            return self.get_export_data(item)

        return self.stream_response(item.name, data_generator)

    file_name = "{title}-analyze-{timestamp}.csv"

    column_names = (
        "", "Name", "Impressions", "Views", "Cost", "Average cpm",
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

    def get_export_data(self, item):
        filters = self.get_filters()

        data = dict(name=item.name)

        fs = {'ad_group__campaign__account_id': item.id}
        if filters['start_date']:
            fs['date__gte'] = filters['start_date']
        if filters['end_date']:
            fs['date__lte'] = filters['end_date']
        if filters['ad_groups']:
            fs['ad_group_id__in'] = filters['ad_groups']
        elif filters['campaigns']:
            fs['ad_group__campaign_id__in'] = filters['campaigns']

        stats = AdGroupStatistic.objects.filter(**fs).aggregate(
            **{s: Sum(s) for s in BASE_STATS + QUARTILE_STATS}
        )
        dict_quartiles_to_rates(stats)
        dict_add_calculated_stats(stats)
        data.update(stats)

        yield self.column_names
        yield ['Summary'] + [data.get(n) for n in self.column_keys]

        for dimension in self.tabs:
            chart = DeliveryChart(
                accounts=[item.id],
                dimension=dimension,
                **filters
            )
            items = chart.get_items()
            for data in items['items']:
                yield [dimension.capitalize()] + [data[n] for n in self.column_keys]


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

    def post(self, request, pk, **_):
        try:
            item = Account.user_objects(request.user).get(pk=pk)
        except Account.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        report = AnalyzeWeeklyReport(item, **filters)

        response = HttpResponse(
            report.get_content(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename="Channel Factory {} Weekly ' \
                                          'Report {}.xlsx"'.format(
            item.name,
            datetime.now().date().strftime("%m.%d.%y")
        )
        return response


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
    serializer_class = AWAccountConnectionRelationsSerializer

    def get_queryset(self):
        qs = AWConnectionToUserRelation.objects.filter(
            user=self.request.user).order_by("connection__email")
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
                        status=HTTP_400_BAD_REQUEST,
                    )
            else:
                # update token
                if refresh_token and \
                                connection.refresh_token != refresh_token:
                    connection.revoked_access = False
                    connection.refresh_token = refresh_token
                    connection.save()

            try:
                AWConnectionToUserRelation.objects.get(
                    user=self.request.user,
                    connection=connection,
                )
            except AWConnectionToUserRelation.DoesNotExist:
                pass
            else:
                return Response(status=HTTP_400_BAD_REQUEST,
                                data=dict(error="You have already linked this account"))

            # -- end of get refresh token
            # save mcc accounts
            try:
                customers = get_customers(
                    connection.refresh_token,
                    **load_web_app_settings()
                )
            except WebFault as e:
                fault_string = e.fault.faultstring
                if "AuthenticationError.NOT_ADS_USER" in fault_string:
                    fault_string = "AdWords account does not exist"
                return Response(status=HTTP_400_BAD_REQUEST,
                                data=dict(error=fault_string))
            except HttpAccessTokenRefreshError as e:
                ex_token_error = "Token has been expired or revoked"
                if ex_token_error in str(e):
                    return Response(status=HTTP_400_BAD_REQUEST,
                                    data=dict(error=ex_token_error))
            else:
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
                    relation = AWConnectionToUserRelation.objects.create(
                        user=self.request.user,
                        connection=connection,
                    )

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

                response = AWAccountConnectionRelationsSerializer(relation).data
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

    @staticmethod
    def delete(request, email, **_):
        try:
            user_connection = AWConnectionToUserRelation.objects.get(
                user=request.user,
                connection__email=email,
            )
        except AWConnectionToUserRelation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        else:
            # we need to remove account creations that created within the connection
            mcc_connection = user_connection.connection
            accounts = Account.objects.filter(managers__mcc_permissions__aw_connection=mcc_connection)
            AccountCreation.objects.filter(owner=request.user, account__in=accounts).delete()

            # now delete the relation itself
            user_connection.delete()

        qs = AWConnectionToUserRelation.objects.filter(
            user=request.user
        ).order_by("connection__email")
        data = AWAccountConnectionRelationsSerializer(qs, many=True).data
        return Response(data)


class BenchmarkBaseChartsApiView(TrackApiBase):
    """
    Return data for chart building
    """

    def get(self, request):
        ch = ChartsHandler(request=request)
        return Response(ch.base_charts())


class BenchmarkProductChartsApiView(TrackApiBase):
    """
    Return data for chart building
    """

    def get(self, request):
        ch = ChartsHandler(request=request)
        return Response(ch.product_charts())


class BenchmarkFiltersListApiView(ListAPIView):
    """
    Lists of the filter names and values
    """

    def get(self, request, *args, **kwargs):
        result = {}
        filters = request.query_params.get('filters', [])
        if 'topics' in filters:
            result['topics'] = Topic.objects.filter(parent__isnull=True).order_by('name').values('id', 'name')
        if 'interests' in filters:
            result['interests'] = Audience.objects.filter(parent__isnull=True).order_by('name').values('id', 'name')
        if 'product_types' in filters:
            result['product_types'] = AdGroup.objects.all().values('type').distinct()
        if 'age_range' in filters:
            age_range_query = AgeRangeStatistic.objects.order_by().values('age_range_id').distinct()
            for age_range in age_range_query:
                age_range['name'] = AgeRanges[age_range['age_range_id']]
            result['age_range'] = age_range_query
        if 'gender' in filters:
            gender_query = GenderStatistic.objects.order_by().values('gender_id').distinct()
            for gender in gender_query:
                gender['name'] = Genders[gender['gender_id']]
            result['gender'] = gender_query
        if 'device' in filters:
            device_query = AdGroupStatistic.objects.order_by().values('device_id').distinct()
            for device in device_query:
                device['name'] = Devices[device['device_id']]
            result['device'] = device_query
        return Response(data=result)
