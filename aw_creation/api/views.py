# pylint: disable=import-error
from apiclient.discovery import build
# pylint: enable=import-error
from django.conf import settings
from django.db import transaction
from django.db.models import Avg, Value, Count, Case, When, \
    IntegerField as AggrIntegerField, DecimalField as AggrDecimalField, FloatField as AggrFloatField, \
    CharField as AggrCharField
from django.db.models.functions import Coalesce
from django.http import StreamingHttpResponse, HttpResponse
from django.shortcuts import get_object_or_404
from django.utils import timezone
from openpyxl import load_workbook
from rest_framework.generics import ListAPIView, RetrieveUpdateAPIView, \
    GenericAPIView, ListCreateAPIView, RetrieveAPIView
from utils.api_paginator import CustomPageNumberPaginator
from rest_framework.parsers import FileUploadParser
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST,  HTTP_200_OK, HTTP_202_ACCEPTED, \
    HTTP_404_NOT_FOUND, HTTP_201_CREATED, HTTP_204_NO_CONTENT
from rest_framework.views import APIView
from utils.permissions import IsAuthQueryTokenPermission
from rest_framework.authtoken.models import Token
from aw_creation.api.serializers import *
from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation, FrequencyCap, Language, LocationRule, AdScheduleRule,\
    TargetingItem, default_languages, CampaignOptimizationSetting, AccountOptimizationSetting
from aw_reporting.demo import demo_view_decorator
from aw_reporting.api.views import DATE_FORMAT
from aw_reporting.api.serializers import CampaignListSerializer, AccountsListSerializer
from aw_reporting.models import CONVERSIONS, QUARTILE_STATS, dict_quartiles_to_rates, all_stats_aggregate, \
    VideoCreativeStatistic, GenderStatistic, Genders, AgeRangeStatistic, AgeRanges, Devices, \
    CityStatistic, DEFAULT_TIMEZONE, BASE_STATS, GeoTarget, SUM_STATS, dict_add_calculated_stats, \
    Topic, Audience, Account, AWConnection, AdGroup, \
    YTChannelStatistic, YTVideoStatistic, KeywordStatistic, AudienceStatistic, TopicStatistic
from aw_reporting.adwords_api import create_customer_account, update_customer_account, handle_aw_api_errors
from aw_reporting.excel_reports import AnalyzeWeeklyReport
from aw_reporting.charts import DeliveryChart
from django.db.models import FloatField, ExpressionWrapper, IntegerField, F
from datetime import timedelta, datetime
from io import StringIO
from collections import OrderedDict
from decimal import Decimal
from suds import WebFault
import itertools
import calendar
import csv
import pytz
import logging

logger = logging.getLogger(__name__)


class GeoTargetListApiView(APIView):
    """
    Returns a list of geo-targets, limit is 100
    Accepts ?search=kharkiv parameter
    """
    queryset = GeoTarget.objects.all().order_by('name')
    serializer_class = SimpleGeoTargetSerializer

    def get(self, request, *args, **kwargs):
        queryset = self.queryset
        search = request.GET.get("search", "").strip()
        if search:
            queryset = queryset.filter(name__icontains=search)
        data = self.serializer_class(queryset[:100],  many=True).data
        return Response(data=data)


class DocumentImportBaseAPIView(GenericAPIView):
    parser_classes = (FileUploadParser,)

    @staticmethod
    def get_xlsx_contents(file_obj, return_lines=False):
        wb = load_workbook(file_obj)
        for sheet in wb:
            for row in sheet:
                if return_lines:
                    yield tuple(i.value for i in row)
                else:
                    for cell in row:
                        yield cell.value

    @staticmethod
    def get_csv_contents(file_obj, return_lines=False):
        string = file_obj.read().decode()
        reader = csv.reader(string.split("\n"))
        for row in reader:
            if return_lines:
                yield tuple(i.strip() for i in row)
            else:
                for cell in row:
                    yield cell.strip()


class DocumentToChangesApiView(DocumentImportBaseAPIView):
    """
    Send a post request with multipart-ford-data encoded file data
    key: 'file'
    will return
    {"result":[{"name":"94002,California,United States","id":9031903}, ..],
                "undefined":[]}
    """

    def post(self, request, content_type, **_):
        file_obj = request.data['file']
        fct = file_obj.content_type
        if fct == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            data = self.get_xlsx_contents(file_obj)
        elif fct in ("text/csv", "application/vnd.ms-excel"):
            data = self.get_csv_contents(file_obj)
        else:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data={"errors": ["The MIME type isn't supported: "
                                 "{}".format(file_obj.content_type)]})
        if content_type == "postal_codes":
            response_data = self.get_location_rules(data)
        else:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data={"errors": ["The content type isn't supported: "
                                 "{}".format(content_type)]})
        return Response(status=HTTP_200_OK, data=response_data)

    def get_location_rules(self, items):
        items = set(str(i) for i in items if i)
        geo_targets = self.get_geo_targets(items)
        result = [dict(id=i['id'], name=i['canonical_name'])
                  for i in geo_targets]
        undefined = list(items - set(i['name'] for i in geo_targets))
        if undefined:
            # let's search for zip+4 postal codes
            re_sub = re.sub
            numeric_values = [re_sub(r"\D", "", i) for i in undefined]
            plus_4_zips = filter(lambda i: len(i) == 9, numeric_values)
            common_codes = [c[:5] for c in plus_4_zips]
            if common_codes:
                geo_targets = self.get_geo_targets(common_codes)
                if geo_targets:
                    valid_zips = set(i['name'] for i in geo_targets)
                    result.extend(
                        [dict(id=i['id'], name=i['canonical_name'])
                         for i in geo_targets]
                    )
                    # remove items from undefined set
                    drop_indexes = []
                    for code in valid_zips:
                        for n, i in enumerate(numeric_values):
                            if i.startswith(code):
                                drop_indexes.append(n)
                    undefined = [i for n, i in enumerate(undefined)
                                 if n not in drop_indexes]

        return {'result': result, "undefined": undefined}

    @staticmethod
    def get_geo_targets(names):
        geo_targets = GeoTarget.objects.filter(
            name__in=names
        ).values("id", "name", "canonical_name")
        return geo_targets


class YoutubeVideoSearchApiView(GenericAPIView):

    def get(self, request, query, **_):
        youtube = build(
            "youtube", "v3",
            developerKey=settings.YOUTUBE_API_DEVELOPER_KEY
        )
        options = {
            'q': query,
            'part': 'snippet',
            'type': "video",
            'maxResults': 50,
            'safeSearch': 'none',
        }
        next_page = request.GET.get("next_page")
        if next_page:
            options["pageToken"] = next_page
        results = youtube.search().list(**options).execute()
        response = dict(
            next_page=results.get("nextPageToken"),
            items_count=results.get("pageInfo", {}).get("totalResults"),
            items=[self.format_item(i) for i in results.get("items", [])]
        )
        return Response(data=response)

    @staticmethod
    def format_item(data):
        snippet = data.get("snippet", {})
        thumbnails = snippet.get("thumbnails", {})
        thumbnail = thumbnails.get("high") if "high" in thumbnails \
            else thumbnails.get("default")
        uid = data.get("id", {})
        if isinstance(uid, dict):
            uid = uid.get("videoId")
        item = dict(
            id=uid,
            title=snippet.get("title"),
            url="https://youtube.com/video/{}".format(uid),
            description=snippet.get("description"),
            thumbnail=thumbnail.get("url"),
            channel=dict(
                id=snippet.get("channelId"),
                title=snippet.get("channelTitle"),
            ),
        )
        return item


class YoutubeVideoFromUrlApiView(YoutubeVideoSearchApiView):
    url_regex = r"^(?:https?:/{1,2})?(?:w{3}\.)?youtu(?:be)?\.(?:com|be)(?:/watch\?v=|/video/)([^\s&]+)$"

    def get(self, request, url, **_):
        match = re.match(self.url_regex, url)
        if match:
            yt_id = match.group(1)
        else:
            return Response(status=HTTP_400_BAD_REQUEST, data=dict(error="Wrong url format"))

        youtube = build(
            "youtube", "v3",
            developerKey=settings.YOUTUBE_API_DEVELOPER_KEY
        )
        options = {
            'id': yt_id,
            'part': 'snippet',
            'maxResults': 1,
        }
        results = youtube.videos().list(**options).execute()
        items = results.get("items", [])
        if not items:
            return Response(status=HTTP_404_NOT_FOUND, data=dict(error="There is no such a video"))

        return Response(data=self.format_item(items[0]))


class ItemsFromSegmentIdsApiView(APIView):

    def post(self, request, segment_type, **_):

        method = "get_{}_item_ids".format(segment_type)
        item_ids = getattr(self, method)(request.data)
        items = [dict(criteria=uid) for uid in item_ids]
        add_targeting_list_items_info(items, segment_type)

        return Response(data=items)

    @staticmethod
    def get_video_item_ids(ids):
        from segment.models import SegmentRelatedVideo
        ids = SegmentRelatedVideo.objects.filter(
            segment_id__in=ids
        ).values_list("related_id", flat=True).order_by("related_id").distinct()
        return ids

    @staticmethod
    def get_channel_item_ids(ids):
        from segment.models import SegmentRelatedChannel
        ids = SegmentRelatedChannel.objects.filter(
            segment_id__in=ids
        ).values_list("related_id", flat=True).order_by("related_id").distinct()
        return ids

    @staticmethod
    def get_keyword_item_ids(ids):
        from keyword_tool.models import KeyWord
        ids = KeyWord.objects.filter(
            text__isnull=False,
            lists__in=ids,
        ).values_list("text", flat=True).order_by("text").distinct()
        return ids


class TargetingItemsSearchApiView(APIView):

    def get(self, request, list_type, query, **_):

        method = "search_{}_items".format(list_type)
        items = getattr(self, method)(query)
        # items = [dict(criteria=uid) for uid in item_ids]
        # add_targeting_list_items_info(items, list_type)

        return Response(data=items)

    @staticmethod
    def search_video_items(query):
        connector = SingleDatabaseApiConnector()
        try:
            response = connector.get_custom_query_result(
                model_name="video",
                fields=["id", "title", "thumbnail_image_url"],
                title__icontains=query,
                limit=50,
            )
        except SingleDatabaseApiConnectorException as e:
            logger.error(e)
            items = []
        else:
            items = [
                dict(id=i['id'], criteria=i['id'],
                     name=i['title'], thumbnail=i['thumbnail_image_url'])
                for i in response
            ]
        return items

    @staticmethod
    def search_channel_items(query):
        connector = SingleDatabaseApiConnector()
        try:
            response = connector.get_custom_query_result(
                model_name="channel",
                fields=["id", "title", "thumbnail_image_url"],
                title__icontains=query,
                limit=50,
            )
        except SingleDatabaseApiConnectorException as e:
            logger.error(e)
            items = []
        else:
            items = [
                dict(id=i['id'], criteria=i['id'],
                     name=i['title'], thumbnail=i['thumbnail_image_url'])
                for i in response
            ]
        return items

    @staticmethod
    def search_keyword_items(query):
        from keyword_tool.models import KeyWord
        keywords = KeyWord.objects.filter(
            text__icontains=query,
        ).exclude(text=query).values_list("text", flat=True).order_by("text")
        items = [
            dict(criteria=k, name=k)
            for k in itertools.chain((query,), keywords)
        ]
        return items

    @staticmethod
    def search_interest_items(query):
        audiences = Audience.objects.filter(
            name__icontains=query,
            type__in=[Audience.AFFINITY_TYPE, Audience.IN_MARKET_TYPE],
        ).values('name', 'id').order_by('name', 'id')

        items = [
            dict(criteria=a['id'], name=a['name'])
            for a in audiences
        ]
        return items

    @staticmethod
    def search_topic_items(query):
        topics = Topic.objects.filter(
            name__icontains=query,
        ).values("id", "name").order_by('name')
        items = [
            dict(criteria=k['id'], name=k['name'])
            for k in topics
        ]
        return items


class OptimizationAccountListPaginator(CustomPageNumberPaginator):
    page_size = 20


class CreationOptionsApiView(APIView):

    @staticmethod
    def get(request, **k):
        def opts_to_response(opts):
            res = [dict(id=i, name=n) for i, n in opts]
            return res

        def list_to_resp(l, n_func=None):
            n_func = n_func or (lambda e: e)
            return [dict(id=i, name=n_func(i)) for i in l]

        def get_week_day_name(n):
            return calendar.day_name[n - 1]

        options = OrderedDict(
            # ACCOUNT
            # create
            name="string;max_length=250;required;validation=^[^#']*$",
            # create and update
            video_ad_format=opts_to_response(
                CampaignCreation.VIDEO_AD_FORMATS[:1],
            ),
            # update
            goal_type=opts_to_response(
                CampaignCreation.GOAL_TYPES[:1],
            ),
            type=opts_to_response(
                CampaignCreation.CAMPAIGN_TYPES[:1],
            ),
            bidding_type=opts_to_response(
                CampaignCreation.BIDDING_TYPES,
            ),
            delivery_method=opts_to_response(
                CampaignCreation.DELIVERY_METHODS[:1],
            ),
            video_networks=opts_to_response(
                CampaignCreation.VIDEO_NETWORKS,
            ),

            # CAMPAIGN
            start="date",
            end="date",
            goal_units="integer;max_value=4294967294",
            budget="decimal;max_digits=10,decimal_places=2",
            max_rate="decimal;max_digits=6,decimal_places=3",
            languages=[
                dict(id=l.id, name=l.name)
                for l in Language.objects.all().annotate(
                    priority=Case(
                        When(
                            id=1000,
                            then=Value(0),
                        ),
                        default=Value(1),
                        output_field=AggrIntegerField()
                    )
                ).order_by('priority', 'name')
            ],
            devices=opts_to_response(CampaignCreation.DEVICES),
            content_exclusions=opts_to_response(CampaignCreation.CONTENT_LABELS),
            frequency_capping=dict(
                event_type=opts_to_response(FrequencyCap.EVENT_TYPES),
                level=opts_to_response(FrequencyCap.LEVELS),
                limit='positive integer;max_value=65534',
                time_unit=opts_to_response(FrequencyCap.TIME_UNITS),
                __help="a list of two objects is allowed"
                       "(for each of the event types);",
            ),
            location_rules=dict(
                geo_target="the list can be requested from:"
                           " /geo_target_list/?search=Kiev",
                latitude="decimal",
                longitude="decimal",
                radius="integer;max_values are 800mi and 500km",
                radius_units=opts_to_response(LocationRule.UNITS),
                __help="a list of objects is accepted;"
                       "either geo_target_id or "
                       "coordinates and a radius - required",
            ),
            ad_schedule_rules=dict(
                day=list_to_resp(range(1, 8), n_func=get_week_day_name),
                from_hour=list_to_resp(range(0, 24)),
                from_minute=list_to_resp(range(0, 60, 15)),
                to_hour=list_to_resp(range(0, 24)),
                to_minute=list_to_resp(range(0, 60, 15)),
                __help="accepts a list of rules;example: [{'day': 1, "
                       "'from_hour': 9, 'from_minute': 15, 'to_hour': 18, "
                       "'to_minute': 45}];from_minute and to_minute are "
                       "not required",
            ),

            # AD GROUP
            video_url="url;validation=valid_yt_video",
            ct_overlay_text="string;max_length=200",
            display_url="string;max_length=200",
            final_url="url;max_length=200",
            genders=opts_to_response(
                AdGroupCreation.GENDERS,
            ),
            parents=opts_to_response(
                AdGroupCreation.PARENTS,
            ),
            age_ranges=opts_to_response(
                AdGroupCreation.AGE_RANGES,
            ),
        )
        return Response(data=options)


@demo_view_decorator
class AccountCreationListApiView(ListAPIView):

    serializer_class = AccountCreationListSerializer
    pagination_class = OptimizationAccountListPaginator
    annotate_sorts = dict(
        impressions=(None, Sum("account__campaigns__impressions")),
        video_views=(None, Sum("account__campaigns__video_views")),
        video_impressions=(None, Sum(Case(
            When(
                account__campaigns__video_views__gt=0,
                then="account__campaigns__impressions",
            ),
            output_field=AggrIntegerField()
        ))),
        clicks=(None, Sum("account__campaigns__clicks")),
        cost=(None, Sum("account__campaigns__cost")),
        video_view_rate=(('video_views', 'video_impressions'), ExpressionWrapper(
            Case(
                When(
                    video_views__isnull=False,
                    video_impressions__gt=0,
                    then=F("video_views") * 1.0 / F("video_impressions"),
                ),
                output_field=AggrFloatField()
            ),
            output_field=AggrFloatField()
        )),
        ctr_v=(('clicks', 'video_views'), ExpressionWrapper(
            Case(
                When(
                    clicks__isnull=False,
                    video_views__gt=0,
                    then=F("clicks") * 1.0 / F("video_views"),
                ),
                output_field=AggrFloatField()
            ),
            output_field=AggrFloatField()
        ))
    )

    def get(self, request, *args, **kwargs):
        # import "read only" accounts:
        # user has access to them, but they are not connected to his account creations
        read_accounts = Account.user_objects(self.request.user).filter(
            can_manage_clients=False,
        ).exclude(
            account_creations__owner=request.user
        ).values("id", "name")
        bulk_create = [
            AccountCreation(
                account_id=i['id'],
                name=i['name'],
                owner=request.user,
                is_managed=False,
            )
            for i in read_accounts
        ]
        if bulk_create:
            AccountCreation.objects.bulk_create(bulk_create)

        return super(AccountCreationListApiView, self).get(request, *args, **kwargs)

    def get_queryset(self, **filters):
        queryset = AccountCreation.objects.filter(
            is_deleted=False,
            owner=self.request.user, **filters
        )
        sort_by = self.request.query_params.get('sort_by')

        if sort_by in self.annotate_sorts:
            dependencies, annotate = self.annotate_sorts[sort_by]
            if dependencies:
                queryset = queryset.annotate(**{d: self.annotate_sorts[d][1] for d in dependencies})

            queryset = queryset.annotate(sort_by=Coalesce(annotate, 0))
            sort_by = "-sort_by"

        elif sort_by != "name":
            sort_by = "-created_at"

        return queryset.order_by('is_ended', sort_by)

    def filter_queryset(self, queryset):
        filters = self.request.query_params

        search = filters.get('search')
        if search:
            queryset = queryset.filter(name__icontains=search)

        min_campaigns_count = filters.get('min_campaigns_count')
        max_campaigns_count = filters.get('max_campaigns_count')
        if min_campaigns_count or max_campaigns_count:
            queryset = queryset.annotate(campaign_creations_count=Count("campaign_creations"))

            queryset = queryset.annotate(campaigns_count=Case(
                    When(
                        campaign_creations_count=0,
                        then=Count("account__campaigns"),
                    ),
                    default="campaign_creations_count",
                    output_field=AggrIntegerField(),
                ),
            )

            if min_campaigns_count:
                queryset = queryset.filter(campaigns_count__gte=min_campaigns_count)
            if max_campaigns_count:
                queryset = queryset.filter(campaigns_count__lte=max_campaigns_count)

        min_start = filters.get('min_start')
        max_start = filters.get('max_start')
        if min_start or max_start:
            queryset = queryset.annotate(start=Coalesce(Min("campaign_creations__start"),
                                                        Min("account__campaigns__start_date")))
            if min_start:
                queryset = queryset.filter(start__gte=min_start)
            if max_start:
                queryset = queryset.filter(start__lte=max_start)

        min_end = filters.get('min_end')
        max_end = filters.get('max_end')
        if min_end or max_end:
            queryset = queryset.annotate(end=Coalesce(Max("campaign_creations__end"),
                                                      Max("account__campaigns__end_date")))
            if min_end:
                queryset = queryset.filter(end__gte=min_end)
            if max_end:
                queryset = queryset.filter(end__lte=max_end)
        status = filters.get('status')
        if status:
            if status == "From AdWords":
                queryset = queryset.filter(is_managed=False)
            elif status == "Ended":
                queryset = queryset.filter(is_ended=True, is_managed=True)
            elif status == "Paused":
                queryset = queryset.filter(is_paused=True, is_managed=True, is_ended=False)
            elif status == "Running":
                queryset = queryset.filter(sync_at__isnull=False, is_managed=True,
                                           is_paused=False, is_ended=False)
            elif status == "Approved":
                queryset = queryset.filter(is_approved=True, is_managed=True, sync_at__isnull=True,
                                           is_paused=False, is_ended=False)
            elif status == "Pending":
                queryset = queryset.filter(is_approved=False, is_managed=True, sync_at__isnull=True,
                                           is_paused=False, is_ended=False)

        annotates = {}
        second_annotates = {}
        having = {}
        for metric in ("impressions", "video_views", "clicks", "cost", "video_view_rate", "ctr_v"):
            for is_max, option in enumerate(("min", "max")):
                filter_value = filters.get("{}_{}".format(option, metric))
                if filter_value:
                    if metric in BASE_STATS:
                        annotate_key = "sum_{}".format(metric)
                        annotates[annotate_key] = Sum("account__campaigns__{}".format(metric))
                        having["{}__{}".format(annotate_key, "lte" if is_max else "gte")] = filter_value
                    elif metric == "video_view_rate":
                        annotates['video_impressions'] = Sum(
                            Case(
                                When(
                                    account__campaigns__video_views__gt=0,
                                    then="account__campaigns__impressions",
                                ),
                                output_field=IntegerField()
                            )
                        )
                        annotates['sum_video_views'] = Sum("account__campaigns__video_views")
                        second_annotates[metric] = Case(
                            When(
                                sum_video_views__isnull=False,
                                video_impressions__gt=0,
                                then=F("sum_video_views") * 100. / F("video_impressions"),
                            ),
                            output_field=FloatField()
                        )
                        having["{}__{}".format(metric, "lte" if is_max else "gte")] = filter_value
                    elif metric == "ctr_v":
                        annotates['video_clicks'] = Sum(
                            Case(
                                When(
                                    account__campaigns__video_views__gt=0,
                                    then="account__campaigns__clicks",
                                ),
                                output_field=IntegerField()
                            )
                        )
                        annotates['sum_video_views'] = Sum("account__campaigns__video_views")
                        second_annotates[metric] = Case(
                            When(
                                video_clicks__isnull=False,
                                sum_video_views__gt=0,
                                then=F("video_clicks") * 100. / F("sum_video_views"),
                            ),
                            output_field=FloatField()
                        )
                        having["{}__{}".format(metric, "lte" if is_max else "gte")] = filter_value
        if annotates:
            queryset = queryset.annotate(**annotates)
        if second_annotates:
            queryset = queryset.annotate(**second_annotates)
        if having:
            queryset = queryset.filter(**having)

        return queryset

    def post(self, *a, **_):
        account_count = AccountCreation.objects.filter(owner=self.request.user).count()

        with transaction.atomic():
            account_creation = AccountCreation.objects.create(
                name="Account {}".format(account_count + 1), owner=self.request.user,
            )
            campaign_creation = CampaignCreation.objects.create(
                name="Campaign 1",
                account_creation=account_creation,
            )
            ad_group_creation = AdGroupCreation.objects.create(
                name="AdGroup 1",
                campaign_creation=campaign_creation,
            )
            AdCreation.objects.create(
                name="Ad 1",
                ad_group_creation=ad_group_creation,
            )
            AccountCreation.objects.filter(id=account_creation.id).update(is_deleted=True)  # do not show it in the list

        for language in default_languages():
            campaign_creation.languages.add(language)

        data = AccountCreationSetupSerializer(account_creation).data
        return Response(status=HTTP_202_ACCEPTED, data=data)


@demo_view_decorator
class AccountCreationDetailsApiView(RetrieveAPIView):

    def get(self, request, *args, **kwargs):
        pk = kwargs.get("pk")
        queryset = AccountCreation.objects.filter(owner=self.request.user)
        try:
            item = queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        data = PerformanceAccountDetailsApiView.get_details_data(item)
        return Response(data=data)


@demo_view_decorator
class AccountCreationSetupApiView(RetrieveUpdateAPIView):

    serializer_class = AccountCreationSetupSerializer

    def get_queryset(self):
        queryset = AccountCreation.objects.filter(owner=self.request.user, is_managed=True)
        return queryset

    @staticmethod
    def account_creation(account_creation, mcc_account, connection):

        aw_id = create_customer_account(
            mcc_account.id, connection.refresh_token,
            account_creation.name, mcc_account.currency_code, mcc_account.timezone,
        )
        # save to db
        customer = Account.objects.create(
            id=aw_id,
            name=account_creation.name,
            currency_code=mcc_account.currency_code,
            timezone=mcc_account.timezone,
        )
        customer.managers.add(mcc_account)
        account_creation.account = customer
        account_creation.save()

        return customer

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        data = request.data
        # approve rules
        if "is_approved" in data:
            if data["is_approved"]:
                if not instance.account:  # create account
                    # check dates
                    today = instance.get_today_date()
                    for c in instance.campaign_creations.all():
                        if c.start and c.start < today or c.end and c.end < today:
                            return Response(status=HTTP_400_BAD_REQUEST,
                                            data=dict(error="The dates cannot be in the past: {}".format(c.name)))

                    mcc_account = Account.user_mcc_objects(request.user).first()
                    if mcc_account:
                        connection = AWConnection.objects.filter(
                            mcc_permissions__account=mcc_account,
                            user_relations__user=request.user,
                        ).first()
                        _, error = handle_aw_api_errors(self.account_creation, instance, mcc_account, connection)
                        if error:
                            return Response(status=HTTP_400_BAD_REQUEST, data=dict(error=error))
                    else:
                        return Response(status=HTTP_400_BAD_REQUEST,
                                        data=dict(error="You have no connected MCC account"))

            elif instance.account:
                return Response(status=HTTP_400_BAD_REQUEST, data=dict(error="You cannot disapprove a running account"))

        if "name" in data and data['name'] != instance.name and instance.account:
            connections = AWConnection.objects.filter(
                mcc_permissions__account=instance.account.managers.all(),
                user_relations__user=request.user,
            ).values("mcc_permissions__account_id", "refresh_token")
            if connections:
                connection = connections[0]
                _, error = handle_aw_api_errors(
                    update_customer_account,
                    connection['mcc_permissions__account_id'],
                    connection['refresh_token'],
                    instance.account.id, data['name'],
                )
                if error:
                    return Response(status=HTTP_400_BAD_REQUEST, data=dict(error=error))

        serializer = AccountCreationUpdateSerializer(
            instance, data=request.data, partial=partial
        )
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)
        return self.retrieve(self, request, *args, **kwargs)

    def delete(self, request, *args, **kwargs):
        instance = self.get_object()
        if instance.account is not None:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="You cannot delete approved setups"))
        AccountCreation.objects.filter(pk=instance.id).update(is_deleted=True)
        return Response(status=HTTP_204_NO_CONTENT)


@demo_view_decorator
class CampaignCreationListSetupApiView(ListCreateAPIView):
    serializer_class = CampaignCreationSetupSerializer

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = CampaignCreation.objects.filter(
            account_creation__owner=self.request.user,
            account_creation_id=pk
        )
        return queryset

    def create(self, request, *args, **kwargs):
        try:
            account_creation = AccountCreation.objects.get(
                pk=kwargs.get("pk"),
                owner=request.user,
                is_managed=True,
            )
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        count = self.get_queryset().count()
        data = dict(
            name="Campaign {}".format(count + 1),
            account_creation=account_creation.id,
        )
        serializer = AppendCampaignCreationSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        campaign_creation = serializer.save()

        for language in default_languages():
            campaign_creation.languages.add(language)

        ad_group_creation = AdGroupCreation.objects.create(
            name="AdGroup 1",
            campaign_creation=campaign_creation,
        )
        AdCreation.objects.create(
            name="Ad 1",
            ad_group_creation=ad_group_creation,
        )

        data = self.get_serializer(instance=campaign_creation).data
        return Response(data, status=HTTP_201_CREATED)


@demo_view_decorator
class CampaignCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = CampaignCreationSetupSerializer

    def get_queryset(self):
        queryset = CampaignCreation.objects.filter(
            account_creation__owner=self.request.user
        )
        return queryset

    def delete(self, *args, **_):
        instance = self.get_object()
        if instance.account_creation.account is not None:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="You cannot delete approved setups"))

        campaigns_count = CampaignCreation.objects.filter(account_creation=instance.account_creation).count()
        if campaigns_count < 2:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="You cannot delete the only campaign"))
        instance.delete()
        return Response(status=HTTP_204_NO_CONTENT)

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        serializer = CampaignCreationUpdateSerializer(
            instance, data=request.data, partial=partial
        )
        serializer.is_valid(raise_exception=True)
        obj = serializer.save()
        self.update_related_models(obj.id, request.data)

        return self.retrieve(self, request, *args, **kwargs)

    @staticmethod
    def update_related_models(uid, data):
        if 'ad_schedule_rules' in data:
            schedule_qs = AdScheduleRule.objects.filter(
                campaign_creation_id=uid
            )
            fields = ("day", "from_hour", "from_minute", "to_hour",
                      "to_minute")
            existed = set(schedule_qs.values_list(*fields))
            sent = set(
                tuple(r.get(f, 0) for f in fields)
                for r in data['ad_schedule_rules']
            )
            to_delete = existed - sent
            for i in to_delete:
                filters = dict(zip(fields, i))
                schedule_qs.filter(**filters).delete()

            to_create = sent - existed
            bulk = []
            for i in to_create:
                filters = dict(zip(fields, i))
                bulk.append(
                    AdScheduleRule(campaign_creation_id=uid, **filters)
                )
            AdScheduleRule.objects.bulk_create(bulk)

        if 'frequency_capping' in data:
            rules = {
                r['event_type']: r
                for r in data.get('frequency_capping', [])
            }
            for event_type, _ in FrequencyCap.EVENT_TYPES:
                rule = rules.get(event_type)
                if rule is None:  # if rule isn't sent
                    FrequencyCap.objects.filter(
                        campaign_creation=uid,
                        event_type=event_type,
                    ).delete()
                else:
                    rule['campaign_creation'] = uid
                    try:
                        instance = FrequencyCap.objects.get(
                            campaign_creation=uid,
                            event_type=event_type,
                        )
                    except FrequencyCap.DoesNotExist:
                        instance = None
                    serializer = FrequencyCapUpdateSerializer(
                        instance=instance, data=rule)
                    serializer.is_valid(raise_exception=True)
                    serializer.save()

        if 'location_rules' in data:
            queryset = LocationRule.objects.filter(
                campaign_creation_id=uid
            )
            fields = ('geo_target_id', 'latitude', 'longitude')
            existed = set(queryset.values_list(*fields))
            sent = set(
                (r.get('geo_target'),
                 Decimal(r['latitude']) if 'latitude' in r else None,
                 Decimal(r['longitude']) if 'longitude' in r else None)
                for r in data['location_rules']
            )
            to_delete = existed - sent
            for rule in to_delete:
                filters = dict(zip(fields, rule))
                queryset.filter(**filters).delete()

            # create or update
            for rule in data['location_rules']:
                rule['campaign_creation'] = uid
                try:
                    instance = queryset.get(
                        geo_target_id=rule.get('geo_target'),
                        latitude=rule.get('latitude'),
                        longitude=rule.get('longitude'),
                    )
                except LocationRule.DoesNotExist:
                    instance = None
                serializer = OptimizationLocationRuleUpdateSerializer(
                    instance=instance, data=rule)
                serializer.is_valid(raise_exception=True)
                serializer.save()


@demo_view_decorator
class AdGroupCreationListSetupApiView(ListCreateAPIView):
    serializer_class = AdGroupCreationSetupSerializer

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=self.request.user,
            campaign_creation_id=pk
        )
        return queryset

    def create(self, request, *args, **kwargs):
        try:
            campaign_creation = CampaignCreation.objects.get(
                pk=kwargs.get("pk"), account_creation__owner=request.user
            )
        except CampaignCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        count = self.get_queryset().count()
        data = dict(
            name="Ad Group {}".format(count + 1),
            campaign_creation=campaign_creation.id,
        )
        serializer = AppendAdGroupCreationSetupSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        ad_group_creation = serializer.save()

        AdCreation.objects.create(
            name="Ad 1",
            ad_group_creation=ad_group_creation,
        )
        data = self.get_serializer(instance=ad_group_creation).data
        return Response(data, status=HTTP_201_CREATED)


@demo_view_decorator
class AdGroupCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = AdGroupCreationSetupSerializer

    def get_queryset(self):
        queryset = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=self.request.user
        )
        return queryset

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        serializer = AdGroupCreationUpdateSerializer(
            instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return self.retrieve(self, request, *args, **kwargs)

    def delete(self, *args, **_):
        instance = self.get_object()
        if instance.campaign_creation.account_creation.account is not None:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="You cannot delete approved setups"))

        count = AdGroupCreation.objects.filter(campaign_creation=instance.campaign_creation).count()
        if count < 2:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="You cannot delete the only item"))
        instance.delete()
        return Response(status=HTTP_204_NO_CONTENT)


@demo_view_decorator
class AdCreationListSetupApiView(ListCreateAPIView):
    serializer_class = AdCreationSetupSerializer

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = AdCreation.objects.filter(
            ad_group_creation__campaign_creation__account_creation__owner=self.request.user,
            ad_group_creation_id=pk
        )
        return queryset

    def create(self, request, *args, **kwargs):
        try:
            ad_group_creation = AdGroupCreation.objects.get(
                pk=kwargs.get("pk"), campaign_creation__account_creation__owner=request.user
            )
        except AdGroupCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        count = self.get_queryset().count()
        data = dict(
            name="Ad {}".format(count + 1),
            ad_group_creation=ad_group_creation.id,
        )
        serializer = AppendAdCreationSetupSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        obj = serializer.save()

        data = self.get_serializer(instance=obj).data
        return Response(data, status=HTTP_201_CREATED)


@demo_view_decorator
class AdCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = AdCreationSetupSerializer

    def get_queryset(self):
        queryset = AdCreation.objects.filter(
            ad_group_creation__campaign_creation__account_creation__owner=self.request.user
        )
        return queryset

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        serializer = AdCreationUpdateSerializer(
            instance, data=request.data, partial=partial,
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return self.retrieve(self, request, *args, **kwargs)

    def delete(self, *args, **_):
        instance = self.get_object()
        if instance.ad_group_creation.campaign_creation.account_creation.account is not None:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="You cannot delete approved setups"))

        count = AdCreation.objects.filter(ad_group_creation=instance.ad_group_creation).count()
        if count < 2:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="You cannot delete the only item"))
        instance.delete()
        return Response(status=HTTP_204_NO_CONTENT)


@demo_view_decorator
class AccountCreationDuplicateApiView(APIView):
    serializer_class = AccountCreationSetupSerializer

    account_fields = ("is_paused", "is_ended")
    campaign_fields = (
        "name", "start", "end", "budget",
        "devices_raw", "delivery_method", "video_ad_format", "video_networks_raw",
        'content_exclusions_raw',
    )
    loc_rules_fields = (
        "geo_target", "latitude", "longitude", "radius", "radius_units", "bid_modifier",
    )
    freq_cap_fields = ("event_type", "level", "limit", "time_unit")
    ad_schedule_fields = (
        "day", "from_hour", "from_minute", "to_hour", "to_minute",
    )
    ad_group_fields = (
        "name", "max_rate", "genders_raw", "parents_raw", "age_ranges_raw",
    )
    ad_fields = (
        "name", "video_url", "display_url", "final_url", "tracking_template", "custom_params", 'companion_banner',
        'video_id', 'video_title', 'video_description', 'video_thumbnail', 'video_channel_title',
    )
    targeting_fields = ("criteria", "type", "is_negative")

    def get_queryset(self):
        queryset = AccountCreation.objects.filter(
            owner=self.request.user,
            is_managed=True,
        )
        return queryset

    def post(self, *args, pk, **kwargs):
        try:
            instance = self.get_queryset().get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        bulk_items = defaultdict(list)
        duplicate = self.duplicate_item(instance, bulk_items)
        self.insert_bulk_items(bulk_items)

        response = self.serializer_class(duplicate).data
        return Response(data=response)

    def duplicate_item(self, item, bulk_items):
        if isinstance(item, AccountCreation):
            return self.duplicate_account(item, bulk_items,
                                          all_names=self.get_queryset().values_list("name", flat=True))
        elif isinstance(item, CampaignCreation):
            parent = item.account_creation
            return self.duplicate_campaign(parent, item, bulk_items,
                                           all_names=parent.campaign_creations.values_list("name", flat=True))
        elif isinstance(item, AdGroupCreation):
            parent = item.campaign_creation
            return self.duplicate_ad_group(parent, item, bulk_items,
                                           all_names=parent.ad_group_creations.values_list("name", flat=True))
        elif isinstance(item, AdCreation):
            parent = item.ad_group_creation
            return self.duplicate_ad(parent, item, bulk_items,
                                     all_names=parent.ad_creations.values_list("name", flat=True))
        else:
            raise NotImplementedError("Unknown item type: {}".format(type(item)))

    def duplicate_account(self, account, bulk_items, all_names):
        account_data = dict(
            name=self.increment_name(account.name, all_names),
            owner=self.request.user,
        )
        for f in self.account_fields:
            account_data[f] = getattr(account, f)
        acc_duplicate = AccountCreation.objects.create(**account_data)

        for c in account.campaign_creations.all():
            self.duplicate_campaign(acc_duplicate, c, bulk_items)

        return acc_duplicate

    def duplicate_campaign(self, account, campaign, bulk_items, all_names=None):
        camp_data = {f: getattr(campaign, f) for f in self.campaign_fields}
        c_duplicate = CampaignCreation.objects.create(
            account_creation=account, **camp_data
        )
        if all_names:
            c_duplicate.name = self.increment_name(c_duplicate.name, all_names)
            c_duplicate.save()
        # through
        language_through = CampaignCreation.languages.through
        for lid in campaign.languages.values_list('id', flat=True):
            bulk_items['languages'].append(
                language_through(campaigncreation_id=c_duplicate.id, language_id=lid)
            )

        for r in campaign.location_rules.all():
            bulk_items['location_rules'].append(
                LocationRule(
                    campaign_creation=c_duplicate,
                    **{f: getattr(r, f) for f in self.loc_rules_fields}
                )
            )

        for i in campaign.frequency_capping.all():
            bulk_items['frequency_capping'].append(
                FrequencyCap(
                    campaign_creation=c_duplicate,
                    **{f: getattr(i, f) for f in self.freq_cap_fields}
                )
            )

        for i in campaign.ad_schedule_rules.all():
            bulk_items['ad_schedule_rules'].append(
                AdScheduleRule(
                    campaign_creation=c_duplicate,
                    **{f: getattr(i, f) for f in self.ad_schedule_fields}
                )
            )

        for a in campaign.ad_group_creations.all():
            self.duplicate_ad_group(c_duplicate, a, bulk_items)

        return c_duplicate

    def duplicate_ad_group(self, campaign, ad_group, bulk_items, all_names=None):
        a_duplicate = AdGroupCreation.objects.create(
            campaign_creation=campaign,
            **{f: getattr(ad_group, f) for f in self.ad_group_fields}
        )
        if all_names:
            a_duplicate.name = self.increment_name(a_duplicate.name, all_names)
            a_duplicate.save()

        for i in ad_group.targeting_items.all():
            bulk_items['targeting_items'].append(
                TargetingItem(
                    ad_group_creation=a_duplicate,
                    **{f: getattr(i, f) for f in self.targeting_fields}
                )
            )

        for ad in ad_group.ad_creations.all():
            self.duplicate_ad(a_duplicate, ad, bulk_items)

        return a_duplicate

    def duplicate_ad(self, ad_group, ad, *_, all_names=None):
        ad_duplicate = AdCreation.objects.create(
            ad_group_creation=ad_group,
            **{f: getattr(ad, f) for f in self.ad_fields}
        )
        if all_names:
            ad_duplicate.name = self.increment_name(ad_duplicate.name, all_names)
            ad_duplicate.save()
        return ad_duplicate

    @staticmethod
    def increment_name(name, all_names):
        len_limit = 250
        mark_match = re.match(r".*( \(\d+\))$", name)

        # clear name from mark
        if mark_match:
            mark_str = mark_match.group(1)
            name = name[:-len(mark_str)]  # crop a previous mark from a name
            # copy_number = int(mark_match.group(2)) + 1

        # find top mark number
        max_number = 0
        for n in all_names:
            n_match = re.match(r"(.*) \((\d+)\)$", n)
            if n_match and n_match.group(1) == name:
                number = int(n_match.group(2))
                if number > max_number:
                    max_number = number

        # create new name
        copy_sign = " ({})".format(max_number + 1)
        max_len = len_limit - len(copy_sign)
        if len(name) >= max_len:
            name = name[:max_len - 2] + ".."
        return name + copy_sign

    @staticmethod
    def insert_bulk_items(bulk_items):
        if bulk_items['languages']:
            CampaignCreation.languages.through.objects.bulk_create(bulk_items['languages'])

        if bulk_items['location_rules']:
            LocationRule.objects.bulk_create(bulk_items['location_rules'])

        if bulk_items['frequency_capping']:
            FrequencyCap.objects.bulk_create(bulk_items['frequency_capping'])

        if bulk_items['ad_schedule_rules']:
            AdScheduleRule.objects.bulk_create(bulk_items['ad_schedule_rules'])

        if bulk_items['targeting_items']:
            TargetingItem.objects.bulk_create(bulk_items['targeting_items'])


@demo_view_decorator
class CampaignCreationDuplicateApiView(AccountCreationDuplicateApiView):
    serializer_class = CampaignCreationSetupSerializer

    def get_queryset(self):
        queryset = CampaignCreation.objects.filter(
            account_creation__owner=self.request.user,
        )
        return queryset


@demo_view_decorator
class AdGroupCreationDuplicateApiView(AccountCreationDuplicateApiView):
    serializer_class = AdGroupCreationSetupSerializer

    def get_queryset(self):
        queryset = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=self.request.user,
        )
        return queryset


@demo_view_decorator
class AdCreationDuplicateApiView(AccountCreationDuplicateApiView):
    serializer_class = AdCreationSetupSerializer

    def get_queryset(self):
        queryset = AdCreation.objects.filter(
            ad_group_creation__campaign_creation__account_creation__owner=self.request.user,
        )
        return queryset


# <<< Performance
@demo_view_decorator
class PerformanceAccountCampaignsListApiView(ListAPIView):
    serializer_class = CampaignListSerializer

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = Campaign.objects.filter(
            account__account_creations__id=pk,
            account__account_creations__owner=self.request.user,
        ).order_by("name", "id").distinct()
        return queryset


@demo_view_decorator
class PerformanceAccountDetailsApiView(APIView):

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
            account_creation = AccountCreation.objects.filter(
                owner=self.request.user
            ).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        data = AccountCreationListSerializer(account_creation).data  # header data
        data['details'] = self.get_details_data(account_creation)
        data['overview'] = self.get_overview_data(account_creation)
        return Response(data=data)

    def get_overview_data(self, account_creation):
        filters = self.get_filters()
        fs = dict(ad_group__campaign__account=account_creation.account)
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
        week_end = datetime.now(tz=pytz.timezone(DEFAULT_TIMEZONE)).date() - timedelta(days=1)
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
    def get_details_data(account_creation):
        fs = dict(ad_group__campaign__account=account_creation.account)
        data = AdGroupStatistic.objects.filter(**fs).aggregate(
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
                channel_info = SingleDatabaseApiConnector().get_custom_query_result(
                    model_name="video",
                    fields=["id", "title", "thumbnail_image_url"],
                    id__in=list(ids),
                    limit=len(ids),
                )
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
class PerformanceChartApiView(APIView):
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
            item = AccountCreation.objects.filter(owner=request.user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        account_ids = []
        if item.account:
            account_ids.append(item.account.id)
        chart = DeliveryChart(account_ids, segmented_by="campaigns", **filters)
        chart_data = chart.get_response()
        return Response(data=chart_data)


@demo_view_decorator
class PerformanceChartItemsApiView(APIView):
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
            item = AccountCreation.objects.filter(owner=request.user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        accounts = []
        if item.account:
            accounts.append(item.account.id)
        chart = DeliveryChart(
            accounts=accounts,
            dimension=dimension,
            **filters
        )
        items = chart.get_items()
        return Response(data=items)


@demo_view_decorator
class PerformanceExportApiView(APIView):
    """
    Send filters to download a csv report

    Body example:

    {"campaigns": ["1", "2"]}
    """

    def post(self, request, pk, **_):
        try:
            item = AccountCreation.objects.filter(owner=request.user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        def data_generator():
            return self.get_export_data(item)

        return self.stream_response(item.name, data_generator)

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

    def get_export_data(self, item):
        filters = self.get_filters()
        data = dict(name=item.name)

        account = item.account

        fs = {'ad_group__campaign__account': account}
        if filters['start_date']:
            fs['date__gte'] = filters['start_date']
        if filters['end_date']:
            fs['date__lte'] = filters['end_date']
        if filters['ad_groups']:
            fs['ad_group_id__in'] = filters['ad_groups']
        elif filters['campaigns']:
            fs['ad_group__campaign_id__in'] = filters['campaigns']

        stats = AdGroupStatistic.objects.filter(**fs).aggregate(
            **all_stats_aggregate
        )
        dict_norm_base_stats(stats)
        dict_quartiles_to_rates(stats)
        dict_calculate_stats(stats)
        data.update(stats)

        yield self.column_names
        yield ['Summary'] + [data.get(n) for n in self.column_keys]

        accounts = []
        if account:
            accounts.append(account.id)

        for dimension in self.tabs:
            chart = DeliveryChart(
                accounts=accounts,
                dimension=dimension,
                **filters
            )
            items = chart.get_items()
            for data in items['items']:
                yield [dimension.capitalize()] + [data[n] for n in self.column_keys]


@demo_view_decorator
class PerformanceExportWeeklyReport(APIView):
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
            item = AccountCreation.objects.filter(owner=request.user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        report = AnalyzeWeeklyReport(item.account, **filters)

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


@demo_view_decorator
class PerformanceTargetingFiltersAPIView(APIView):

    def get_queryset(self):
        return AccountCreation.objects.filter(owner=self.request.user)

    @staticmethod
    def get_campaigns(item):
        rows = CampaignCreation.objects.filter(account_creation=item).values(
            "name", "id", "ad_group_creations__name", "ad_group_creations__id",
            "campaign__status", "ad_group_creations__ad_group__status",
        ).order_by(
            "name", "id", "ad_group_creations__name", "ad_group_creations__id",
        ).annotate(
            start=Coalesce("start", "campaign__start_date"),
            end=Coalesce("end", "campaign__end_date"),
        )
        campaigns = []
        for row in rows:
            if not campaigns or row['id'] != campaigns[-1]['id']:
                campaigns.append(
                    dict(
                        id=row['id'],
                        name=row['name'],
                        start_date=row['start'],
                        end_date=row['end'],
                        status=row['campaign__status'],
                        ad_groups=[]
                    )
                )
            if row['ad_group_creations__id'] is not None:
                campaigns[-1]['ad_groups'].append(
                    dict(
                        id=row['ad_group_creations__id'],
                        name=row['ad_group_creations__name'],
                        status=row['ad_group_creations__ad_group__status'],
                    )
                )
        return campaigns

    @staticmethod
    def get_static_filters():
        filters = dict(
            average_cpv=dict(min=0, max=10),
            average_cpm=dict(min=0, max=100),
            ctr=dict(min=0, max=10),
            ctr_v=dict(min=0, max=20),
            video_view_rate=dict(min=0, max=100),
        )
        return filters

    def get(self, request, pk, **_):
        try:
            item = self.get_queryset().get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        if not item.is_managed and item.account:
            self.create_manage_objects(item)

        dates = AdGroupStatistic.objects.filter(ad_group__campaign__account=item.account).aggregate(
            min_date=Min("date"), max_date=Max("date"),
        )
        filters = self.get_static_filters()
        filters["start_date"] = dates["min_date"]
        filters["end_date"] = dates["max_date"]
        filters["campaigns"] = self.get_campaigns(item)
        return Response(data=filters)

    @staticmethod
    def create_manage_objects(account_creation):
        """
        The method creates campaign_creation and ad_group_creation instances from campaigns and ad groups
        :param account_creation:
        :return:
        """
        account = account_creation.account

        # create campaign creations
        existed_c_ids = account_creation.campaign_creations.all().values_list("campaign_id", flat=True)
        campaign_data = account.campaigns.exclude(id__in=existed_c_ids).values("id", "name")
        bulk_campaign = []
        for c in campaign_data:
            bulk_campaign.append(
                CampaignCreation(name=c["name"], campaign_id=c["id"], account_creation=account_creation)
            )
        if bulk_campaign:
            CampaignCreation.objects.bulk_create(bulk_campaign)

        # create ad_group_creations
        existed_a_ids = AdGroupCreation.objects.filter(
            campaign_creation__account_creation=account_creation
        ).values_list("ad_group_id", flat=True)
        ad_group_data = AdGroup.objects.filter(
            campaign__account=account
        ).exclude(id__in=existed_a_ids).values(
            "id", "name", "campaign_id"
        )
        campaign_creations_ids = account_creation.campaign_creations.all().values("id", "campaign_id")
        campaign_creations_ids = {c["campaign_id"]: c["id"] for c in campaign_creations_ids}
        bulk_ad_group = []
        for a in ad_group_data:
            campaign_creation_id = campaign_creations_ids.get(a["campaign_id"])
            if campaign_creation_id:
                bulk_ad_group.append(
                    AdGroupCreation(name=a['name'], ad_group_id=a["id"],
                                    campaign_creation_id=campaign_creation_id)
                )
        if bulk_ad_group:
            AdGroupCreation.objects.bulk_create(bulk_ad_group)


@demo_view_decorator
class PerformanceTargetingReportAPIView(APIView):

    def get_queryset(self):
        return AccountCreation.objects.filter(owner=self.request.user)

    def get_filters(self):
        data = self.request.data
        filters = dict(
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            campaigns=data.get("campaigns"),
            ad_groups=data.get("ad_groups"),
        )
        return filters

    def post(self, request, pk, list_type, **_):
        try:
            item = self.get_queryset().get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        order_by = ("campaign_creation__name", "campaign_creation_id", "name", "id")
        values = order_by + ("campaign_creation__campaign__status",)

        queryset = AdGroupCreation.objects.filter(
            campaign_creation__account_creation=item
        ).values(*values).order_by(*values).annotate(
            start=Coalesce("campaign_creation__start", "campaign_creation__campaign__start_date"),
            end=Coalesce("campaign_creation__end", "campaign_creation__campaign__end_date"),
        )
        queryset = self.filter_queryset(queryset)

        data = self.add_annotate(queryset, list_type)

        campaigns = []
        base_keys = set(base_stats_aggregate.keys())
        for i in data:
            campaign = campaigns[-1] if campaigns else None
            if not campaign or campaign['id'] != i['campaign_creation_id']:
                campaign = dict(
                    id=i['campaign_creation_id'],
                    name=i['campaign_creation__name'],
                    start_date=i['start'],
                    end_date=i['end'],
                    status=i['campaign_creation__campaign__status'],
                    ad_groups=[],
                    **{k: 0 for k in base_keys}
                )
                campaigns.append(campaign)

            ad_group = dict(
                id=i['id'],
                name=i['name'],
                **{k: i[k] for k in base_keys}
            )
            dict_norm_base_stats(ad_group)
            dict_calculate_stats(ad_group)
            campaign['ad_groups'].append(ad_group)

            for key in base_keys:
                campaign[key] += i[key] or 0

        for c in campaigns:
            dict_norm_base_stats(c)
            dict_calculate_stats(c)

        campaigns_settings = self.get_campaigns_settings(item)
        self.set_passes_fields(campaigns, campaigns_settings)
        return Response(data=campaigns)

    @staticmethod
    def get_campaigns_settings(account_creation):
        fields = CampaignOptimizationSetting.KPI_TYPES
        try:
            saved_settings = account_creation.optimization_setting
        except AccountOptimizationSetting.DoesNotExist:
            account_settings = AccountOptimizationSetting.default_settings
        else:
            account_settings = {k: getattr(saved_settings, k) for k in fields}

        campaign_settings = CampaignCreation.objects.filter(
            account_creation=account_creation
        ).values("id", *["optimization_setting__{}".format(f) for f in fields])

        response = {}
        for c in campaign_settings:
            c_settings = dict(**account_settings)
            c_settings.update(
                {f: c["optimization_setting__{}".format(f)]
                 for f in fields
                 if c["optimization_setting__{}".format(f)] is not None}
            )
            response[c['id']] = c_settings

        return response

    def set_passes_fields(self, campaigns, campaigns_settings):
        for campaign in campaigns:
            c_settings = campaigns_settings[campaign['id']]
            self.set_item_passes_fields(campaign, c_settings)
            for ad_group in campaign["ad_groups"]:
                self.set_item_passes_fields(ad_group, c_settings)

    @staticmethod
    def set_item_passes_fields(item, options):
        for k, v in options.items():
            if v is not None and item[k] is not None:
                item[k] = dict(value=item[k], passes=float(item[k]) >= float(v))

    def filter_queryset(self, qs):
        f = {}
        filters = self.get_filters()
        if filters["ad_groups"]:
            f["id__in"] = filters["ad_groups"]
        if filters["campaigns"]:
            f["campaign_creation_id__in"] = filters["campaigns"]
        qs = qs.filter(**f)
        return qs

    lookups = dict(
        channel="channel_statistics",
        video="managed_video_statistics",
        keyword="keywords",
        topic="topics",
        interest="audiences",
    )

    def add_annotate(self, qs, lt):
        lookup = self.lookups[lt]

        f = {}
        filters = self.get_filters()
        if filters["start_date"]:
            f["ad_group__{}__date__gte".format(lookup)] = filters["start_date"]
        if filters["end_date"]:
            f["ad_group__{}__date__lte".format(lookup)] = filters["end_date"]

        impressions_field = "ad_group__{}__impressions".format(lookup)
        views_field = "ad_group__{}__video_views".format(lookup)
        clicks_field = "ad_group__{}__clicks".format(lookup)
        cost_field = "ad_group__{}__cost".format(lookup)
        video_impressions_cond = {
            "ad_group__{}__video_views__gt".format(lookup): 0,
            "then": "ad_group__{}__impressions".format(lookup),
        }
        video_impressions_cond.update(f)
        qs = qs.annotate(
            sum_impressions=Sum(
                Case(
                    When(
                        then=impressions_field,
                        **f
                    ),
                    output_field=AggrIntegerField()
                )
                if f else impressions_field
            ),
            video_impressions=Sum(
                Case(
                    When(**video_impressions_cond),
                    output_field=AggrIntegerField()
                )
            ),
            sum_video_views=Sum(
                Case(
                    When(
                        then=views_field,
                        **f
                    ),
                    output_field=AggrIntegerField()
                )
                if f else views_field
            ),
            sum_clicks=Sum(
                Case(
                    When(
                        then=clicks_field,
                        **f
                    ),
                    output_field=AggrIntegerField()
                )
                if f else clicks_field
            ),
            sum_cost=Sum(
                Case(
                    When(
                        then=cost_field,
                        **f
                    ),
                    output_field=AggrIntegerField()
                )
                if f else cost_field
            ),
        )
        return qs


@demo_view_decorator
class PerformanceTargetingReportDetailsAPIView(APIView):

    def get_filters(self):
        data = self.request.data
        filters = dict(
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
        )
        return filters

    def get_queryset(self):
        qs = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=self.request.user
        )
        return qs

    def post(self, request, pk, list_type, **_):
        try:
            item = self.get_queryset().get(pk=pk)
        except AdGroupCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        method = "get_{}_items".format(list_type)
        items = getattr(self, method)(item.ad_group)

        ag_settings = self.get_ad_group_settings(item)
        self.set_passes_fields(items, ag_settings)

        return Response(data=items)

    @staticmethod
    def get_ad_group_settings(ad_group):
        fields = AccountOptimizationSetting.KPI_TYPES
        try:
            saved_settings = ad_group.campaign_creation.account_creation.optimization_setting
        except AccountOptimizationSetting.DoesNotExist:
            ad_group_settings = dict(**AccountOptimizationSetting.default_settings)
        else:
            ad_group_settings = {k: getattr(saved_settings, k) for k in fields}

        try:
            campaign_settings = ad_group.campaign_creation.optimization_setting
        except CampaignOptimizationSetting.DoesNotExist:
            pass
        else:
            campaign_settings = {k: getattr(campaign_settings, k) for k in fields}
            ad_group_settings.update(
                **{k: campaign_settings[k] for k in fields if campaign_settings[k] is not None}
            )
        return ad_group_settings

    @staticmethod
    def set_passes_fields(items, options):
        sort_fields = {k for k, v in options.items() if v is not None}
        if sort_fields:
            items = list(sorted(items, key=lambda i: tuple(i[f] or 0 for f in sort_fields), reverse=True))
            for item in items:
                for k, v in options.items():
                    if v is not None and item[k] is not None:
                        item[k] = dict(value=item[k], passes=float(item[k]) >= float(v))

    @staticmethod
    def get_channel_items(ad_group):
        items = YTChannelStatistic.objects.filter(
            ad_group=ad_group
        ).values("yt_id").order_by("yt_id").annotate(
            **base_stats_aggregate
        )

        info = {}
        ids = {i['yt_id'] for i in items}
        if ids:
            connector = SingleDatabaseApiConnector()
            try:
                resp = connector.get_custom_query_result(
                    model_name="channel",
                    fields=["id", "title", "thumbnail_image_url"],
                    id__in=list(ids),
                    limit=len(ids),
                )
                info = {r['id']: r for r in resp}
            except SingleDatabaseApiConnectorException as e:
                logger.error(e)

        for i in items:
            item_details = info.get(i['yt_id'], {})
            i["id"] = i['yt_id']
            i["name"] = item_details.get("title", i['yt_id'])
            i["thumbnail"] = item_details.get("thumbnail_image_url")
            del i['yt_id']
            dict_norm_base_stats(i)
            dict_calculate_stats(i)
        return items

    @staticmethod
    def get_video_items(ad_group):
        items = YTVideoStatistic.objects.filter(
            ad_group=ad_group
        ).values("yt_id").order_by("yt_id").annotate(
            **base_stats_aggregate
        )

        info = {}
        ids = {i['yt_id'] for i in items}
        if ids:
            connector = SingleDatabaseApiConnector()
            try:
                resp = connector.get_custom_query_result(
                    model_name="video",
                    fields=["id", "title", "thumbnail_image_url"],
                    id__in=list(ids),
                    limit=len(ids),
                )
                info = {r['id']: r for r in resp}
            except SingleDatabaseApiConnectorException as e:
                logger.error(e)

        for i in items:
            item_details = info.get(i['yt_id'], {})
            i["id"] = i['yt_id']
            i["name"] = item_details.get("title", i['yt_id'])
            i["thumbnail"] = item_details.get("thumbnail_image_url")
            del i['yt_id']
            dict_norm_base_stats(i)
            dict_calculate_stats(i)
        return items

    @staticmethod
    def get_keyword_items(ad_group):
        items = KeywordStatistic.objects.filter(
            ad_group=ad_group
        ).values("keyword").order_by("keyword").annotate(
            **base_stats_aggregate
        )
        for i in items:
            i["id"] = i['keyword']
            i["name"] = i['keyword']
            del i['keyword']
            dict_norm_base_stats(i)
            dict_calculate_stats(i)
        return items

    @staticmethod
    def get_interest_items(ad_group):
        group_by = ("audience__id", "audience__name")
        items = AudienceStatistic.objects.filter(
            ad_group=ad_group
        ).values(*group_by).order_by(*group_by).annotate(
            **base_stats_aggregate
        )
        for i in items:
            i["id"] = i['audience__id']
            i["name"] = i['audience__name']
            del i['audience__id'], i['audience__name']
            dict_norm_base_stats(i)
            dict_calculate_stats(i)
        return items

    @staticmethod
    def get_topic_items(ad_group):
        group_by = ("topic__id", "topic__name")
        items = TopicStatistic.objects.filter(
            ad_group=ad_group
        ).values(*group_by).order_by(*group_by).annotate(
            **base_stats_aggregate
        )
        for i in items:
            i["id"] = i['topic__id']
            i["name"] = i['topic__name']
            del i['topic__id'], i['topic__name']
            dict_norm_base_stats(i)
            dict_calculate_stats(i)
        return items


@demo_view_decorator
class PerformanceTargetingSettingsAPIView(APIView):

    def get_queryset(self):
        return AccountCreation.objects.filter(owner=self.request.user)

    def get(self, request, pk, **kwargs):
        try:
            account_creation = self.get_queryset().get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        return Response(data=self.get_settings(account_creation))

    @staticmethod
    def get_settings(account_creation):
        fields = AccountOptimizationSetting.KPI_TYPES

        account_settings = dict(
            id=account_creation.id,
            name=account_creation.name,
            campaign_creations=[],
        )
        try:
            saved_settings = account_creation.optimization_setting
        except AccountOptimizationSetting.DoesNotExist:
            account_settings.update(AccountOptimizationSetting.default_settings)
        else:
            account_settings.update(**{k: getattr(saved_settings, k) for k in fields})

        campaign_data = account_creation.campaign_creations.all().values("id", "name")

        campaign_settings = CampaignOptimizationSetting.objects.filter(
            item__in=set(i['id'] for i in campaign_data)
        ).values("item_id", *CampaignOptimizationSetting.KPI_TYPES)
        campaign_settings = {i['item_id']: i for i in campaign_settings}

        for e in campaign_data:
            if e['id'] in campaign_settings:
                s = campaign_settings[e['id']]
                e.update(**{k: s[k] for k in fields})
            else:
                e.update(AccountOptimizationSetting.default_settings)
            account_settings['campaign_creations'].append(e)
        return account_settings

    def put(self, request, pk, **kwargs):
        try:
            account_creation = self.get_queryset().get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        data = request.data
        fields = AccountOptimizationSetting.KPI_TYPES
        AccountOptimizationSetting.objects.update_or_create(
            item=account_creation, defaults={f: data[f] for f in fields if f in data}
        )

        for c_data in data.get("campaign_creations", []):
            campaign_creation = account_creation.campaign_creations.get(id=c_data["id"])
            CampaignOptimizationSetting.objects.update_or_create(
                item=campaign_creation, defaults={f: c_data[f] for f in fields if f in c_data}
            )

        return Response(data=self.get_settings(account_creation), status=HTTP_202_ACCEPTED)


class UserListsImportMixin:

    @staticmethod
    def get_lists_items_ids(ids, list_type):
        from segment.models import get_segment_model_by_type
        from keyword_tool.models import KeywordsList

        if list_type == "keyword":
            item_ids = KeywordsList.objects.filter(
                id__in=ids, keywords__text__isnull=False
            ).values_list(
                "keywords__text", flat=True
            ).order_by("keywords__text").distinct()
        else:
            manager = get_segment_model_by_type(list_type).objects
            item_ids = manager.filter(id__in=ids, related__related_id__isnull=False)\
                              .values_list('related__related_id', flat=True)\
                              .order_by('related__related_id')\
                              .distinct()
        return item_ids

# tools


class TopicToolListApiView(ListAPIView):
    serializer_class = TopicHierarchySerializer
    queryset = Topic.objects.filter(parent__isnull=True).order_by('name')


class TopicToolListExportApiView(TopicToolListApiView):
    permission_classes = (IsAuthQueryTokenPermission,)
    export_fields = ('id', 'name', 'parent_id')
    file_name = "topic_list"

    def get(self, request, *args, **kwargs):
        queryset = self.get_queryset()

        export_ids = request.GET.get('export_ids')
        if export_ids:
            export_ids = set(int(i) for i in export_ids.split(','))
        fields = self.export_fields

        def generator():
            def to_line(line):
                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(line)
                return output.getvalue()

            def item_with_children(item):
                if not export_ids or item.id in export_ids:
                    yield to_line(tuple(getattr(item, f) for f in fields))

                for i in item.children.all():
                    if not export_ids or i.id in export_ids:
                        yield to_line(tuple(getattr(i, f) for f in fields))

            yield to_line(fields)
            for topic in queryset:
                yield from item_with_children(topic)

        response = StreamingHttpResponse(
            generator(), content_type='text/csv')
        filename = "{}_{}.csv".format(
            self.file_name,
            datetime.now().strftime("%Y%m%d"),
        )
        response['Content-Disposition'] = 'attachment; filename=' \
                                          '"{}"'.format(filename)
        return response


# Tools
class AudienceToolListApiView(ListAPIView):
    serializer_class = AudienceHierarchySerializer
    queryset = Audience.objects.filter(
        parent__isnull=True,
        type__in=[Audience.AFFINITY_TYPE, Audience.IN_MARKET_TYPE],
    ).order_by('type', 'name')


class AudienceToolListExportApiView(TopicToolListExportApiView):
    permission_classes = (IsAuthQueryTokenPermission,)
    export_fields = ('id', 'name', 'parent_id', 'type')
    file_name = "audience_list"
    queryset = AudienceToolListApiView.queryset


# targeting lists
class TargetingListBaseAPIClass(GenericAPIView):
    serializer_class = AdGroupTargetingListSerializer

    def get_user(self):
        return self.request.user

    def get_queryset(self):
        pk = self.kwargs.get('pk')
        list_type = self.kwargs.get('list_type')
        queryset = TargetingItem.objects.filter(
            ad_group_creation_id=pk,
            type=list_type,
            ad_group_creation__campaign_creation__account_creation__owner
            =self.get_user()
        )
        return queryset

    @staticmethod
    def data_to_list(data):
        data = [i['criteria'] if type(i) is dict else i
                for i in data]
        return data

    def data_to_dicts(self, data):
        is_negative = self.request.GET.get('is_negative', False)
        data = [i if type(i) is dict
                else dict(criteria=str(i), is_negative=is_negative)
                for i in data]
        return data

    def add_items_info(self, data):
        list_type = self.kwargs.get('list_type')
        add_targeting_list_items_info(data, list_type)


@demo_view_decorator
class AdGroupTargetingListApiView(TargetingListBaseAPIClass):

    def get(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        data = self.get_serializer(queryset, many=True).data
        self.add_items_info(data)
        return Response(data=data)

    def post(self, request, *args, **kwargs):
        pk = self.kwargs.get('pk')
        list_type = self.kwargs.get('list_type')
        try:
            ad_group_creation = AdGroupCreation.objects.get(pk=pk)
        except AdGroupCreation.DoesNotExsist:
            return Response(status=HTTP_404_NOT_FOUND)

        data = self.data_to_dicts(request.data)
        data = self.drop_not_valid(data, list_type)

        criteria_sent = set(i['criteria'] for i in data)

        queryset = self.get_queryset()
        criteria_exists = set(
            queryset.filter(
                criteria__in=criteria_sent
            ).values_list('criteria', flat=True)
        )
        post_data = []
        for item in data:
            if item['criteria'] not in criteria_exists:
                item['type'] = list_type
                item['ad_group_creation'] = pk
                post_data.append(item)

        serializer = AdGroupTargetingListUpdateSerializer(
            data=post_data, many=True)
        serializer.is_valid(raise_exception=True)
        res = serializer.save()
        if res:
            ad_group_creation.save()
        response = self.get(request, *args, **kwargs)
        return response

    def delete(self, request, *args, **kwargs):
        criteria_list = self.data_to_list(request.data)
        count, details = self.get_queryset().filter(
            criteria__in=criteria_list
        ).delete()
        if count:
            pk = self.kwargs.get('pk')
            try:
                ad_group_creation = AdGroupCreation.objects.get(pk=pk)
            except AdGroupCreation.DoesNotExsist:
                pass
            else:
                ad_group_creation.save()

        response = self.get(request, *args, **kwargs)
        return response

    def drop_not_valid(self, objects_list, list_type):
        method = "drop_not_valid_{}".format(list_type)
        if hasattr(self, method):
            return getattr(self, method)(objects_list)
        else:
            return objects_list

    @staticmethod
    def drop_not_valid_topic(objects_list):
        existed_ids = set(
            Topic.objects.filter(
                id__in=[i['criteria'] for i in objects_list]
            ).values_list('id', flat=True)
        )
        valid_list = [i for i in objects_list
                      if int(i['criteria']) in existed_ids]
        return valid_list

    @staticmethod
    def drop_not_valid_interest(objects_list):
        existed_ids = set(
            Audience.objects.filter(
                type__in=(Audience.IN_MARKET_TYPE, Audience.AFFINITY_TYPE),
                id__in=[i['criteria'] for i in objects_list]
            ).values_list('id', flat=True)
        )
        valid_list = [i for i in objects_list
                      if int(i['criteria']) in existed_ids]
        return valid_list


@demo_view_decorator
class AdGroupCreationTargetingExportApiView(TargetingListBaseAPIClass):

    permission_classes = (IsAuthQueryTokenPermission,)

    def get_user(self):
        auth_token = self.request.query_params.get("auth_token")
        token = Token.objects.get(key=auth_token)
        return token.user

    def get_data(self):
        queryset = self.get_queryset()
        sub_list_type = self.kwargs["sub_list_type"]
        queryset = queryset.filter(is_negative=sub_list_type == "negative")
        data = self.get_serializer(queryset, many=True).data
        self.add_items_info(data)
        return data

    def get(self, request, pk, list_type, sub_list_type, **_):
        data = self.get_data()

        def generator():
            def to_line(line):
                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(line)
                return output.getvalue()

            fields = ['criteria', 'name']
            yield to_line(fields)
            for item in data:
                yield to_line(tuple(item.get(f) for f in fields))

        response = StreamingHttpResponse(generator(), content_type='text/csv')
        filename = "targeting_list_{}_{}_{}_{}.csv".format(
            datetime.now().strftime("%Y%m%d"), pk, list_type, sub_list_type
        )
        response['Content-Disposition'] = 'attachment; filename=' \
                                          '"{}"'.format(filename)
        return response


class TargetingItemsImportApiView(DocumentImportBaseAPIView):
    parser_classes = (FileUploadParser,)

    def post(self, request, list_type, **_):

        method = "import_{}_criteria".format(list_type)
        if not hasattr(self, method):
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Unsupported list type: {}".format(list_type))

        criteria_list = []
        for _, file_obj in request.data.items():
            fct = file_obj.content_type
            if fct == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                data = self.get_xlsx_contents(file_obj, return_lines=True)
            elif fct in ("text/csv", "application/vnd.ms-excel"):
                data = self.get_csv_contents(file_obj, return_lines=True)
            else:
                return Response(
                    status=HTTP_400_BAD_REQUEST,
                    data={"errors": ["The MIME type isn't supported: "
                                     "{}".format(file_obj.content_type)]})

            criteria_list.extend(getattr(self, method)(data))

        add_targeting_list_items_info(criteria_list, list_type)

        return Response(criteria_list)

    @staticmethod
    def import_keyword_criteria(data):
        kws = []
        data = list(data)
        for line in data[1:]:
            if len(line):
                criteria = line[0]
            else:
                continue

            if re.search(r"\w+", criteria):
                kws.append(
                    dict(criteria=criteria)
                )
        return kws

    @staticmethod
    def import_channel_criteria(data):
        channels = []
        channel_pattern = re.compile(r"[\w-]{24}")

        for line in data:
            criteria = None
            if len(line) > 1:
                first, second, *_ = line
            elif len(line):
                first, second = line[0], ""
            else:
                continue

            match = channel_pattern.search(second)
            if match:
                criteria = match.group(0)
            else:
                match = channel_pattern.search(first)
                if match:
                    criteria = match.group(0)

            if criteria:
                channels.append(
                    dict(criteria=criteria)
                )
        return channels

    @staticmethod
    def import_video_criteria(data):
        videos = []
        pattern = re.compile(r"[\w-]{11}")
        for line in data:
            criteria = None
            if len(line) > 1:
                first, second, *_ = line
            elif len(line):
                first, second = line[0], ""
            else:
                continue

            match = pattern.search(second)
            if match:
                criteria = match.group(0)
            else:
                match = pattern.search(first)
                if match:
                    criteria = match.group(0)

            if criteria:
                videos.append(
                    dict(criteria=criteria)
                )
        return videos

    @staticmethod
    def import_topic_criteria(data):
        objects = []
        topic_ids = set(Topic.objects.values_list('id', flat=True))
        for line in data:
            if len(line):
                criteria = line[0]
            else:
                continue

            try:
                criteria = int(criteria)
            except ValueError:
                continue
            else:
                if criteria in topic_ids:
                    objects.append(
                        dict(criteria=criteria)
                    )
        return objects

    @staticmethod
    def import_interest_criteria(data):
        objects = []
        interest_ids = set(
            Audience.objects.filter(
                type__in=(Audience.IN_MARKET_TYPE, Audience.AFFINITY_TYPE)
            ).values_list('id', flat=True)
        )
        for line in data:
            if len(line):
                criteria = line[0]
            else:
                continue

            try:
                criteria = int(criteria)
            except ValueError:
                continue
            else:
                if criteria in interest_ids:
                    objects.append(
                        dict(criteria=criteria)
                    )
        return objects


@demo_view_decorator
class AdGroupTargetingListImportListsApiView(AdGroupTargetingListApiView,
                                             UserListsImportMixin):

    def post(self, request, *args, **kwargs):
        pk = self.kwargs.get('pk')
        list_type = self.kwargs.get('list_type')
        is_negative = request.query_params.get('is_negative', False)
        try:
            ad_group_creation = AdGroupCreation.objects.get(pk=pk)
        except AdGroupCreation.DoesNotExsist:
            return Response(status=HTTP_404_NOT_FOUND)

        ids = request.data
        assert type(ids) is list

        if ids:
            criteria_list = self.get_lists_items_ids(ids, list_type)
            existed_ids = set(
                TargetingItem.objects.filter(
                    ad_group_creation=ad_group_creation,
                    criteria__in=criteria_list,
                    type=list_type,
                ).values_list("criteria", flat=True)
            )
            items = [
                TargetingItem(
                    ad_group_creation=ad_group_creation,
                    criteria=cid,
                    type=list_type,
                    is_negative=is_negative,
                )
                for cid in criteria_list if cid not in existed_ids
            ]
            if items:
                TargetingItem.objects.bulk_create(items)
        return self.get(request, *args, **kwargs)

    def delete(self, request, *args, **kwargs):
        raise NotImplementedError


class AwCreationChangedAccountsListAPIView(GenericAPIView):
    permission_classes = tuple()

    @staticmethod
    def get(*_, **kwargs):
        manager_id = kwargs.get('manager_id')
        ids = AccountCreation.objects.filter(
            account__managers__id=manager_id,
            account_id__isnull=False,
            is_managed=True,
            is_approved=True,
        ).exclude(
            sync_at__gte=F("updated_at"),
        ).values_list(
            "account_id", flat=True
        ).order_by("account_id").distinct()
        return Response(data=ids)


class AwCreationCodeRetrieveAPIView(GenericAPIView):
    permission_classes = tuple()

    @staticmethod
    def get(request, account_id, **_):
        try:
            account_management = AccountCreation.objects.get(
                account_id=account_id,
                is_managed=True,
            )
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        with open('aw_creation/aws_functions.js') as f:
            functions = f.read()
        code = functions + "\n" + account_management.get_aws_code(request)
        return Response(data={'code': code})


class AwCreationChangeStatusAPIView(GenericAPIView):
    permission_classes = tuple()

    @staticmethod
    def patch(request, account_id, **_):
        updated_at = request.data.get("updated_at")
        AccountCreation.objects.filter(
            account_id=account_id, is_managed=True,
        ).update(sync_at=updated_at)
        CampaignCreation.objects.not_empty().filter(
            account_creation__account_id=account_id,
            account_creation__is_managed=True,
        ).update(sync_at=updated_at)
        AdGroupCreation.objects.not_empty().filter(
            campaign_creation__account_creation__account_id=account_id,
            campaign_creation__account_creation__is_managed=True,
        ).update(sync_at=updated_at)
        AdCreation.objects.not_empty().filter(
            ad_group_creation__campaign_creation__account_creation__account_id=account_id,
            ad_group_creation__campaign_creation__account_creation__is_managed=True,
        ).update(sync_at=updated_at)

        # save campaigns and ad_groups
        campaigns = request.data.get("campaigns", [])
        existed_c_ids = set(Campaign.objects.filter(account_id=account_id).values_list("id", flat=True))
        existed_a_ids = set(AdGroup.objects.filter(campaign__account_id=account_id).values_list("id", flat=True))
        c_bulk = [Campaign(id=c['id'], name=c['name'], account_id=account_id)
                  for c in campaigns if c['id'] not in existed_c_ids]
        if c_bulk:
            Campaign.objects.bulk_create(c_bulk)

        a_bulk = [AdGroup(id=a['id'], name=a['name'], campaign_id=c['id'])
                  for c in campaigns for a in c['ad_groups']
                  if a['id'] not in existed_a_ids]
        if a_bulk:
            AdGroup.objects.bulk_create(a_bulk)

        # adding relations between campaign creations and campaign objects
        for account_creation in AccountCreation.objects.filter(account_id=account_id, is_managed=True):
            campaign_creations = account_creation.campaign_creations.all().values("id", "campaign_id")
            matched_campaign_ids = set(c['campaign_id'] for c in campaign_creations if c['campaign_id'])
            for campaign_creation in filter(lambda c: not c["campaign_id"], campaign_creations):
                # match campaign creations
                uid_key = "#{}".format(campaign_creation['id'])
                for campaign in filter(lambda c: c['id'] not in matched_campaign_ids, campaigns):
                    if campaign['name'].endswith(uid_key):
                        CampaignCreation.objects.filter(
                            id=campaign_creation['id']
                        ).update(campaign_id=campaign['id'])
                        break

            ad_group_creations = AdGroupCreation.objects.filter(
                campaign_creation__account_creation=account_creation
            ).values("id", "ad_group_id", "campaign_creation__campaign_id")
            matched_ad_group_ids = set(c['ad_group_id'] for c in ad_group_creations if c['ad_group_id'])
            for ad_group_creation in filter(lambda a: not a['ad_group_id'], ad_group_creations):
                uid_key = "#{}".format(ad_group_creation['id'])
                for campaign in filter(lambda c: ad_group_creation['campaign_creation__campaign_id'] == c['id'],
                                       campaigns):
                    for ad_group in filter(lambda a: a['id'] not in matched_ad_group_ids, campaign['ad_groups']):
                        if ad_group['name'].endswith(uid_key):
                            AdGroupCreation.objects.filter(
                                id=ad_group_creation['id']
                            ).update(ad_group_id=ad_group['id'])
                            break
        return Response()
