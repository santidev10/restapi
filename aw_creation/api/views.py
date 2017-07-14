# pylint: disable=import-error
from apiclient.discovery import build
# pylint: enable=import-error
from django.conf import settings
from django.db import transaction
from django.db.models import Avg, Value, Count, Case, When, \
    IntegerField as AggrIntegerField, DecimalField as AggrDecimalField
from django.http import StreamingHttpResponse, HttpResponse
from django.shortcuts import get_object_or_404
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
    TargetingItem, CampaignOptimizationTuning, AdGroupOptimizationTuning
from aw_reporting.demo import demo_view_decorator
from aw_reporting.api.views import DATE_FORMAT
from aw_reporting.api.serializers import CampaignListSerializer, AccountsListSerializer
from aw_reporting.models import CONVERSIONS, QUARTILE_STATS, dict_quartiles_to_rates, all_stats_aggregate, \
    VideoCreativeStatistic, GenderStatistic, Genders, AgeRangeStatistic, AgeRanges, Devices, \
    CityStatistic, DEFAULT_TIMEZONE, BASE_STATS, GeoTarget, SUM_STATS, dict_add_calculated_stats, \
    Topic, Audience
from aw_reporting.excel_reports import AnalyzeWeeklyReport
from aw_reporting.charts import DeliveryChart
from django.db.models import FloatField, ExpressionWrapper, IntegerField, F
from datetime import timedelta
from io import StringIO
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
        uid = data.get("id", {}).get("videoId")
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

    def get_queryset(self, **filters):
        sort_by = self.request.query_params.get('sort_by')
        if sort_by != "name":
            sort_by = "-created_at"

        queryset = AccountCreation.objects.filter(
            is_deleted=False,
            owner=self.request.user, **filters
        ).order_by('is_ended', sort_by)
        return queryset

    filters = ('search', 'show_closed',
               'min_campaigns_count', 'max_campaigns_count',
               'min_start', 'max_start', 'min_end', 'max_end')

    def get_filters(self):
        filters = {}
        query_params = self.request.query_params
        for f in self.filters:
            v = query_params.get(f)
            if v:
                filters[f] = v
        return filters

    def filter_queryset(self, queryset):
        filters = self.get_filters()

        if filters.get('show_closed') != "1":
            queryset = queryset.filter(is_ended=False)

        search = filters.get('search')
        if search:
            queryset = queryset.filter(name__icontains=search)

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
            queryset = queryset.annotate(start=Min("campaign_creations__start"))
            if min_start:
                queryset = queryset.filter(start__gte=min_start)
            if max_start:
                queryset = queryset.filter(start__lte=max_start)

        min_end = filters.get('min_end')
        max_end = filters.get('max_end')
        if min_end or max_end:
            queryset = queryset.annotate(end=Max("campaign_creations__end"))
            if min_end:
                queryset = queryset.filter(end__gte=min_end)
            if max_end:
                queryset = queryset.filter(end__lte=max_end)

        return queryset

    def post(self, *a, **_):
        account_count = AccountCreation.objects.filter(owner=self.request.user).count()

        with transaction.atomic():
            account_creation = AccountCreation.objects.create(
                name="Account {}".format(account_count + 1),
                owner=self.request.user,
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
        queryset = AccountCreation.objects.filter(owner=self.request.user)
        return queryset

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
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
        instance.is_deleted = True
        instance.save()
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
                pk=kwargs.get("pk"), owner=request.user
            )
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        request.data['account_creation'] = account_creation.id
        if not request.data.get('name'):
            count = self.get_queryset().count()
            request.data['name'] = "Campaign {}".format(count + 1)

        serializer = AppendCampaignCreationSerializer(
            data=request.data)
        serializer.is_valid(raise_exception=True)
        campaign_creation = serializer.save()

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

        request.data['campaign_creation'] = campaign_creation.id
        if not request.data.get('name'):
            count = self.get_queryset().count()
            request.data['name'] = "Ad Group {}".format(count + 1)

        serializer = AppendAdGroupCreationSetupSerializer(
            data=request.data)
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

        request.data['ad_group_creation'] = ad_group_creation.id
        if not request.data.get('name'):
            count = self.get_queryset().count()
            request.data['name'] = "Ad {}".format(count + 1)

        serializer = AppendAdCreationSetupSerializer(
            data=request.data)
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
            instance, data=request.data, partial=partial)
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

    duplicate_sign = " (copy)"
    account_fields = (
        "is_paused", "is_ended",
    )
    campaign_fields = (
        "name", "start", "end", "budget",
        "devices_raw", "delivery_method", "video_ad_format", "video_networks_raw",
        'content_exclusions_raw', 'genders_raw', 'age_ranges_raw', 'parents_raw',
    )
    loc_rules_fields = (
        "geo_target", "latitude", "longitude", "radius", "radius_units",
        "bid_modifier",
    )
    freq_cap_fields = ("event_type", "level", "limit", "time_unit")
    ad_schedule_fields = (
        "day", "from_hour", "from_minute", "to_hour", "to_minute",
    )
    ad_group_fields = (
        "name", "genders_raw", "parents_raw", "age_ranges_raw",
    )
    ad_fields = (
        "name", "video_url", "display_url", "final_url", "tracking_template", "custom_params",
    )
    targeting_fields = (
        "criteria", "type", "is_negative",
    )

    def get_queryset(self):
        queryset = AccountCreation.objects.filter(
            owner=self.request.user,
        )
        return queryset

    def post(self, *args, pk, **kwargs):
        try:
            instance = self.get_queryset().get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        data = self.duplicate_item(instance)
        return Response(data=data)

    def duplicate_item(self, account):
        account_data = dict(
            name=self.get_duplicate_name(account.name),
            owner=self.request.user,
        )
        for f in self.account_fields:
            account_data[f] = getattr(account, f)
        acc_duplicate = AccountCreation.objects.create(**account_data)

        for c in account.campaign_creations.all():
            camp_data = {f: getattr(c, f) for f in self.campaign_fields}
            c_duplicate = CampaignCreation.objects.create(
                account_creation=acc_duplicate, **camp_data
            )
            for l in c.languages.all():
                c_duplicate.languages.add(l)
            for r in c.location_rules.all():
                LocationRule.objects.create(
                    campaign_creation=c_duplicate,
                    **{f: getattr(r, f) for f in self.loc_rules_fields}
                )
            for i in c.frequency_capping.all():
                FrequencyCap.objects.create(
                    campaign_creation=c_duplicate,
                    **{f: getattr(i, f) for f in self.freq_cap_fields}
                )
            for i in c.ad_schedule_rules.all():
                AdScheduleRule.objects.create(
                    campaign_creation=c_duplicate,
                    **{f: getattr(i, f) for f in self.ad_schedule_fields}
                )
            for a in c.ad_group_creations.all():
                a_duplicate = AdGroupCreation.objects.create(
                    campaign_creation=c_duplicate,
                    **{f: getattr(a, f) for f in self.ad_group_fields}
                )
                for i in a.targeting_items.all():
                    TargetingItem.objects.create(
                        ad_group_creation=a_duplicate,
                        **{f: getattr(i, f) for f in self.targeting_fields}
                    )
                for ad in a.ad_creations.all():
                    AdCreation.objects.create(
                        ad_group_creation=a_duplicate,
                        **{f: getattr(ad, f) for f in self.ad_fields}
                    )

        account_data = self.serializer_class(acc_duplicate).data
        return account_data

    def get_duplicate_name(self, name):
        if len(name) + len(self.duplicate_sign) <= 250 and \
                        self.duplicate_sign not in name:
            name += self.duplicate_sign
        return name


@demo_view_decorator
class CampaignCreationDuplicateApiView(AccountCreationDuplicateApiView):
    serializer_class = CampaignCreationSetupSerializer

    def get_queryset(self):
        queryset = CampaignCreation.objects.filter(
            account_creation__owner=self.request.user,
        )
        return queryset

    def duplicate_item(self, item):
        duplicate = CampaignCreation.objects.create(
            account_creation=item.account_creation,
            **{f: getattr(item, f) for f in self.campaign_fields}
        )
        for l in item.languages.all():
            duplicate.languages.add(l)

        for r in item.location_rules.all():
            LocationRule.objects.create(
                campaign_creation=duplicate,
                **{f: getattr(r, f) for f in self.loc_rules_fields}
            )

        for i in item.frequency_capping.all():
            FrequencyCap.objects.create(
                campaign_creation=duplicate,
                **{f: getattr(i, f) for f in self.freq_cap_fields}
            )
        for i in item.ad_schedule_rules.all():
            AdScheduleRule.objects.create(
                campaign_creation=duplicate,
                **{f: getattr(i, f) for f in self.ad_schedule_fields}
            )
        for a in item.ad_group_creations.all():
            a_duplicate = AdGroupCreation.objects.create(
                campaign_creation=duplicate,
                **{f: getattr(a, f) for f in self.ad_group_fields}
            )
            for i in a.targeting_items.all():
                TargetingItem.objects.create(
                    ad_group_creation=a_duplicate,
                    **{f: getattr(i, f) for f in self.targeting_fields}
                )
            for ad in a.ad_creations.all():
                AdCreation.objects.create(
                    ad_group_creation=a_duplicate,
                    **{f: getattr(ad, f) for f in self.ad_fields}
                )
        data = self.serializer_class(duplicate).data
        return data


@demo_view_decorator
class AdGroupCreationDuplicateApiView(AccountCreationDuplicateApiView):
    serializer_class = AdGroupCreationSetupSerializer

    def get_queryset(self):
        queryset = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=self.request.user,
        )
        return queryset

    def duplicate_item(self, item):
        duplicate = AdGroupCreation.objects.create(
            campaign_creation=item.campaign_creation,
            **{f: getattr(item, f) for f in self.ad_group_fields}
        )
        for i in item.targeting_items.all():
            TargetingItem.objects.create(
                ad_group_creation=duplicate,
                **{f: getattr(i, f) for f in self.targeting_fields}
            )

        for ad in item.ad_creations.all():
            AdCreation.objects.create(
                ad_group_creation=duplicate,
                **{f: getattr(ad, f) for f in self.ad_fields}
            )
        data = self.serializer_class(duplicate).data
        return data


@demo_view_decorator
class AdCreationDuplicateApiView(AccountCreationDuplicateApiView):
    serializer_class = AdCreationSetupSerializer

    def get_queryset(self):
        queryset = AdCreation.objects.filter(
            ad_group_creation__campaign_creation__account_creation__owner=self.request.user,
        )
        return queryset

    def duplicate_item(self, item):
        duplicate = AdCreation.objects.create(
            ad_group_creation=item.ad_group_creation,
            **{f: getattr(item, f) for f in self.ad_fields}
        )
        data = self.serializer_class(duplicate).data
        return data


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
class AdGroupTargetingListExportApiView(TargetingListBaseAPIClass):

    permission_classes = (IsAuthQueryTokenPermission,)

    def get_user(self):
        auth_token = self.request.query_params.get("auth_token")
        token = Token.objects.get(key=auth_token)
        return token.user

    def get_data(self):
        queryset = self.get_queryset()
        data = self.get_serializer(queryset, many=True).data
        self.add_items_info(data)
        return data

    def get(self, request, *args, **kwargs):
        pk = self.kwargs.get('pk')
        list_type = self.kwargs.get('list_type')
        data = self.get_data()

        def generator():
            def to_line(line):
                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(line)
                return output.getvalue()

            fields = ['criteria', 'is_negative', 'name']
            yield to_line(fields)
            for item in data:
                yield to_line(tuple(item.get(f) for f in fields))
        response = StreamingHttpResponse(generator(), content_type='text/csv')
        filename = "ad_group_targeting_list_{}_{}_{}.csv".format(
            datetime.now().strftime("%Y%m%d"), pk, list_type
        )
        response['Content-Disposition'] = 'attachment; filename=' \
                                          '"{}"'.format(filename)
        return response


@demo_view_decorator
class AdGroupTargetingListImportApiView(AdGroupTargetingListApiView,
                                        DocumentImportBaseAPIView):
    parser_classes = (FileUploadParser,)

    def post(self, request, *args, **kwargs):

        pk = self.kwargs.get('pk')
        list_type = self.kwargs.get('list_type')
        method = "import_{}_criteria".format(list_type)
        if not hasattr(self, method):
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Unsupported list type: {}".format(list_type))

        criteria_list = []
        for _, file_obj in request.data.items():

            file_obj = request.data['file']
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

        if criteria_list:
            existed_criteria = list(
                self.get_queryset().filter(
                    criteria__in=[i['criteria'] for i in criteria_list]
                ).values_list('criteria', flat=True)
            )

            to_create = []
            for i in criteria_list:
                criteria = str(i['criteria'])
                if criteria not in existed_criteria:
                    existed_criteria.append(criteria)
                    to_create.append(i)

            if to_create:
                is_negative = request.GET.get('is_negative', False)
                for i in to_create:
                    if i.get('is_negative') is None:
                        i['is_negative'] = is_negative
                    i['ad_group_creation'] = pk
                    i['type'] = list_type

                serializer = AdGroupTargetingListUpdateSerializer(
                    data=to_create, many=True)
                serializer.is_valid(raise_exception=True)
                serializer.save()

                try:
                    ad_group_creation = AdGroupCreation.objects.get(
                        pk=pk
                    )
                    ad_group_creation.save()
                except AdGroupCreation.DoesNotExsist:
                    pass

        response = self.get(request, *args, **kwargs)
        return response

    @staticmethod
    def import_keyword_criteria(data):
        kws = []
        data = list(data)
        for line in data[1:]:
            if len(line) > 1:
                criteria, is_negative, *_ = line
                if is_negative == "True":
                    is_negative = True
                elif is_negative == "False":
                    is_negative = False
                else:
                    is_negative = None
            elif len(line):
                criteria, is_negative = line[0], None
            else:
                continue

            if re.search(r"\w+", criteria):
                kws.append(
                    dict(criteria=criteria, is_negative=is_negative)
                )
        return kws

    @staticmethod
    def import_channel_criteria(data):
        channels = []
        channel_pattern = re.compile(r"[\w-]{24}")

        for line in data:
            criteria, is_negative = None, None
            if len(line) > 1:
                first, second, *_ = line
                if second is "True":
                    is_negative = True
                elif second is "False":
                    is_negative = False

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
                    dict(criteria=criteria, is_negative=is_negative)
                )
        return channels

    @staticmethod
    def import_video_criteria(data):
        videos = []
        pattern = re.compile(r"[\w-]{11}")
        for line in data:
            criteria, is_negative = None, None
            if len(line) > 1:
                first, second, *_ = line
                if second is "True":
                    is_negative = True
                elif second is "False":
                    is_negative = False

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
                    dict(criteria=criteria, is_negative=is_negative)
                )
        return videos

    @staticmethod
    def import_topic_criteria(data):
        objects = []
        topic_ids = set(Topic.objects.values_list('id', flat=True))
        for line in data:
            if len(line) > 1:
                criteria, is_negative, *_ = line
                is_negative = is_negative == "True" if is_negative in ("True", "False") else None
            elif len(line):
                criteria, is_negative = line[0], False
            else:
                continue

            try:
                criteria = int(criteria)
            except ValueError:
                continue
            else:
                if criteria in topic_ids:
                    objects.append(
                        dict(criteria=criteria, is_negative=is_negative)
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
            if len(line) > 1:
                criteria, is_negative, *_ = line
                is_negative = is_negative == "True" if is_negative in ("True", "False") else None
            elif len(line):
                criteria, is_negative = line[0], False
            else:
                continue

            try:
                criteria = int(criteria)
            except ValueError:
                continue
            else:
                if criteria in interest_ids:
                    objects.append(
                        dict(criteria=criteria, is_negative=is_negative)
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


# optimize tab
@demo_view_decorator
class OptimizationFiltersApiView(APIView):

    def get_object(self):
        pk = self.kwargs.get("pk")
        account = get_object_or_404(
            AccountCreation,
            owner=self.request.user,
            pk=pk,
        )
        return account

    def get(self, request, pk, kpi, **_):
        account_creation = self.get_object()
        queryset = account_creation.campaign_creations.filter(
            Q(optimization_tuning__value__isnull=False,
              optimization_tuning__kpi=kpi) |
            Q(ad_group_creations__optimization_tuning__value__isnull=False,
              ad_group_creations__optimization_tuning__kpi=kpi)
        ).order_by('id').distinct()
        data = OptimizationFiltersCampaignSerializer(
            queryset, many=True, kpi=kpi,
        ).data
        return Response(data=data)


@demo_view_decorator
class OptimizationSettingsApiView(OptimizationFiltersApiView):
    """
    Settings at the Optimization tab
    """

    def get(self, request, pk, kpi, **_):
        account_creation = self.get_object()
        data = OptimizationSettingsSerializer(
            account_creation, kpi=kpi).data
        return Response(data=data)

    def put(self, request, pk, kpi, **kwargs):
        account_creation = self.get_object()

        campaign_creations = request.data.get('campaign_creations', [])
        if campaign_creations:
            c_ids = set(
                CampaignCreation.objects.filter(
                    account_creation=account_creation
                ).values_list('id', flat=True)
            )
            for i in campaign_creations:
                if i['id'] in c_ids:
                    CampaignOptimizationTuning.objects.update_or_create(
                        dict(value=i['value']),
                        item_id=i['id'],
                        kpi=kpi,
                    )

        ad_group_creations = request.data.get('ad_group_creations', [])
        if ad_group_creations:
            a_ids = set(
                AdGroupCreation.objects.filter(
                    campaign_creation__account_creation=account_creation
                ).values_list('id', flat=True)
            )
            for i in ad_group_creations:
                if i['id'] in a_ids:
                    AdGroupOptimizationTuning.objects.update_or_create(
                        dict(value=i['value']),
                        item_id=i['id'],
                        kpi=kpi,
                    )

        return self.get(request, pk, kpi, **kwargs)


@demo_view_decorator
class OptimizationTargetingApiView(OptimizationFiltersApiView,
                                   TargetingListBaseAPIClass):

    def get_filters(self):
        filters = {}
        qp = self.request.query_params
        campaign_creations = [
            uid
            for uid in qp.get('campaign_creations', "").split(",")
            if uid
        ]
        if campaign_creations:
            filters['campaign_creation_id__in'] = campaign_creations

        ad_group_creations = [
            uid
            for uid in qp.get('ad_group_creations', "").split(",")
            if uid
        ]
        if ad_group_creations:
            filters['id__in'] = ad_group_creations
        return filters

    def get(self, request, pk, kpi, list_type, **_):
        account_creation = self.get_object()
        filters = self.get_filters()
        ad_group_creations = AdGroupCreation.objects.filter(
            campaign_creation__account_creation=account_creation,
            **filters
        ).filter(
            Q(optimization_tuning__value__isnull=False,
              optimization_tuning__kpi=kpi) |
            Q(campaign_creation__optimization_tuning__value__isnull=False,
              campaign_creation__optimization_tuning__kpi=kpi)
        )

        values = ad_group_creations.aggregate(
            value=Avg(
                Case(
                    When(
                        campaign_creation__optimization_tuning__value__isnull=False,
                        campaign_creation__optimization_tuning__kpi=kpi,
                        then="campaign_creation__optimization_tuning__value",
                    ),
                    When(
                        optimization_tuning__value__isnull=False,
                        optimization_tuning__kpi=kpi,
                        then="optimization_tuning__value",
                    ),
                    output_field=AggrDecimalField()
                )
            )
        )
        value = values['value']
        items = []
        if ad_group_creations:
            items = TargetingItem.objects.filter(
                ad_group_creation__in=ad_group_creations,
                type=list_type,
                is_negative=False,
            ).values('criteria').order_by('criteria').distinct()
            self.add_items_info(items)
            self.add_items_stats(items, kpi, value)

        return Response(data=dict(items=items, value=value))

    @staticmethod
    def add_items_stats(items, kpi, value):
        stats = dict(zip(SUM_STATS, (0 for _ in range(len(SUM_STATS)))))
        dict_add_calculated_stats(stats)
        for i in items:
            i.update(stats)
            i['bigger_than_value'] = (i.get(kpi) or 0) > (value or 0)
