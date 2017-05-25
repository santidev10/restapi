import calendar
import csv
import re
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
from io import StringIO

from apiclient.discovery import build
from django.conf import settings
from django.db import transaction
from django.db.models import Q, Avg, Max, When, Case, Value, \
    IntegerField as AggrIntegerField, DecimalField as AggrDecimalField
from django.http import StreamingHttpResponse
from django.shortcuts import get_object_or_404
from django.utils import timezone
from openpyxl import load_workbook
from rest_framework.generics import ListAPIView, RetrieveUpdateAPIView, \
    GenericAPIView, ListCreateAPIView
from rest_framework.pagination import PageNumberPagination
from rest_framework.parsers import FileUploadParser
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, \
    HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_404_NOT_FOUND, HTTP_201_CREATED
from rest_framework.views import APIView
from utils.permissions import IsAuthQueryTokenPermission
from rest_framework.authtoken.models import Token

from aw_creation.api.serializers import add_targeting_list_items_info, \
    SimpleGeoTargetSerializer, OptimizationAdGroupSerializer, LocationRuleSerializer, \
    OptimizationAccountDetailsSerializer, FrequencyCapUpdateSerializer, FrequencyCapSerializer, \
    OptimizationCampaignsSerializer, OptimizationAccountListSerializer, \
    OptimizationUpdateAccountSerializer, OptimizationCreateAccountSerializer, \
    OptimizationUpdateCampaignSerializer, OptimizationCreateCampaignSerializer, \
    OptimizationLocationRuleUpdateSerializer, OptimizationAdGroupUpdateSerializer, TopicHierarchySerializer, \
    AudienceHierarchySerializer, AdGroupTargetingListSerializer, \
    AdGroupTargetingListUpdateSerializer, OptimizationFiltersCampaignSerializer, OptimizationSettingsSerializer, \
    OptimizationAppendCampaignSerializer, OptimizationAppendAdGroupSerializer

from aw_creation.models import BULK_CREATE_CAMPAIGNS_COUNT, \
    BULK_CREATE_AD_GROUPS_COUNT, AccountCreation, CampaignCreation, \
    AdGroupCreation, FrequencyCap, Language, LocationRule, AdScheduleRule,\
    TargetingItem, CampaignOptimizationTuning, AdGroupOptimizationTuning
from aw_reporting.models import GeoTarget, SUM_STATS, CONVERSIONS, \
    dict_add_calculated_stats, Topic, Audience


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


class OptimizationAccountListPaginator(PageNumberPagination):
    page_size = 100

    def get_paginated_response(self, data):
        """
        Update response to return
        """
        response_data = {
            'items_count': self.page.paginator.count,
            'items': data,
            'current_page': self.page.number,
            'max_page': self.page.paginator.num_pages,
        }
        return Response(response_data)


class OptimizationOptionsApiView(APIView):

    @staticmethod
    def get(*_, **k):
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
            campaign_count=list_to_resp(
                range(1, BULK_CREATE_CAMPAIGNS_COUNT + 1)
            ),
            ad_group_count=list_to_resp(
                range(1, BULK_CREATE_AD_GROUPS_COUNT + 1)
            ),
            name="string;max_length=250;required;validation=^[^#']*$",
            # create and update
            video_ad_format=opts_to_response(
                AccountCreation.VIDEO_AD_FORMATS[:1],
            ),
            # update
            goal_type=opts_to_response(
                AccountCreation.GOAL_TYPES[:1],
            ),
            type=opts_to_response(
                AccountCreation.CAMPAIGN_TYPES[:1],
            ),
            bidding_type=opts_to_response(
                AccountCreation.BIDDING_TYPES,
            ),
            delivery_method=opts_to_response(
                AccountCreation.DELIVERY_METHODS[:1],
            ),
            video_networks=opts_to_response(
                AccountCreation.VIDEO_NETWORKS,
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


class OptimizationAccountListApiView(ListAPIView):

    serializer_class = OptimizationAccountListSerializer
    pagination_class = OptimizationAccountListPaginator

    def get_queryset(self, **filters):
        sort_by = self.request.query_params.get('sort_by')
        if sort_by != "name":
            sort_by = "-created_at"

        today = datetime.now().date()
        queryset = AccountCreation.objects.filter(
            owner=self.request.user, **filters
        ).annotate(
            campaigns_status=Max(
                Case(
                    When(
                        campaign_creations__end__lt=today,
                        then=Value(2),  # ended
                    ),
                    When(
                        campaign_creations__end__gte=today,
                        then=Value(1),  # live
                    ),
                    default=Value(0),
                    output_field=AggrIntegerField()
                )
            )
        ).annotate(
            ended_status=Case(
                When(
                    campaigns_status__lt=2,
                    is_ended=False,
                    then=Value(0),  # shown by default
                ),
                default=Value(1),
                output_field=AggrIntegerField()
            )
        ).order_by('ended_status', sort_by)
        return queryset

    def filter_queryset(self, queryset):
        if not self.request.query_params.get('show_closed', False):
            queryset = queryset.filter(
                ended_status__lt=1,
            )
        search = self.request.query_params.get('search', '').strip()
        if search:
            queryset = queryset.filter(name__icontains=search)
        return queryset

    def post(self, request, *args, **kwargs):
        data = request.data
        video_ad_format = data.get('video_ad_format')
        campaign_count = data.get('campaign_count')
        ad_group_count = data.get('ad_group_count')
        assert video_ad_format == AccountCreation.IN_STREAM_TYPE
        assert 0 < campaign_count <= BULK_CREATE_CAMPAIGNS_COUNT
        assert 0 < ad_group_count <= BULK_CREATE_AD_GROUPS_COUNT

        with transaction.atomic():
            account_creation = AccountCreation.objects.create(
                name=data.get('name'),
                video_ad_format=video_ad_format,
                owner=self.request.user,
            )
            for i in range(campaign_count):
                c_uid = i + 1
                campaign_creation = CampaignCreation.objects.create(
                    name="Campaign {}".format(c_uid),
                    account_creation=account_creation,
                )
                for j in range(ad_group_count):
                    a_uid = j + 1
                    AdGroupCreation.objects.create(
                        name="AdGroup {}.{}".format(c_uid, a_uid),
                        campaign_creation=campaign_creation,
                    )

        obj = self.get_queryset(pk=account_creation.pk)[0]
        data = OptimizationAccountDetailsSerializer(obj).data
        return Response(status=HTTP_202_ACCEPTED, data=data)


class OptimizationAccountApiView(RetrieveUpdateAPIView):

    serializer_class = OptimizationAccountDetailsSerializer

    def get_queryset(self):
        queryset = AccountCreation.objects.filter(
            owner=self.request.user
        )
        return queryset

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        serializer = OptimizationUpdateAccountSerializer(
            instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)
        return self.retrieve(self, request, *args, **kwargs)


class OptimizationCampaignListApiView(ListCreateAPIView):
    serializer_class = OptimizationCampaignsSerializer

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

        serializer = OptimizationAppendCampaignSerializer(
            data=request.data)
        serializer.is_valid(raise_exception=True)
        campaign_creation = serializer.save()

        data = self.get_serializer(instance=campaign_creation).data
        return Response(data, status=HTTP_201_CREATED)


class OptimizationCampaignApiView(RetrieveUpdateAPIView):
    serializer_class = OptimizationCampaignsSerializer

    def get_queryset(self):
        queryset = CampaignCreation.objects.filter(
            account_creation__owner=self.request.user
        )
        return queryset

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        serializer = OptimizationUpdateCampaignSerializer(
            instance, data=request.data, partial=partial)
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


class OptimizationAdGroupListApiView(ListCreateAPIView):
    serializer_class = OptimizationAdGroupSerializer

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
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        request.data['campaign_creation'] = campaign_creation.id
        if not request.data.get('name'):
            count = self.get_queryset().count()
            request.data['name'] = "Campaign {}".format(count + 1)

        serializer = OptimizationAppendAdGroupSerializer(
            data=request.data)
        serializer.is_valid(raise_exception=True)
        obj = serializer.save()

        data = self.get_serializer(instance=obj).data
        return Response(data, status=HTTP_201_CREATED)


class OptimizationAdGroupApiView(RetrieveUpdateAPIView):
    serializer_class = OptimizationAdGroupSerializer

    def get_queryset(self):
        queryset = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=self.request.user
        )
        return queryset

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        serializer = OptimizationAdGroupUpdateSerializer(
            instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return self.retrieve(self, request, *args, **kwargs)


class CreationOptionsApiView(APIView):
    """
    Returns a list of fields (with values sometimes)
    that could be sent during the account creation process
    """

    @staticmethod
    def get(*_, **k):
        def opts_to_response(opts):
            res = [dict(id=i, name=n) for i, n in opts]
            return res

        def list_to_resp(l, n_func=None):
            n_func = n_func or (lambda e: e)
            return [dict(id=i, name=n_func(i)) for i in l]

        def get_week_day_name(n):
            return calendar.day_name[n - 1]

        options = OrderedDict(
            # 1
            video_ad_format=opts_to_response(
                AccountCreation.VIDEO_AD_FORMATS[:1],
            ),
            # 2
            name="string;max_length=250;required;validation=^[^#']*$",
            campaign_count=list_to_resp(
                range(1, BULK_CREATE_CAMPAIGNS_COUNT + 1)
            ),
            ad_group_count=list_to_resp(
                range(1, BULK_CREATE_AD_GROUPS_COUNT + 1)
            ),
            # 3
            video_url="url;validation=valid_yt_video",
            ct_overlay_text="string;max_length=200",
            display_url="string;max_length=200",
            final_url="url;max_length=200",

            # 4
            genders=opts_to_response(
                AdGroupCreation.GENDERS,
            ),
            parents=opts_to_response(
                AdGroupCreation.PARENTS,
            ),
            age_ranges=opts_to_response(
                AdGroupCreation.AGE_RANGES,
            ),
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

            # 5
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

            # 6
            devices=opts_to_response(CampaignCreation.DEVICES),
            frequency_capping=dict(
                event_type=opts_to_response(FrequencyCap.EVENT_TYPES),
                level=opts_to_response(FrequencyCap.LEVELS),
                limit='positive integer;max_value=65534',
                time_unit=opts_to_response(FrequencyCap.TIME_UNITS),
                __help="a list of two objects is allowed"
                       "(for each of the event types);",
            ),

            # 7
            start="date",
            end="date",
            goal_type=opts_to_response(
                AccountCreation.GOAL_TYPES[:1],
            ),
            goal_units="integer;max_value=4294967294",
            budget="decimal;max_digits=10,decimal_places=2",
            max_rate="decimal;max_digits=6,decimal_places=3",

            # 8
            channel_lists="array of user lists' ids",
            video_lists="array of user lists' ids",
            keyword_lists="array of user lists' ids",
            topic_lists="array of user lists' ids",
            interest_lists="array of user lists' ids",
        )
        return Response(data=options)


class CreationAccountApiView(APIView):
    """
    Accepts POST request and creates an account
    Example body:
    {"video_ad_format":"TRUE_VIEW_IN_STREAM","name":"T-800","campaign_count":2,"ad_group_count":2,"ct_overlay_text":"be be bee","display_url":"https://saas-rc.channelfactory.com/","final_url":"https://saas-rc.channelfactory.com/","video_url":"https://youtube.com/video/OPYcFQxsKlQ","genders":["GENDER_FEMALE","GENDER_MALE"],"parents":["PARENT_PARENT","PARENT_NOT_A_PARENT","PARENT_UNDETERMINED"],"age_ranges":["AGE_RANGE_18_24","AGE_RANGE_25_34","AGE_RANGE_35_44","AGE_RANGE_45_54","AGE_RANGE_55_64","AGE_RANGE_65_UP"],"languages":[1000,1036],"location_rules":[{"latitude":40.7127837,"longitude":-74.005941,"geo_target":200501,"east":-73.700272,"north":40.9152555,"south":40.4960439,"west":-74.255734}],"devices":["DESKTOP_DEVICE","MOBILE_DEVICE"],"frequency_capping":[{"event_type":"IMPRESSION","level":"CAMPAIGN","limit":5,"time_unit":"DAY"}],"start":"2017-05-24","end":"2017-05-31","budget":100,"goal_units":1000,"goal_type":"GOAL_VIDEO_VIEWS","max_rate":5,"channel_lists":[],"video_lists":[],"interest_lists":[],"topic_lists":[],"keyword_lists":[]}
    """

    def post(self, request, *args, **kwargs):
        from segment.models import Segment
        from keyword_tool.models import KeywordsList

        owner = self.request.user
        data = request.data

        v_ad_format = data.get('video_ad_format')
        campaign_count = data.get('campaign_count', 1)
        ad_group_count = data.get('ad_group_count', 1)
        goal_units = data.get('goal_units', 0)
        budget = data.get('budget', 0)
        assert 0 < campaign_count <= BULK_CREATE_CAMPAIGNS_COUNT
        assert 0 < ad_group_count <= BULK_CREATE_AD_GROUPS_COUNT

        channel_lists = data.get('channel_lists', [])
        if channel_lists:
            channel_ids = Segment.objects.filter(
                id__in=channel_lists
            ).values_list("channels__channel_id", flat=True).distinct()

            def set_channel_targeting(ad_group):
                items = [
                    TargetingItem(
                        ad_group_creation=ad_group,
                        criteria=cid,
                        type=TargetingItem.CHANNEL_TYPE,
                    )
                    for cid in channel_ids
                ]
                if items:
                    TargetingItem.objects.bulk_create(items)
        else:
            def set_channel_targeting(ad_group):
                pass

        video_lists = data.get('video_lists', [])
        if video_lists:
            video_ids = Segment.objects.filter(
                id__in=video_lists
            ).values_list("videos__video_id", flat=True).distinct()

            def set_video_targeting(ad_group):
                items = [
                    TargetingItem(
                        ad_group_creation=ad_group,
                        criteria=uid,
                        type=TargetingItem.VIDEO_TYPE,
                    )
                    for uid in video_ids
                ]
                if items:
                    TargetingItem.objects.bulk_create(items)
        else:
            def set_video_targeting(ad_group):
                pass

        keyword_lists = data.get('keyword_lists', [])
        if keyword_lists:
            kws = KeywordsList.objects.filter(
                id__in=keyword_lists
            ).values_list("keywords__text", flat=True).distinct()

            def set_kw_targeting(ad_group):
                items = [
                    TargetingItem(
                        ad_group_creation=ad_group,
                        criteria=kw,
                        type=TargetingItem.KEYWORD_TYPE,
                    )
                    for kw in kws
                ]
                if items:
                    TargetingItem.objects.bulk_create(items)
        else:
            def set_kw_targeting(ad_group):
                pass

        with transaction.atomic():
            account_data = dict(
                name=data.get('name'),
                owner=owner.id,
                video_networks=[
                    i[0] for i in AccountCreation.VIDEO_NETWORKS
                ],
            )
            account_data.update(data)
            serializer = OptimizationCreateAccountSerializer(
                data=account_data)
            serializer.is_valid(raise_exception=True)
            account_creation = serializer.save()

            for i in range(campaign_count):
                c_uid = i + 1
                # campaign goal
                c_goal = goal_units // campaign_count
                if i == 0:
                    c_goal += goal_units % campaign_count
                # campaign budget
                c_budget = budget // campaign_count
                if i == 0:
                    c_budget += budget % campaign_count

                campaign_data = dict(**data)
                campaign_data.update(dict(
                    name="Campaign {}".format(c_uid),
                    account_creation=account_creation.id,
                    goal_units=c_goal,
                    budget=c_budget,
                ))
                serializer = OptimizationCreateCampaignSerializer(
                    data=campaign_data,
                )
                serializer.is_valid(raise_exception=True)
                campaign_creation = serializer.save()
                # ad_schedule, location, freq_capping
                OptimizationCampaignApiView.update_related_models(
                    campaign_creation.id, data
                )

                for j in range(ad_group_count):
                    a_uid = j + 1
                    ag_data = dict(
                        name="AdGroup {}.{}".format(c_uid, a_uid),
                        campaign_creation=campaign_creation.id,
                    )
                    ag_data.update(data)
                    serializer = OptimizationAdGroupUpdateSerializer(
                        data=ag_data,
                    )
                    serializer.is_valid(raise_exception=True)
                    ag_creation = serializer.save()
                    set_channel_targeting(ag_creation)
                    set_video_targeting(ag_creation)
                    set_kw_targeting(ag_creation)

        age_ranges = data['age_ranges']
        parents = data['parents']
        genders = data['genders']
        devices = data['devices']
        goal_type = data['goal_type']

        response = OrderedDict(
            id=account_creation.id,
            name=account_creation.name,
            video_ad_format=dict(
                id=account_creation.video_ad_format,
                name=dict(AccountCreation.VIDEO_AD_FORMATS)[v_ad_format]
            ),
            campaign_count=campaign_count,
            ad_group_count=ad_group_count,

            video_url=data['video_url'],
            ct_overlay_text=data['ct_overlay_text'],
            display_url=data['display_url'],
            final_url=data['final_url'],
            age_ranges=[dict(id=uid, name=n)
                        for uid, n in AdGroupCreation.AGE_RANGES
                        if uid in age_ranges],
            parents=[dict(id=uid, name=n)
                     for uid, n in AdGroupCreation.PARENTS
                     if uid in parents],
            genders=[dict(id=uid, name=n)
                     for uid, n in AdGroupCreation.GENDERS
                     if uid in genders],

            languages=Language.objects.filter(
                campaigns__account_creation=account_creation
            ).values('id', 'name').distinct(),
            location_rules=LocationRuleSerializer(
                LocationRule.objects.filter(
                    campaign_creation__account_creation=account_creation
                ),
                many=True, read_only=True,
            ).data,

            devices=[dict(id=uid, name=n)
                     for uid, n in CampaignCreation.DEVICES
                     if uid in devices],
            frequency_capping=FrequencyCapSerializer(
                FrequencyCap.objects.filter(
                    campaign_creation__account_creation=account_creation
                ).distinct(),
                many=True, read_only=True,
            ).data,
            start=data['start'], end=data['end'],

            goal_units=data['goal_units'],
            goal_type=dict(
                id=goal_type,
                name=dict(AccountCreation.GOAL_TYPES)[goal_type]
            ),
            budget=data['budget'],
            max_rate=data['max_rate'],
        )

        return Response(data=response)


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
            ad_group_creation__campaign_creation__account_creation__owner=self.get_user()
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


class AdGroupTargetingListExportApiView(TargetingListBaseAPIClass):

    permission_classes = (IsAuthQueryTokenPermission,)

    def get_user(self):
        auth_token = self.request.query_params.get("auth_token")
        token = Token.objects.get(key=auth_token)
        return token.user

    def get(self, request, *args, **kwargs):
        pk = self.kwargs.get('pk')
        list_type = self.kwargs.get('list_type')

        queryset = self.get_queryset()
        data = self.get_serializer(queryset, many=True).data
        self.add_items_info(data)

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
                if is_negative is "True":
                    is_negative = True
                elif is_negative is "False":
                    is_negative = True
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
                is_negative = is_negative == "True"
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
                is_negative = is_negative == "True"
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


# optimize tab
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
        stat_names = SUM_STATS + CONVERSIONS
        stats = dict(zip(stat_names, (0 for _ in range(len(stat_names)))))
        dict_add_calculated_stats(stats)
        for i in items:
            i.update(stats)
            i['bigger_than_value'] = (i.get(kpi) or 0) > (value or 0)
