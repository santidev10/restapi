import calendar
import csv
import itertools
import re
from collections import OrderedDict
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from io import StringIO

import isodate
from apiclient.discovery import build
from django.conf import settings
from django.templatetags.static import static
from django.db import transaction
from django.db.models import Case, Q
from django.db.models import F
from django.db.models import IntegerField as AggrIntegerField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Value
from django.db.models import When
from django.http import Http404
from django.http import StreamingHttpResponse
from django.shortcuts import get_object_or_404
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from openpyxl import load_workbook
from rest_framework.generics import GenericAPIView
from rest_framework.generics import ListAPIView
from rest_framework.generics import ListCreateAPIView
from rest_framework.generics import RetrieveUpdateAPIView
from rest_framework.generics import UpdateAPIView
from rest_framework.parsers import FileUploadParser
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.api.serializers import *
from aw_creation.api.views import schemas
from aw_creation.api.views.schemas import CREATION_OPTIONS_SCHEMA
from aw_creation.models import AccountCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import AdScheduleRule
from aw_creation.models import CampaignCreation
from aw_creation.models import FrequencyCap
from aw_creation.models import Language
from aw_creation.models import LocationRule
from aw_creation.models import TargetingItem
from aw_creation.models import default_languages
from aw_creation.models.creation import BUDGET_TYPE_CHOICES
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.views import forbidden_for_demo
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import BASE_STATS
from aw_reporting.models import Campaign
from aw_reporting.models import GeoTarget
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import base_stats_aggregator
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.constants import Sections
from segment.models import CustomSegment
from userprofile.models import UserDeviceToken
from utils.permissions import IsAuthQueryTokenPermission
from utils.permissions import MediaBuyingAddOnPermission
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from utils.views import XLSX_CONTENT_TYPE

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
        data = self.serializer_class(queryset[:100], many=True).data
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


DOCUMENT_LOAD_ERROR_TEXT = "Only Microsoft Excel(.xlsx) and CSV(.csv) files are allowed. " \
                           "Also please use the UTF-8 encoding. It is expected that item ID " \
                           "placed in one of first two columns."


class DocumentToChangesApiView(DocumentImportBaseAPIView):
    """
    Send a post request with multipart-ford-data encoded file data
    key: 'file'
    will return
    {"result":[{"name":"94002,California,United States","id":9031903}, ..],
                "undefined":[]}
    """
    parser_classes = (MultiPartParser,)

    def post(self, request, content_type, **_):
        file_obj = request.data['file']
        fct = file_obj.content_type
        if fct == XLSX_CONTENT_TYPE:
            data = self.get_xlsx_contents(file_obj)
        elif fct in ("text/csv", "application/vnd.ms-excel"):
            data = self.get_csv_contents(file_obj)
        else:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data={"errors": [DOCUMENT_LOAD_ERROR_TEXT]})
        if content_type == "postal_codes":
            try:
                response_data = self.get_location_rules(data)
            except UnicodeDecodeError:
                return Response(
                    status=HTTP_400_BAD_REQUEST,
                    data={"errors": [DOCUMENT_LOAD_ERROR_TEXT]},
                )
        else:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data={
                    "errors": ["The content type isn't supported: "
                               "{}".format(content_type)]
                })
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

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                name="query",
                required=True,
                in_=openapi.IN_PATH,
                description="urlencoded search string to lookup Youtube videos",
                type=openapi.TYPE_STRING,
            ),
            schemas.VIDEO_FORMAT_PARAMETER,
        ],
        responses={
            HTTP_200_OK: schemas.VIDEO_RESPONSE_SCHEMA
        }
    )
    def get(self, request, query, **_):
        video_ad_format = request.GET.get("video_ad_format")

        youtube = build(
            "youtube", "v3",
            developerKey=settings.YOUTUBE_API_DEVELOPER_KEY
        )
        next_page = self.request.GET.get("next_page")

        items, next_token, total_result = self.search_yt_videos(youtube, query,
                                                                next_page)

        if video_ad_format == "BUMPER":
            while len(items) < 10 and next_token:
                add_items, next_token, total_result = self.search_yt_videos(
                    youtube, query, next_token)
                items.extend(add_items)

        response = dict(next_page=next_token, items_count=total_result,
                        items=items)
        return Response(data=response)

    def search_yt_videos(self, youtube, query, next_page):

        video_ad_format = self.request.GET.get("video_ad_format")
        options = {
            'q': query,
            'part': 'id',
            'type': "video",
            'videoDuration': "short" if video_ad_format == "BUMPER" else "any",
            'maxResults': 50,
            'safeSearch': 'none',
            'eventType': 'completed',
        }
        if next_page:
            options["pageToken"] = next_page
        search_results = youtube.search().list(**options).execute()
        ids = [i.get("id", {}).get("videoId") for i in
               search_results.get("items", [])]

        results = youtube.videos().list(part="snippet,contentDetails",
                                        id=",".join(ids)).execute()
        items = [self.format_item(i) for i in results.get("items", [])]

        if video_ad_format == "BUMPER":
            items = list(
                filter(lambda i: i["duration"] and i["duration"] <= 6, items))

        return items, search_results.get("nextPageToken"), search_results.get(
            "pageInfo", {}).get("totalResults")

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
            duration=isodate.parse_duration(
                data["contentDetails"]["duration"]).total_seconds(),
        )
        return item


class YoutubeVideoFromUrlApiView(YoutubeVideoSearchApiView):
    url_regex = r"^(?:https?:/{1,2})?(?:w{3}\.)?youtu(?:be)?\.(?:com|be)(?:/watch\?v=|/video/|/)([^\s&/\?]+)(?:.*)$"

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                name="url",
                required=True,
                in_=openapi.IN_PATH,
                description="urlencoded Youtube video URL",
                type=openapi.TYPE_STRING,
            ),
            schemas.VIDEO_FORMAT_PARAMETER,
        ],
        responses={
            HTTP_200_OK: schemas.VIDEO_ITEM_SCHEMA,
            HTTP_400_BAD_REQUEST: openapi.Response("Wrong request parameters"),
            HTTP_404_NOT_FOUND: openapi.Response("Video not found"),
        }
    )
    def get(self, request, url, **_):
        match = re.match(self.url_regex, url)
        if match:
            yt_id = match.group(1)
        else:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="Wrong url format"))

        video_ad_format = request.GET.get("video_ad_format")

        youtube = build(
            "youtube", "v3",
            developerKey=settings.YOUTUBE_API_DEVELOPER_KEY
        )
        options = {
            'id': yt_id,
            'part': 'snippet,contentDetails',
            'maxResults': 1,
        }
        results = youtube.videos().list(**options).execute()
        items = results.get("items", [])
        if not items:
            return Response(status=HTTP_404_NOT_FOUND,
                            data=dict(error="There is no such a video"))

        item = self.format_item(items[0])
        if video_ad_format == "BUMPER" and item["duration"] and item[
            "duration"] > 6:
            return Response(status=HTTP_404_NOT_FOUND,
                            data=dict(
                                error="There is no such a Bumper ads video (<= 6 seconds)"))

        return Response(data=item)


class ItemsFromSegmentIdsApiView(APIView):
    permission_classes = (MediaBuyingAddOnPermission,)

    def post(self, request, segment_type, **_):
        all_related_items = []
        ids = request.data
        for segment_id in ids:
            segment = CustomSegment.objects.get(id=segment_id)
            scan = segment.generate_search_with_params().scan()
            related_ids = [
                {
                    "criteria": item.main.id,
                    "id": item.main.id,
                    "name": item.general_data.title,
                    "thumnail": item.general_data.thumbnail_image_url
                } for item in scan
            ]
            all_related_items.extend(related_ids)
        return Response(status=HTTP_200_OK, data=all_related_items)


class TargetingItemsSearchApiView(APIView):
    permission_classes = (MediaBuyingAddOnPermission,)

    def get(self, request, list_type, query, **_):

        method = "search_{}_items".format(list_type)
        items = getattr(self, method)(query)
        # items = [dict(criteria=uid) for uid in item_ids]
        # add_targeting_list_items_info(items, list_type)

        return Response(data=items)

    @staticmethod
    def search_video_items(query):
        manager = VideoManager(sections=(Sections.GENERAL_DATA,))
        videos = manager.search(
            limit=10,
            query={"match": {"general_data.title": query}},
            sort=[{"stats.views": {"order": "desc"}}]
        ).execute()
        items = [
            dict(id=video.main.id, criteria=video.main.id, name=video.general_data.title,
                 thumbnail=video.general_data.thumbnail_image_url)
            for video in videos
        ]
        return items

    @staticmethod
    def search_channel_items(query):
        manager = ChannelManager(sections=(Sections.GENERAL_DATA,))
        channels = manager.search(
            limit=10,
            query={"match": {f"{Sections.GENERAL_DATA}.title": query.lower()}},
            sort=[{f"{Sections.STATS}.subscribers": {"order": "desc"}}]
        ).execute()
        items = [
            dict(id=channel.main.id, criteria=channel.main.id, name=channel.general_data.title,
                 thumbnail=channel.general_data.thumbnail_image_url)
            for channel in channels
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


class CreationOptionsApiView(APIView):
    @swagger_auto_schema(
        operation_description="Allowed options for account creations",
        responses={
            HTTP_200_OK: CREATION_OPTIONS_SCHEMA,
        },
    )
    def get(self, request, **k):
        def opts_to_response(opts):
            res = [dict(id=i, name=n) for i, n in opts]
            return res

        def list_to_resp(l, n_func=None):
            n_func = n_func or (lambda e: e)
            return [dict(id=i, name=n_func(i)) for i in l]

        def get_week_day_name(n):
            return calendar.day_name[n - 1]

        ad_schedule_rules = list_to_resp(range(1, 8), n_func=get_week_day_name)
        additional_ad_schedule_rules = [
            {'id': 8, 'name': 'All Days'},
            {'id': 9, 'name': 'Weekdays'},
            {'id': 10, 'name': 'Weekends'},
        ]
        ad_schedule_rules.extend(additional_ad_schedule_rules)

        options = OrderedDict(
            # ACCOUNT
            # create
            name="string;max_length=250;required;validation=^[^#']*$",
            # create and update
            video_ad_format=[
                dict(
                    id=i, name=n,
                    thumbnail=request.build_absolute_uri(
                        static("img/{}.png".format(i)))
                )
                for i, n in AdGroupCreation.VIDEO_AD_FORMATS
            ],
            # update
            goal_type=opts_to_response(
                CampaignCreation.GOAL_TYPES[:1],
            ),
            type=opts_to_response(
                CampaignCreation.CAMPAIGN_TYPES,
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
            budget_type=opts_to_response(BUDGET_TYPE_CHOICES),
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
            content_exclusions=opts_to_response(
                CampaignCreation.CONTENT_LABELS),
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
                day=ad_schedule_rules,
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

            # Bidding Strategy
            bidding_strategy_types=opts_to_response(
                CampaignCreation.BID_STRATEGY_TYPES,
            ),
        )
        return Response(data=options)


def is_demo(*args,  **kwargs):
    str_pk = kwargs.get("pk")
    return str_pk.isnumeric() and int(str_pk) == DEMO_ACCOUNT_ID


class CampaignCreationListSetupApiView(ListCreateAPIView):
    serializer_class = CampaignCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = CampaignCreation.objects.filter(
            Q(campaign__account_id=DEMO_ACCOUNT_ID)
            | Q(account_creation__owner=self.request.user)) \
            .filter(
            account_creation_id=pk,
            is_deleted=False,
        )
        return queryset

    @forbidden_for_demo(is_demo)
    def create(self, request, *args, **kwargs):
        try:
            account_creation = AccountCreation.objects.get(
                pk=kwargs.get("pk"),
                owner=request.user,
                is_managed=True,
            )
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        count = CampaignCreation.objects.filter(
            account_creation__owner=self.request.user,
            account_creation_id=kwargs.get("pk"),
        ).count()
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


class CampaignCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = CampaignCreationSetupSerializer
    permission_classes = (or_permission_classes(
        user_has_permission("userprofile.settings_my_aw_accounts"),
        MediaBuyingAddOnPermission),
    )

    @swagger_auto_schema(
        operation_description="Update campaign creation",
        manual_parameters=[
            openapi.Parameter(
                name="id",
                required=True,
                in_=openapi.IN_PATH,
                description="Campaign creation id",
                type=openapi.TYPE_STRING,
            ),
        ],
    )
    def put(self, request, *args, **kwargs):
        return super().put(request, *args, **kwargs)

    def get_queryset(self):
        queryset = CampaignCreation.objects \
            .filter(Q(account_creation__owner=self.request.user) | Q(campaign__account_id=DEMO_ACCOUNT_ID)) \
            .filter(is_deleted=False)
        return queryset

    def delete(self, *args, **_):
        instance = self.get_object()

        campaigns_count = self.get_queryset().filter(
            account_creation=instance.account_creation).count()
        if campaigns_count < 2:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(
                                error="You cannot delete the only campaign"))
        instance.is_deleted = True
        instance.save()
        return Response(status=HTTP_204_NO_CONTENT)

    def update(self, request, *args, **kwargs):
        """
        PUT method handler: Entire update of CampaignCreation
        :param request: request.data -> dict of full CampaignCreation object to PUT
        """

        partial = False
        instance = self.get_object()
        serializer = CampaignCreationUpdateSerializer(
            instance, data=request.data, partial=partial
        )
        serializer.is_valid(raise_exception=True)
        obj = serializer.save()
        self.update_related_models(obj.id, request.data)

        return self.retrieve(self, request, *args, **kwargs)

    def partial_update(self, request, *args, **kwargs):
        """
        PATCH method handler: Partial updating of CampaignCreations
        :param request: request.data -> dict of fields and values to PATCH
        """
        partial = True
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


class AdGroupCreationListSetupApiView(ListCreateAPIView):
    serializer_class = AdGroupCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = AdGroupCreation.objects.filter(
            Q(campaign_creation__account_creation__owner=self.request.user)
            | Q(ad_group__campaign__account_id=DEMO_ACCOUNT_ID)) \
            .filter(
            campaign_creation_id=pk,
            is_deleted=False,
        )
        return queryset

    @forbidden_for_demo(lambda *args, **kwargs: CampaignCreation.objects.filter(pk=kwargs.get("pk"),
                                                                                campaign__account_id=DEMO_ACCOUNT_ID).exists())
    def create(self, request, *args, **kwargs):
        try:
            campaign_creation = CampaignCreation.objects.filter(
                Q(account_creation__owner=request.user)
                | Q(campaign__account_id=DEMO_ACCOUNT_ID)
            ) \
                .get(pk=kwargs.get("pk"))
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


class AdGroupCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = AdGroupCreationSetupSerializer
    permission_classes = (or_permission_classes(
        user_has_permission("userprofile.settings_my_aw_accounts"),
        MediaBuyingAddOnPermission),
    )

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                name="id",
                required=True,
                in_=openapi.IN_PATH,
                description="Ad Group creation id",
                type=openapi.TYPE_STRING,
            ),
        ],
    )
    def put(self, request, *args, **kwargs):
        return super().put(request, *args, **kwargs)

    def get_queryset(self):
        queryset = AdGroupCreation.objects.filter(
            Q(campaign_creation__account_creation__owner=self.request.user)
            | Q(ad_group__campaign__account_id=DEMO_ACCOUNT_ID)) \
            .filter(is_deleted=False)
        return queryset

    @forbidden_for_demo(lambda *args, **kwargs: AdGroupCreation.objects.filter(
        ad_group__campaign__account_id=DEMO_ACCOUNT_ID,
        pk=kwargs.get("pk")).exists())
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
        count = self.get_queryset().filter(
            campaign_creation=instance.campaign_creation).count()
        if count < 2:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="You cannot delete the only item"))
        instance.is_deleted = True
        instance.save()
        return Response(status=HTTP_204_NO_CONTENT)


class AdCreationListSetupApiView(ListCreateAPIView):
    serializer_class = AdCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = AdCreation.objects.filter(
            Q(ad_group_creation__campaign_creation__account_creation__owner=self.request.user)
            | Q(ad__ad_group__campaign__account_id=DEMO_ACCOUNT_ID)
        ) \
            .filter(
            ad_group_creation_id=pk,
            is_deleted=False,
        )
        return queryset

    @forbidden_for_demo(lambda *args, **kwargs: AdGroupCreation.objects.filter(pk=kwargs.get("pk"),
                                                                               ad_group__campaign__account_id=DEMO_ACCOUNT_ID).exists())
    def create(self, request, *args, **kwargs):
        try:
            ad_group_creation = AdGroupCreation.objects.get(
                pk=kwargs.get("pk"),
                campaign_creation__account_creation__owner=request.user
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


class AdCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = AdCreationSetupSerializer
    permission_classes = (or_permission_classes(
        user_has_permission("userprofile.settings_my_aw_accounts"),
        MediaBuyingAddOnPermission),
    )

    @swagger_auto_schema(
        operation_description="Get Ad creation",
        manual_parameters=[
            openapi.Parameter(
                name="id",
                required=True,
                in_=openapi.IN_PATH,
                description="Ad creation id",
                type=openapi.TYPE_STRING,
            ),
        ],
    )
    @forbidden_for_demo(lambda *args, **kwargs: AdCreation.objects.filter(pk=kwargs.get("pk"),
                                                                          ad__ad_group__campaign__account_id=DEMO_ACCOUNT_ID).exists())
    def patch(self, request, *args, **kwargs):
        return super().patch(request, *args, **kwargs)

    def get_queryset(self):
        queryset = AdCreation.objects.filter(
            Q(ad__ad_group__campaign__account_id=DEMO_ACCOUNT_ID)
            | Q(ad_group_creation__campaign_creation__account_creation__owner=self.request.user)) \
            .filter(is_deleted=False, )
        return queryset

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        data = request.data
        serializer = self.get_serializer(instance, data=data, partial=partial)
        serializer.is_valid(raise_exception=True)

        # validate video ad format and video duration
        video_ad_format = data.get(
            "video_ad_format") or instance.ad_group_creation.video_ad_format
        if video_ad_format == AdGroupCreation.BUMPER_AD:
            # data is get from multipart form data, all values are strings
            video_duration = data.get("video_duration")
            video_duration = instance.video_duration if video_duration is None else float(
                video_duration)
            if video_duration > 6:
                return Response(
                    dict(error="Bumper ads video must be 6 seconds or less"),
                    status=HTTP_400_BAD_REQUEST)

        if "video_ad_format" in data:
            set_ad_format = data["video_ad_format"]
            ad_group_creation = instance.ad_group_creation
            campaign_creation = ad_group_creation.campaign_creation
            video_ad_format = ad_group_creation.video_ad_format
            if set_ad_format != video_ad_format:
                # ad group restrictions
                if ad_group_creation.is_pulled_to_aw:
                    return Response(
                        dict(
                            error="{} is the only available ad type for this ad group".format(
                                video_ad_format)),
                        status=HTTP_400_BAD_REQUEST,
                    )

                if ad_group_creation.ad_creations.filter(
                        is_deleted=False).count() > 1:
                    return Response(dict(
                        error="Ad type cannot be changed for only one ad within an ad group"),
                        status=HTTP_400_BAD_REQUEST)

                # Invalid if the campaign bid strategy type is Target CPA and the ad long headline and short headline have not been set
                if campaign_creation.bid_strategy_type == CampaignCreation.TARGET_CPA_STRATEGY and \
                        (data.get('long_headline') is None or data.get('short_headline') is None):
                    return Response(
                        dict(
                            error="You must provide a short headline and long headline.",
                            status=HTTP_400_BAD_REQUEST
                        )
                    )

                # campaign restrictions
                set_bid_strategy = None
                if set_ad_format == AdGroupCreation.BUMPER_AD and \
                        campaign_creation.bid_strategy_type != CampaignCreation.MAX_CPM_STRATEGY:
                    set_bid_strategy = CampaignCreation.MAX_CPM_STRATEGY
                elif set_ad_format in (AdGroupCreation.IN_STREAM_TYPE,
                                       AdGroupCreation.DISCOVERY_TYPE) and \
                        campaign_creation.bid_strategy_type != CampaignCreation.MAX_CPV_STRATEGY:
                    set_bid_strategy = CampaignCreation.MAX_CPV_STRATEGY

                if set_bid_strategy:
                    if campaign_creation.is_pulled_to_aw:
                        return Response(
                            dict(
                                error="You cannot use an ad of {} type in this campaign".format(
                                    set_bid_strategy)),
                            status=HTTP_400_BAD_REQUEST,
                        )

                    if AdCreation.objects.filter(
                            ad_group_creation__campaign_creation=campaign_creation).count() > 1:
                        return Response(dict(
                            error="Ad bid type cannot be changed for only one ad within a campaign"),
                            status=HTTP_400_BAD_REQUEST)

                    CampaignCreation.objects.filter(
                        id=campaign_creation.id).update(
                        bid_strategy_type=set_bid_strategy)
                    campaign_creation.bid_strategy_type = set_bid_strategy

                AdGroupCreation.objects.filter(id=ad_group_creation.id).update(
                    video_ad_format=set_ad_format)
                ad_group_creation.video_ad_format = set_ad_format

        serializer = AdCreationUpdateSerializer(
            instance, data=request.data, partial=partial,
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return self.retrieve(self, request, *args, **kwargs)

    @forbidden_for_demo(lambda *args, **kwargs: AdCreation.objects.filter(pk=kwargs.get("pk"),
                                                                          ad__ad_group__campaign__account_id=DEMO_ACCOUNT_ID).exists())
    def delete(self, *args, **_):
        instance = self.get_object()

        count = self.get_queryset().filter(
            ad_group_creation=instance.ad_group_creation).count()
        if count < 2:
            return Response(dict(error="You cannot delete the only item"),
                            status=HTTP_400_BAD_REQUEST)
        instance.is_deleted = True
        instance.save()
        return Response(status=HTTP_204_NO_CONTENT)


class AdCreationAvailableAdFormatsApiView(APIView):
    permission_classes = (MediaBuyingAddOnPermission,)

    @swagger_auto_schema(
        operation_description="Get Ad group creation",
        manual_parameters=[
            openapi.Parameter(
                name="id",
                required=True,
                in_=openapi.IN_PATH,
                description="Ad Group creation id",
                type=openapi.TYPE_STRING,
            ),
        ],
    )
    def get(self, request, pk, **_):
        try:
            ad_creation = AdCreation.objects.get(pk=pk)
        except AdCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        return Response(
            ad_creation.ad_group_creation.get_available_ad_formats())


class BaseCreationDuplicateApiView(APIView):
    serializer_class = AccountCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    campaign_fields = (
        "name", "start", "end", "budget", "devices_raw", "delivery_method",
        "type", "bid_strategy_type",
        "video_networks_raw", "content_exclusions_raw",
        "target_cpa",
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
        "name", "max_rate", "genders_raw", "parents_raw", "age_ranges_raw",
        "video_ad_format",
    )
    ad_fields = (
        "name", "video_url", "display_url", "final_url", "tracking_template",
        "custom_params", 'companion_banner',
        'video_id', 'video_title', 'video_description', 'video_thumbnail',
        'video_channel_title', 'video_duration',
        "beacon_impression_1", "beacon_impression_2", "beacon_impression_3",
        "beacon_view_1", "beacon_view_2", "beacon_view_3",
        "beacon_skip_1", "beacon_skip_2", "beacon_skip_3",
        "beacon_first_quartile_1", "beacon_first_quartile_2",
        "beacon_first_quartile_3",
        "beacon_midpoint_1", "beacon_midpoint_2", "beacon_midpoint_3",
        "beacon_third_quartile_1", "beacon_third_quartile_2",
        "beacon_third_quartile_3",
        "beacon_completed_1", "beacon_completed_2", "beacon_completed_3",
        "beacon_vast_1", "beacon_vast_2", "beacon_vast_3",
        "beacon_dcm_1", "beacon_dcm_2", "beacon_dcm_3",
        "business_name",
        "long_headline",
        "short_headline",
        "description_1",
        "description_2",
    )
    targeting_fields = ("criteria", "type", "is_negative")

    is_demo = None

    def get_queryset(self):
        raise NotImplementedError

    @forbidden_for_demo(lambda view, request, pk, **kwargs: view.get_queryset().filter(view.is_demo & Q(pk=pk)).exists())
    def post(self, request, pk, **kwargs):
        try:
            instance = self.get_queryset().get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        bulk_items = defaultdict(list)
        with transaction.atomic():
            duplicate = self.duplicate_item(instance, bulk_items,
                                            request.GET.get("to"))
            self.insert_bulk_items(bulk_items)

        response = self.serializer_class(duplicate).data
        return Response(data=response)

    def duplicate_item(self, item, bulk_items, to_parent):
        if isinstance(item, CampaignCreation):
            parent = item.account_creation
            return self.duplicate_campaign(parent, item, bulk_items,
                                           all_names=parent.campaign_creations.values_list(
                                               "name", flat=True))
        elif isinstance(item, AdGroupCreation):
            if to_parent:
                try:
                    parent = CampaignCreation.objects.filter(
                        account_creation__owner=self.request.user).get(
                        pk=to_parent)
                except CampaignCreation.DoesNotExist:
                    raise Http404
            else:
                parent = item.campaign_creation

            return self.duplicate_ad_group(
                parent, item, bulk_items,
                all_names=None if to_parent else parent.ad_group_creations.values_list(
                    "name", flat=True),
            )
        elif isinstance(item, AdCreation):
            if to_parent:
                try:
                    parent = AdGroupCreation.objects.filter(
                        campaign_creation__account_creation__owner=self.request.user
                    ).get(pk=to_parent)
                except AdGroupCreation.DoesNotExist:
                    raise Http404
            else:
                parent = item.ad_group_creation
            return self.duplicate_ad(
                parent, item, bulk_items,
                all_names=None if to_parent else parent.ad_creations.values_list(
                    "name", flat=True),
            )
        else:
            raise NotImplementedError(
                "Unknown item type: {}".format(type(item)))

    def duplicate_campaign(self, account, campaign, bulk_items,
                           all_names=None):
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
                language_through(campaigncreation_id=c_duplicate.id,
                                 language_id=lid)
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

        for a in campaign.ad_group_creations.filter(is_deleted=False):
            self.duplicate_ad_group(c_duplicate, a, bulk_items)

        return c_duplicate

    def duplicate_ad_group(self, campaign, ad_group, bulk_items,
                           all_names=None):
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

        for ad in ad_group.ad_creations.filter(is_deleted=False):
            self.duplicate_ad(a_duplicate, ad, bulk_items)

        return a_duplicate

    def duplicate_ad(self, ad_group, ad, *_, all_names=None):
        tag_field_names = AdCreation.tag_field_names
        data = {}
        for f in self.ad_fields:
            data[f] = getattr(ad, f)
            if f in tag_field_names and getattr(ad, f):
                data["{}_changed".format(f)] = True
        ad_duplicate = AdCreation.objects.create(ad_group_creation=ad_group,
                                                 **data)
        if all_names:
            ad_duplicate.name = self.increment_name(ad_duplicate.name,
                                                    all_names)
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
            CampaignCreation.languages.through.objects.bulk_create(
                bulk_items['languages'])

        if bulk_items['location_rules']:
            LocationRule.objects.bulk_create(bulk_items['location_rules'])

        if bulk_items['frequency_capping']:
            FrequencyCap.objects.bulk_create(bulk_items['frequency_capping'])

        if bulk_items['ad_schedule_rules']:
            AdScheduleRule.objects.bulk_create(bulk_items['ad_schedule_rules'])

        if bulk_items['targeting_items']:
            TargetingItem.objects.bulk_create(bulk_items['targeting_items'])


class CampaignCreationDuplicateApiView(BaseCreationDuplicateApiView):
    serializer_class = CampaignCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)
    is_demo = Q(campaign__account_id=DEMO_ACCOUNT_ID)

    def get_queryset(self):
        queryset = CampaignCreation.objects.filter(
            Q(account_creation__owner=self.request.user)
            | self.is_demo,
        )
        return queryset


class AdGroupCreationDuplicateApiView(BaseCreationDuplicateApiView):
    serializer_class = AdGroupCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)
    is_demo = Q(ad_group__campaign__account_id=DEMO_ACCOUNT_ID)

    def get_queryset(self):
        queryset = AdGroupCreation.objects.filter(
            Q(campaign_creation__account_creation__owner=self.request.user)
            | self.is_demo,
        )
        return queryset


class AdCreationDuplicateApiView(BaseCreationDuplicateApiView):
    serializer_class = AdCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)
    is_demo = Q(ad__ad_group__campaign__account_id=DEMO_ACCOUNT_ID)

    def get_queryset(self):
        queryset = AdCreation.objects.filter(
            Q(ad_group_creation__campaign_creation__account_creation__owner=self.request.user)
            | self.is_demo,
        )
        return queryset


class PerformanceTargetingFiltersAPIView(APIView):
    def get_queryset(self):
        user = self.request.user
        return AccountCreation.objects.user_related(user)

    @staticmethod
    def get_campaigns(item):
        campaign_creation_ids = set(
            item.campaign_creations.filter(
                is_deleted=False
            ).values_list("id", flat=True)
        )

        rows = Campaign.objects.filter(account__account_creation=item).values(
            "name", "id", "ad_groups__name", "ad_groups__id",
            "status", "ad_groups__status", "start_date", "end_date",
        ).order_by("name", "id", "ad_groups__name", "ad_groups__id")
        campaigns = []
        for row in rows:

            campaign_creation_id = None
            cid_search = re.match(r"^.*#(\d+)$", row['name'])
            if cid_search:
                cid = int(cid_search.group(1))
                if cid in campaign_creation_ids:
                    campaign_creation_id = cid

            if not campaigns or row['id'] != campaigns[-1]['id']:
                campaigns.append(
                    dict(
                        id=row['id'],
                        name=row['name'],
                        start_date=row['start_date'],
                        end_date=row['end_date'],
                        status=row['status'],
                        ad_groups=[],
                        campaign_creation_id=campaign_creation_id,
                    )
                )
            if row['ad_groups__id'] is not None:
                campaigns[-1]['ad_groups'].append(
                    dict(
                        id=row['ad_groups__id'],
                        name=row['ad_groups__name'],
                        status=row['ad_groups__status'],
                    )
                )
        return campaigns

    @staticmethod
    def get_static_filters():
        filters = dict(
            targeting=[
                dict(id=t, name="{}s".format(t.capitalize()))
                for t in ("channel", "video", "keyword", "topic", "interest")
            ],
            group_by=[
                dict(id="account", name="All Campaigns"),
                dict(id="campaign", name="Individual Campaigns"),
            ],
        )
        return filters

    def get(self, request, pk, **_):
        try:
            item = self.get_queryset().get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        dates = AdGroupStatistic.objects.filter(
            ad_group__campaign__account=item.account).aggregate(
            min_date=Min("date"), max_date=Max("date"),
        )
        filters = self.get_static_filters()
        filters["start_date"] = dates["min_date"]
        filters["end_date"] = dates["max_date"]
        filters["campaigns"] = self.get_campaigns(item)
        return Response(data=filters)


class PerformanceTargetingReportAPIView(APIView):
    channel_manager = ChannelManager()
    es_fields_to_load_channel_info = ("main.id", "general_data.title", "general_data.thumbnail_image_url",)

    video_manager = VideoManager()
    es_fields_to_load_video_info = ("main.id", "general_data.title", "general_data.thumbnail_image_url",)

    def get_object(self):
        pk = self.kwargs["pk"]
        user = self.request.user
        try:
            item = AccountCreation.objects.user_related(user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404
        else:
            return item

    def get_items(self, targeting, account):
        items = []
        for target in targeting:
            method = "get_%s_items" % target
            if hasattr(self, method):
                items.extend(getattr(self, method)(account))
        return items

    def get_negative_targeting_items(self, targeting, account_creation):
        queryset = TargetingItem.objects.filter(
            ad_group_creation__campaign_creation__account_creation=account_creation,
            ad_group_creation__ad_group__isnull=False,
            type__in=targeting,
            is_negative=True,
        )
        data = self.request.data
        if data.get("ad_groups"):
            queryset = queryset.filter(
                ad_group_creation__ad_group_id__in=data["ad_groups"])
        if data.get("campaigns"):
            queryset = queryset.filter(
                ad_group_creation__ad_group__campaign_id__in=data["campaigns"])

        items = defaultdict(lambda: defaultdict(set))
        for e in queryset.values("type", "criteria",
                                 "ad_group_creation__ad_group_id"):
            items[
                e["type"]
            ][
                e["ad_group_creation__ad_group_id"]
            ].add(
                e["criteria"]
            )
        return items

    def post(self, request, **_):
        item = self.get_object()
        group_by, targeting = request.data.get("group_by",
                                               ""), request.data.get(
            "targeting", [])
        items = self.get_items(targeting, item.account)
        negative_items = self.get_negative_targeting_items(targeting, item)

        reports = []
        if group_by == "campaign":
            items_by_campaign = defaultdict(list)
            for i in items:
                uid = (i["campaign"]["name"], i["campaign"]["id"])
                items_by_campaign[uid].append(i)
            items_by_campaign = [dict(label=k[0], id=k[1], items=v) for k, v in
                                 items_by_campaign.items()]
            reports.extend(
                sorted(items_by_campaign, key=lambda el: el["label"]))
        else:
            reports.append(dict(label="All campaigns", items=items, id=None))
        for report in reports:
            # get calculated fields
            stat_fields = BASE_STATS + ("video_impressions",)
            summary = {k: 0 for k in stat_fields}
            for i in report['items']:
                dict_norm_base_stats(i)
                for k, v in i.items():
                    if k in stat_fields and v:
                        summary[k] += v
                dict_add_calculated_stats(i)
                del i['video_impressions']

                # add status field
                targeting_type = i["targeting"].lower()[:-1]
                ad_group_id = i["ad_group"]["id"]
                i["is_negative"] = str(i["item"]["id"]) in \
                                   negative_items[targeting_type][ad_group_id]

            dict_add_calculated_stats(summary)
            del summary['video_impressions']
            report.update(summary)

            report["kpi"] = self.get_kpi_limits(report['items'])

        data = dict(
            reports=reports,
        )
        return Response(data=data)

    @staticmethod
    def get_kpi_limits(items):
        kpi = dict(
            average_cpv=[],
            average_cpm=[],
            ctr=[],
            ctr_v=[],
            video_view_rate=[],
        )
        for item in items:
            for key, values in kpi.items():
                value = item[key]
                if value is not None:
                    values.append(value)

        kpi_limits = dict()
        for key, values in kpi.items():
            kpi_limits[key] = dict(min=min(values) if values else None,
                                   max=max(values) if values else None)
        return kpi_limits

    def filter_queryset(self, qs):
        data = self.request.data
        if data.get("ad_groups"):
            qs = qs.filter(ad_group_id__in=data["ad_groups"])
        if data.get("campaigns"):
            qs = qs.filter(ad_group__campaign_id__in=data["campaigns"])
        if data.get("start_date"):
            qs = qs.filter(date__gte=data["start_date"])
        if data.get("end_date"):
            qs = qs.filter(date__lte=data["end_date"])
        return qs

    _annotate = base_stats_aggregator("ad_group__")
    _values = ("ad_group__id", "ad_group__name", "ad_group__campaign__id",
               "ad_group__campaign__name",
               "ad_group__campaign__status")

    @staticmethod
    def _set_group_and_campaign_fields(el):
        el["ad_group"] = dict(id=el['ad_group__id'], name=el['ad_group__name'])
        el["campaign"] = dict(id=el['ad_group__campaign__id'],
                              name=el['ad_group__campaign__name'],
                              status=el["ad_group__campaign__status"])
        del el['ad_group__id'], el['ad_group__name'], el[
            'ad_group__campaign__id']
        del el['ad_group__campaign__name'], el['ad_group__campaign__status']

    def get_channel_items(self, account):
        qs = YTChannelStatistic.objects.filter(
            ad_group__campaign__account=account
        )
        qs = self.filter_queryset(qs)
        items = qs.values("yt_id", *self._values).order_by("yt_id",
                                                           "ad_group_id").annotate(
            **self._annotate)

        info = {}
        ids = {i['yt_id'] for i in items}
        if ids:
            try:
                items = self.channel_manager.search(
                    filters=self.channel_manager.ids_query(ids)
                ). \
                    source(includes=list(self.es_fields_to_load_channel_info)).execute().hits
                info = {r.main.id: r for r in items}
            except Exception as e:
                logger.error(e)

        for i in items:
            item_details = info.get(i['yt_id'])
            i["item"] = dict(id=i['yt_id'],
                             name=item_details.general_data.title if item_details else i['yt_id'],
                             thumbnail=item_details.general_data.thumbnail_image_url if item_details else i['yt_id'])
            del i['yt_id']

            self._set_group_and_campaign_fields(i)
            i["targeting"] = "Channels"
        return items

    def get_video_items(self, account):
        qs = YTVideoStatistic.objects.filter(
            ad_group__campaign__account=account)
        qs = self.filter_queryset(qs)
        items = qs.values("yt_id", *self._values).order_by("yt_id",
                                                           "ad_group_id").annotate(
            **self._annotate)

        info = {}
        ids = {i['yt_id'] for i in items}
        if ids:
            try:
                items = self.video_manager.search(
                    filters=self.video_manager.ids_query(ids)
                ). \
                    source(includes=list(self.es_fields_to_load_video_info)).execute().hits
                info = {r.main.id: r for r in items}
            except Exception as e:
                logger.error(e)

        for i in items:
            item_details = info.get(i['yt_id'])
            i["item"] = dict(id=i['yt_id'],
                             name=item_details.general_data.title if item_details else i['yt_id'],
                             thumbnail=item_details.general_data.thumbnail_image_url if item_details else None)
            del i['yt_id']

            self._set_group_and_campaign_fields(i)
            i["targeting"] = "Videos"
        return items

    def get_keyword_items(self, account):
        qs = KeywordStatistic.objects.filter(
            ad_group__campaign__account=account)
        qs = self.filter_queryset(qs)
        items = qs.values("keyword", *self._values).order_by("keyword",
                                                             "ad_group_id").annotate(
            **self._annotate)

        for i in items:
            i["item"] = dict(id=i['keyword'], name=i['keyword'])
            del i['keyword']
            self._set_group_and_campaign_fields(i)
            i["targeting"] = "Keywords"
        return items

    def get_interest_items(self, account):
        qs = AudienceStatistic.objects.filter(
            ad_group__campaign__account=account)
        qs = self.filter_queryset(qs)
        items = qs.values("audience__id", "audience__name",
                          *self._values).order_by(
            "audience__id", "ad_group_id").annotate(**self._annotate)

        for i in items:
            i["item"] = dict(id=i['audience__id'], name=i['audience__name'])
            del i['audience__id'], i['audience__name']
            self._set_group_and_campaign_fields(i)
            i["targeting"] = "Interests"
        return items

    def get_topic_items(self, account):
        qs = TopicStatistic.objects.filter(ad_group__campaign__account=account)
        qs = self.filter_queryset(qs)
        items = qs.values("topic__id", "topic__name", *self._values).order_by(
            "topic__id", "ad_group_id").annotate(**self._annotate)

        for i in items:
            i["item"] = dict(id=i['topic__id'], name=i['topic__name'])
            del i['topic__id'], i['topic__name']
            self._set_group_and_campaign_fields(i)
            i["targeting"] = "Topics"
        return items


class PerformanceTargetingItemAPIView(UpdateAPIView):
    serializer_class = UpdateTargetingDirectionSerializer

    @forbidden_for_demo(lambda *args, **kwargs: AdGroup.objects.filter(pk=kwargs["ad_group_id"], campaign__account_id=DEMO_ACCOUNT_ID).exists())
    def update(self, request, *args, **kwargs):
        return super(PerformanceTargetingItemAPIView, self).update(request, *args, **kwargs)

    def get_object(self):
        targeting_type = self.kwargs["targeting"].lower()
        if targeting_type.endswith("s"):
            targeting_type = targeting_type[:-1]

        ad_group_creation = get_object_or_404(
            AdGroupCreation,
            campaign_creation__account_creation__owner=self.request.user,
            ad_group_id=self.kwargs["ad_group_id"],
        )
        obj = get_object_or_404(
            TargetingItem, criteria=self.kwargs["criteria"],
            ad_group_creation=ad_group_creation, type=targeting_type,
        )
        return obj


class TopicToolListApiView(ListAPIView):
    serializer_class = TopicHierarchySerializer

    def get_queryset(self):
        queryset = Topic.objects.filter(parent__isnull=True).order_by('name')
        if 'ids' in self.request.query_params:
            queryset = Topic.objects.all()
            queryset = queryset.filter(
                id__in=self.request.query_params['ids'].split(','))
        return queryset


class TopicToolFlatListApiView(ListAPIView):
    serializer_class = TopicHierarchySerializer

    def get_queryset(self):
        queryset = Topic.objects.all()
        if 'title' in self.request.query_params:
            titles = self.request.query_params.getlist("title")
            queryset = queryset.filter(
                name__in=titles)
        return queryset


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


class AudienceFlatListApiView(ListAPIView):
    serializer_class = AudienceHierarchySerializer

    def get_queryset(self):
        queryset = Audience.objects.all()
        if "title" in self.request.query_params:
            titles = self.request.query_params.getlist("title")
            queryset = queryset.filter(name__in=titles)
        return queryset


class AudienceToolListExportApiView(TopicToolListExportApiView):
    permission_classes = (IsAuthQueryTokenPermission,)
    export_fields = ('id', 'name', 'parent_id', 'type')
    file_name = "audience_list"

    def get_queryset(self):
        return AudienceToolListApiView.queryset.all()


# targeting lists
class TargetingListBaseAPIClass(GenericAPIView):
    serializer_class = AdGroupTargetingListSerializer

    def get_user(self):
        return self.request.user

    def get_queryset(self):
        pk = self.kwargs.get('pk')
        list_type = self.kwargs.get('list_type')
        queryset = TargetingItem.objects.filter(
            Q(ad_group_creation__campaign_creation__account_creation__owner=self.get_user())
            | Q(ad_group_creation__ad_group__campaign__account_id=DEMO_ACCOUNT_ID)) \
            .filter(
            ad_group_creation_id=pk,
            type=list_type,

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


class AdGroupCreationTargetingExportApiView(TargetingListBaseAPIClass):
    permission_classes = (IsAuthQueryTokenPermission,)

    def get_user(self):
        auth_token = self.request.query_params.get("auth_token")
        token = UserDeviceToken.objects.get(key=auth_token)
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
    parser_classes = (MultiPartParser,)
    permission_classes = (MediaBuyingAddOnPermission,)

    def post(self, request, list_type, **_):

        method = "import_{}_criteria".format(list_type)
        if not hasattr(self, method):
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Unsupported list type: {}".format(list_type))

        criteria_list = []
        for _, file_obj in request.data.items():
            if not hasattr(file_obj, 'content_type'):
                # skip empty items
                continue

            fct = file_obj.content_type
            try:
                if fct == XLSX_CONTENT_TYPE:
                    data = self.get_xlsx_contents(file_obj, return_lines=True)
                elif fct in ("text/csv", "application/vnd.ms-excel"):
                    data = self.get_csv_contents(file_obj, return_lines=True)
                else:
                    return Response(status=HTTP_400_BAD_REQUEST,
                                    data={
                                        "errors": [DOCUMENT_LOAD_ERROR_TEXT]
                                    })
            except Exception as e:
                return Response(status=HTTP_400_BAD_REQUEST,
                                data={
                                    "errors": [DOCUMENT_LOAD_ERROR_TEXT,
                                               'Stage: Load File Data. Cause: {}'.format(
                                                   e)]
                                })

            try:
                criteria_list.extend(getattr(self, method)(data))
            except Exception as e:
                return Response(
                    status=HTTP_400_BAD_REQUEST,
                    data={
                        "errors": [DOCUMENT_LOAD_ERROR_TEXT,
                                   'Stage: Data Extraction. Cause: {}'.format(
                                       e)]
                    },
                )

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

        with open('aw_creation/scripts/aws_functions.js') as f:
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
        existed_c_ids = set(
            Campaign.objects.filter(account_id=account_id).values_list("id",
                                                                       flat=True))
        existed_a_ids = set(AdGroup.objects.filter(
            campaign__account_id=account_id).values_list("id", flat=True))
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
        for account_creation in AccountCreation.objects.filter(
                account_id=account_id, is_managed=True):
            campaign_creations = account_creation.campaign_creations.all().values(
                "id", "campaign_id")
            matched_campaign_ids = set(
                c['campaign_id'] for c in campaign_creations if
                c['campaign_id'])
            for campaign_creation in filter(lambda c: not c["campaign_id"],
                                            campaign_creations):
                # match campaign creations
                uid_key = "#{}".format(campaign_creation['id'])
                for campaign in filter(
                        lambda c: c['id'] not in matched_campaign_ids,
                        campaigns):
                    if campaign['name'].endswith(uid_key):
                        CampaignCreation.objects.filter(
                            id=campaign_creation['id']
                        ).update(campaign_id=campaign['id'])
                        break

            ad_group_creations = AdGroupCreation.objects.filter(
                campaign_creation__account_creation=account_creation
            ).values("id", "ad_group_id", "campaign_creation__campaign_id")
            matched_ad_group_ids = set(
                c['ad_group_id'] for c in ad_group_creations if
                c['ad_group_id'])
            for ad_group_creation in filter(lambda a: not a['ad_group_id'],
                                            ad_group_creations):
                uid_key = "#{}".format(ad_group_creation['id'])
                for campaign in filter(lambda c: ad_group_creation[
                                                     'campaign_creation__campaign_id'] ==
                                                 c['id'],
                                       campaigns):
                    for ad_group in filter(
                            lambda a: a['id'] not in matched_ad_group_ids,
                            campaign['ad_groups']):
                        if ad_group['name'].endswith(uid_key):
                            AdGroupCreation.objects.filter(
                                id=ad_group_creation['id']
                            ).update(ad_group_id=ad_group['id'])
                            break
        return Response('Successfully updated Campaign: {}'.format(str(account_id)))
