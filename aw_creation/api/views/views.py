# pylint: disable=import-error
import calendar
import csv
import itertools
import re
from collections import OrderedDict, defaultdict
from datetime import datetime
from decimal import Decimal
from io import StringIO

import isodate
from apiclient.discovery import build
from django.conf import settings
# pylint: enable=import-error
from django.contrib.staticfiles.templatetags.staticfiles import static
from django.db import transaction
from django.db.models import Value, Case, When, F, \
    IntegerField as AggrIntegerField, Min, Max
from django.http import StreamingHttpResponse, HttpResponse, Http404
from django.shortcuts import get_object_or_404
from openpyxl import load_workbook
from rest_framework.authtoken.models import Token
from rest_framework.generics import ListAPIView, RetrieveUpdateAPIView, \
    GenericAPIView, ListCreateAPIView, RetrieveAPIView, UpdateAPIView
from rest_framework.parsers import FileUploadParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_200_OK, \
    HTTP_404_NOT_FOUND, HTTP_201_CREATED, HTTP_204_NO_CONTENT
from rest_framework.views import APIView

from aw_creation.api.serializers import *
from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation, FrequencyCap, Language, LocationRule, AdScheduleRule, \
    TargetingItem, default_languages
from aw_reporting.api.serializers.campaign_list_serializer import \
    CampaignListSerializer
from aw_reporting.charts import DeliveryChart, Indicator
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.excel_reports import AnalyzeWeeklyReport
from aw_reporting.models import dict_quartiles_to_rates, all_stats_aggregate, \
    BASE_STATS, GeoTarget, Topic, Audience, AdGroup, YTChannelStatistic, \
    YTVideoStatistic, KeywordStatistic, AudienceStatistic, TopicStatistic, \
    DATE_FORMAT, base_stats_aggregator, campaign_type_str, Campaign, \
    AdGroupStatistic, dict_norm_base_stats, dict_add_calculated_stats
from userprofile.models import UserSettingsKey
from utils.permissions import IsAuthQueryTokenPermission, \
    MediaBuyingAddOnPermission, user_has_permission, or_permission_classes, \
    UserHasCHFPermission
from utils.registry import registry

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

    def post(self, request, content_type, **_):
        file_obj = request.data['file']
        fct = file_obj.content_type
        if fct == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
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
        ).values_list("related_id", flat=True).order_by(
            "related_id").distinct()
        return ids

    @staticmethod
    def get_channel_item_ids(ids):
        from segment.models import SegmentRelatedChannel
        ids = SegmentRelatedChannel.objects.filter(
            segment_id__in=ids
        ).values_list("related_id", flat=True).order_by(
            "related_id").distinct()
        return ids

    @staticmethod
    def get_keyword_item_ids(ids):
        from segment.models import SegmentRelatedKeyword
        ids = SegmentRelatedKeyword.objects.filter(
            segment_id__in=ids
        ).values_list("related_id", flat=True).order_by(
            "related_id").distinct()
        return ids


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
        words = [s.lower() for s in re.split(r'\s+', query)]
        fields = ("video_id", "title", "thumbnail_image_url")
        query_params = dict(fields=",".join(fields), text_search__term=words,
                            sort="views:desc")
        connector = SingleDatabaseApiConnector()
        try:
            response_data = connector.get_video_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            logger.error(e)
            items = []
        else:
            items = [
                dict(id=i['video_id'], criteria=i['video_id'], name=i['title'],
                     thumbnail=i['thumbnail_image_url'])
                for i in response_data["items"]
            ]
        return items

    @staticmethod
    def search_channel_items(query):
        words = [s.lower() for s in re.split(r'\s+', query)]
        fields = ("channel_id", "title", "thumbnail_image_url")
        query_params = dict(fields=",".join(fields), text_search__term=words,
                            sort="subscribers:desc")
        connector = SingleDatabaseApiConnector()
        try:
            response_data = connector.get_channel_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            logger.error(e)
            items = []
        else:
            items = [
                dict(id=i['channel_id'], criteria=i['channel_id'],
                     name=i['title'], thumbnail=i['thumbnail_image_url'])
                for i in response_data["items"]
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
class CampaignCreationListSetupApiView(ListCreateAPIView):
    serializer_class = CampaignCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = CampaignCreation.objects.filter(
            account_creation__owner=self.request.user,
            account_creation_id=pk,
            is_deleted=False,
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


@demo_view_decorator
class CampaignCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = CampaignCreationSetupSerializer
    permission_classes = (or_permission_classes(
        user_has_permission("userprofile.settings_my_aw_accounts"),
        MediaBuyingAddOnPermission),
    )

    def get_queryset(self):
        queryset = CampaignCreation.objects.filter(
            account_creation__owner=self.request.user,
            is_deleted=False,
        )
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
    permission_classes = (MediaBuyingAddOnPermission,)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=self.request.user,
            campaign_creation_id=pk,
            is_deleted=False,
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
    permission_classes = (or_permission_classes(
        user_has_permission("userprofile.settings_my_aw_accounts"),
        MediaBuyingAddOnPermission),
    )

    def get_queryset(self):
        queryset = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=self.request.user,
            is_deleted=False,
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
        count = self.get_queryset().filter(
            campaign_creation=instance.campaign_creation).count()
        if count < 2:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="You cannot delete the only item"))
        instance.is_deleted = True
        instance.save()
        return Response(status=HTTP_204_NO_CONTENT)


@demo_view_decorator
class AdCreationListSetupApiView(ListCreateAPIView):
    serializer_class = AdCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = AdCreation.objects.filter(
            ad_group_creation__campaign_creation__account_creation__owner=self.request.user,
            ad_group_creation_id=pk,
            is_deleted=False,
        )
        return queryset

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


@demo_view_decorator
class AdCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = AdCreationSetupSerializer
    permission_classes = (or_permission_classes(
        user_has_permission("userprofile.settings_my_aw_accounts"),
        MediaBuyingAddOnPermission),
    )

    def get_queryset(self):
        queryset = AdCreation.objects.filter(
            ad_group_creation__campaign_creation__account_creation__owner=self.request.user,
            is_deleted=False,
        )
        return queryset

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        data = request.data

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

                # campaign restrictions
                set_bid_strategy = None
                if set_ad_format == AdGroupCreation.BUMPER_AD and \
                        campaign_creation.bid_strategy_type != CampaignCreation.CPM_STRATEGY:
                    set_bid_strategy = CampaignCreation.CPM_STRATEGY
                elif set_ad_format in (AdGroupCreation.IN_STREAM_TYPE,
                                       AdGroupCreation.DISCOVERY_TYPE) and \
                        campaign_creation.bid_strategy_type != CampaignCreation.CPV_STRATEGY:
                    set_bid_strategy = CampaignCreation.CPV_STRATEGY

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


@demo_view_decorator
class AdCreationAvailableAdFormatsApiView(APIView):
    permission_classes = (MediaBuyingAddOnPermission,)

    def get(self, request, pk, **_):
        try:
            ad_creation = AdCreation.objects.get(pk=pk)
        except AdCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        return Response(
            ad_creation.ad_group_creation.get_available_ad_formats())


@demo_view_decorator
class AccountCreationDuplicateApiView(APIView):
    serializer_class = AccountCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    account_fields = ("is_paused", "is_ended")
    campaign_fields = (
        "name", "start", "end", "budget", "devices_raw", "delivery_method",
        "type", "bid_strategy_type",
        "video_networks_raw", "content_exclusions_raw",
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
    )
    targeting_fields = ("criteria", "type", "is_negative")

    def get_queryset(self):
        queryset = AccountCreation.objects.filter(
            owner=self.request.user,
            is_managed=True,
        )
        return queryset

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
        if isinstance(item, AccountCreation):
            return self.duplicate_account(item, bulk_items,
                                          all_names=self.get_queryset().values_list(
                                              "name", flat=True))
        elif isinstance(item, CampaignCreation):
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

    def duplicate_account(self, account, bulk_items, all_names):
        account_data = dict(
            name=self.increment_name(account.name, all_names),
            owner=self.request.user,
        )
        for f in self.account_fields:
            account_data[f] = getattr(account, f)
        acc_duplicate = AccountCreation.objects.create(**account_data)

        for c in account.campaign_creations.filter(is_deleted=False):
            self.duplicate_campaign(acc_duplicate, c, bulk_items)

        return acc_duplicate

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


@demo_view_decorator
class CampaignCreationDuplicateApiView(AccountCreationDuplicateApiView):
    serializer_class = CampaignCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    def get_queryset(self):
        queryset = CampaignCreation.objects.filter(
            account_creation__owner=self.request.user,
        )
        return queryset


@demo_view_decorator
class AdGroupCreationDuplicateApiView(AccountCreationDuplicateApiView):
    serializer_class = AdGroupCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    def get_queryset(self):
        queryset = AdGroupCreation.objects.filter(
            campaign_creation__account_creation__owner=self.request.user,
        )
        return queryset


@demo_view_decorator
class AdCreationDuplicateApiView(AccountCreationDuplicateApiView):
    serializer_class = AdCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    def get_queryset(self):
        queryset = AdCreation.objects.filter(
            ad_group_creation__campaign_creation__account_creation__owner=self.request.user,
        )
        return queryset


# <<< Performance
@demo_view_decorator
class PerformanceAccountCampaignsListApiView(APIView):
    permission_classes = (IsAuthenticated, UserHasCHFPermission)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        user = registry.user
        account_id = AccountCreation.objects.get(id=pk).account_id
        types_hidden = user.aw_settings.get(
            UserSettingsKey.HIDDEN_CAMPAIGN_TYPES, {}).get(account_id, [])
        types_to_exclude = [campaign_type_str(t) for t in types_hidden]
        queryset = Campaign.objects \
            .filter(
            account__account_creations__id=pk,
            account__account_creations__owner=self.request.user) \
            .exclude(type__in=types_to_exclude) \
            .order_by("name", "id").distinct()
        return queryset

    def get(self, request, pk, **kwargs):
        filters = {"is_deleted": False}
        if request.query_params.get("is_chf") == "1":
            filters["account__id__in"] = []
            user_settings = self.request.user.aw_settings
            if user_settings.get(UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY):
                filters["account__id__in"] = \
                    user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
        else:
            filters["owner"] = self.request.user
        try:
            account_creation = AccountCreation.objects.filter(
                **filters).get(pk=pk)
        except AccountCreation.DoesNotExist:
            campaign_creation_ids = set()
        else:
            campaign_creation_ids = set(
                account_creation.campaign_creations.filter(
                    is_deleted=False
                ).values_list("id", flat=True))
        queryset = self.get_queryset()
        serializer = CampaignListSerializer(
            queryset, many=True, campaign_creation_ids=campaign_creation_ids)
        return Response(serializer.data)


@demo_view_decorator
class PerformanceChartApiView(APIView):
    """
    Send filters to get data for charts

    Body example:

    {"indicator": "impressions", "dimension": "device"}
    """
    permission_classes = (IsAuthenticated, UserHasCHFPermission)

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
            dimension=data.get("dimension"))
        return filters

    def post(self, request, pk, **_):
        self.filter_hidden_sections()
        filters = {}
        if request.data.get("is_chf") == 1:
            user_settings = self.request.user.aw_settings
            filters["account__id__in"] = \
                user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
        else:
            filters["owner"] = self.request.user
        try:
            item = AccountCreation.objects.filter(**filters).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        filters = self.get_filters()
        account_ids = []
        if item.account:
            account_ids.append(item.account.id)
        chart = DeliveryChart(account_ids, segmented_by="campaigns", **filters)
        chart_data = chart.get_response()
        return Response(data=chart_data)

    def filter_hidden_sections(self):
        user = registry.user
        if user.aw_settings.get(UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN):
            hidden_indicators = Indicator.CPV, Indicator.CPM, Indicator.COSTS
            if self.request.data.get("indicator") in hidden_indicators:
                raise Http404


@demo_view_decorator
class PerformanceChartItemsApiView(APIView):
    """
    Send filters to get a list of targeted items

    Body example:

    {"segmented": false}
    """
    permission_classes = (IsAuthenticated, UserHasCHFPermission)

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
            segmented_by=data.get("segmented"))
        return filters

    def post(self, request, pk, **kwargs):
        dimension = kwargs.get('dimension')
        filters = {}
        if request.data.get("is_chf") == 1:
            user_settings = self.request.user.aw_settings
            filters["account__id__in"] = \
                user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
        else:
            filters["owner"] = self.request.user
        try:
            item = AccountCreation.objects.filter(**filters).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        filters = self.get_filters()
        accounts = []
        if item.account:
            accounts.append(item.account.id)
        chart = DeliveryChart(
            accounts=accounts,
            dimension=dimension,
            **filters)
        data = chart.get_items()
        data = self._filter_costs(data)
        return Response(data=data)

    def _filter_costs(self, data):
        user = registry.user
        if user.aw_settings.get(UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN):
            for item in data["items"]:
                item["average_cpm"] = item["average_cpv"] = item["cost"] = None

        return data


@demo_view_decorator
class PerformanceExportApiView(APIView):
    """
    Send filters to download a csv report

    Body example:

    {"campaigns": ["1", "2"]}
    """
    permission_classes = (IsAuthenticated, UserHasCHFPermission)

    def post(self, request, pk, **_):
        filters = {}
        if request.data.get("is_chf") == 1:
            user_settings = self.request.user.aw_settings
            visible_accounts = user_settings.get(
                UserSettingsKey.VISIBLE_ACCOUNTS)
            if visible_accounts:
                filters["account__id__in"] = visible_accounts
        else:
            filters["owner"] = self.request.user
        try:
            item = AccountCreation.objects.filter(**filters).get(pk=pk)
        except AccountCreation.DoesNotExist:
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
        dict_add_calculated_stats(stats)
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
                yield [dimension.capitalize()] + [data[n] for n in
                                                  self.column_keys]


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
            item = AccountCreation.objects.filter(owner=request.user).get(
                pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        report = AnalyzeWeeklyReport(item.account, **filters)

        response = HttpResponse(
            report.get_content(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response[
            'Content-Disposition'] = 'attachment; filename="Channel Factory {} Weekly ' \
                                     'Report {}.xlsx"'.format(
            item.name,
            datetime.now().date().strftime("%m.%d.%y")
        )
        return response


@demo_view_decorator
class PerformanceTargetingDetailsAPIView(RetrieveAPIView):
    serializer_class = AccountCreationListSerializer

    def get_queryset(self):
        queryset = AccountCreation.objects.filter(
            is_deleted=False,
            owner=self.request.user,
        )
        return queryset


@demo_view_decorator
class PerformanceTargetingFiltersAPIView(APIView):
    def get_queryset(self):
        return AccountCreation.objects.filter(owner=self.request.user)

    @staticmethod
    def get_campaigns(item):
        campaign_creation_ids = set(
            item.campaign_creations.filter(
                is_deleted=False
            ).values_list("id", flat=True)
        )

        rows = Campaign.objects.filter(account__account_creations=item).values(
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


@demo_view_decorator
class PerformanceTargetingReportAPIView(APIView):
    def get_object(self):
        pk = self.kwargs["pk"]
        try:
            item = AccountCreation.objects.filter(owner=self.request.user).get(
                pk=pk)
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
            connector = SingleDatabaseApiConnector()
            try:
                resp = connector.get_channels_base_info(ids)
                info = {r['id']: r for r in resp}
            except SingleDatabaseApiConnectorException as e:
                logger.error(e)

        for i in items:
            item_details = info.get(i['yt_id'], {})
            i["item"] = dict(id=i['yt_id'],
                             name=item_details.get("title", i['yt_id']),
                             thumbnail=item_details.get("thumbnail_image_url"))
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
            connector = SingleDatabaseApiConnector()
            try:
                resp = connector.get_videos_base_info(ids)
                info = {r['id']: r for r in resp}
            except SingleDatabaseApiConnectorException as e:
                logger.error(e)

        for i in items:
            item_details = info.get(i['yt_id'], {})
            i["item"] = dict(id=i['yt_id'],
                             name=item_details.get("title", i['yt_id']),
                             thumbnail=item_details.get("thumbnail_image_url"))
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


@demo_view_decorator
class PerformanceTargetingItemAPIView(UpdateAPIView):
    serializer_class = UpdateTargetingDirectionSerializer

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


class UserListsImportMixin:
    @staticmethod
    def get_lists_items_ids(ids, list_type):
        from segment.utils import get_segment_model_by_type
        from keyword_tool.models import KeywordsList

        if list_type == "keyword":
            item_ids = KeywordsList.objects.filter(
                id__in=ids, keywords__text__isnull=False
            ).values_list(
                "keywords__text", flat=True
            ).order_by("keywords__text").distinct()
        else:
            manager = get_segment_model_by_type(list_type).objects
            item_ids = manager.filter(id__in=ids,
                                      related__related_id__isnull=False) \
                .values_list('related__related_id', flat=True) \
                .order_by('related__related_id') \
                .distinct()
        return item_ids


# tools


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
                if fct == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                    data = self.get_xlsx_contents(file_obj, return_lines=True)
                elif fct in ("text/csv", "application/vnd.ms-excel"):
                    data = self.get_csv_contents(file_obj, return_lines=True)
                else:
                    return Response(status=HTTP_400_BAD_REQUEST,
                                    data={
                                        "errors": [DOCUMENT_LOAD_ERROR_TEXT]})
            except Exception as e:
                return Response(status=HTTP_400_BAD_REQUEST,
                                data={"errors": [DOCUMENT_LOAD_ERROR_TEXT,
                                                 'Stage: Load File Data. Cause: {}'.format(
                                                     e)]})

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
        return Response()
