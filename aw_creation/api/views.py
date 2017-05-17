import csv
import logging
import re
from collections import OrderedDict

from apiclient.discovery import build
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from openpyxl import load_workbook
from rest_framework.generics import ListAPIView, RetrieveUpdateAPIView, GenericAPIView
from rest_framework.pagination import PageNumberPagination
from rest_framework.parsers import FileUploadParser
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, \
    HTTP_200_OK, HTTP_202_ACCEPTED
from rest_framework.views import APIView

from aw_creation.api.serializers import *
from aw_reporting.models import GeoTarget

logger = logging.getLogger(__name__)


class GeoTargetListApiView(APIView):

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
        print(results)
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
        queryset = AccountCreation.objects.filter(
            owner=self.request.user, **filters
        ).order_by('is_ended', '-created_at')
        return queryset

    def filter_queryset(self, queryset):
        now = timezone.now()
        if not self.request.query_params.get('show_closed', False):
            ids = list(
                queryset.filter(
                    Q(campaign_creations__end__isnull=True) |
                    Q(campaign_creations__end__gte=now)
                ).values_list('id', flat=True).distinct()
            )
            queryset = queryset.filter(id__in=ids)
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

    def post(self, request, *args, **kwargs):
        owner = self.request.user
        data = request.data

        number = AccountCreation.objects.filter(owner=owner).count() + 1

        v_ad_format = data.get('video_ad_format')
        campaign_count = data.get('campaign_count', 1)
        ad_group_count = data.get('ad_group_count', 1)
        assert 0 < campaign_count <= BULK_CREATE_CAMPAIGNS_COUNT
        assert 0 < ad_group_count <= BULK_CREATE_AD_GROUPS_COUNT

        with transaction.atomic():
            account_data = dict(
                name="Account {}".format(number),
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
                campaign_data = dict(
                    name="Campaign {}".format(c_uid),
                    account_creation=account_creation.id,
                )
                campaign_data.update(data)
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
                    ad_group_creation = serializer.save()

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
