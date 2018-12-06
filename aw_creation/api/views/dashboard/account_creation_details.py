import logging
import requests

from django.core.cache import cache
from django.conf import settings
from django.db.models import Avg
from django.db.models import Case
from django.db.models import F
from django.db.models import FloatField as AggrFloatField
from django.db.models import Sum
from django.db.models import When
from django.http import Http404
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.api.serializers.dashboard.account_creation_details_serializer import \
    DashboardAccountCreationDetailsSerializer
from aw_creation.models import AccountCreation
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AgeRanges
from aw_reporting.models import CONVERSIONS
from aw_reporting.models import GenderStatistic
from aw_reporting.models import Genders
from aw_reporting.models import QUARTILE_STATS
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import device_str
from aw_reporting.models import dict_quartiles_to_rates
from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException
from userprofile.constants import UserSettingsKey
from utils.db.aggregators import ConcatAggregate
from utils.permissions import UserHasDashboardPermission

logger = logging.getLogger(__name__)


@demo_view_decorator
class DashboardAccountCreationDetailsAPIView(APIView):
    serializer_class = DashboardAccountCreationDetailsSerializer
    permission_classes = (IsAuthenticated, UserHasDashboardPermission)

    def post(self, request, pk):
        account_creation = self._get_account_creation(request, pk)
        if account_creation is None:
            return Response(status=HTTP_404_NOT_FOUND)
        data = self.serializer_class(account_creation, context={"request": request}).data
        show_conversions = self.request.user.get_aw_settings().get(UserSettingsKey.SHOW_CONVERSIONS)
        data["details"] = self.get_details_data(account_creation, show_conversions)
        return Response(data=data)

    def _get_account_creation(self, request, pk):
        queryset = AccountCreation.objects.all()
        user_settings = request.user.get_aw_settings()
        if not user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS):
            visible_accounts = user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
            queryset = queryset.filter(account__id__in=visible_accounts)
        try:
            return queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404

    def get_details_data(self, account_creation, show_conversions):
        if show_conversions:
            ads_and_placements_stats = {s: Sum(s) for s in
                                        CONVERSIONS + QUARTILE_STATS}
        else:
            ads_and_placements_stats = {s: Sum(s) for s in QUARTILE_STATS}

        fs = dict(ad_group__campaign__account=account_creation.account)
        data = AdGroupStatistic.objects.filter(**fs).aggregate(
            ad_network=ConcatAggregate('ad_network', distinct=True),
            average_position=Avg(
                Case(
                    When(
                        average_position__gt=0,
                        then=F('average_position'),
                    ),
                    output_field=AggrFloatField(),
                )
            ),
            impressions=Sum("impressions"),
            **ads_and_placements_stats
        )
        dict_quartiles_to_rates(data)
        del data['impressions']

        annotate = dict(v=Sum('cost'))
        creative = VideoCreativeStatistic.objects.filter(**fs).values(
            "creative_id").annotate(**annotate).order_by('v')[:3]
        if creative:
            ids = [i['creative_id'] for i in creative]
            unresolved_ids = []
            creative = []
            try:
                videos_singledb_info = SingleDatabaseApiConnector().get_videos_base_info(ids)
            except SingleDatabaseApiConnectorException as e:
                logger.critical(e)
            else:
                video_info = {i['id']: i for i in videos_singledb_info}
                for video_id in ids:
                    info = video_info.get(video_id)
                    if info is not None:
                        creative.append(
                            dict(
                                id=video_id,
                                name=info.get("title"),
                                thumbnail=info.get('thumbnail_image_url'),
                            )
                        )
                    else:
                        unresolved_ids.append(video_id)

            if unresolved_ids:
                try:
                    videos_details = self.resolve_videos_info(unresolved_ids)
                except Exception as e:
                    logger.error(e)
                    videos_details = {}

                for video_id in ids:
                    info = videos_details.get(video_id, dict(id=video_id, name=None, thumbnail=None))
                    creative.append(info)
        data.update(creative=creative)

        # second section
        gender = GenderStatistic.objects.filter(**fs).values(
            'gender_id').order_by('gender_id').annotate(**annotate)
        gender = [dict(name=Genders[i['gender_id']], value=i['v']) for i in
                  gender]

        age = AgeRangeStatistic.objects.filter(**fs).values(
            "age_range_id").order_by("age_range_id").annotate(**annotate)
        age = [dict(name=AgeRanges[i['age_range_id']], value=i['v']) for i in
               age]

        device = AdGroupStatistic.objects.filter(**fs).values(
            "device_id").order_by("device_id").annotate(**annotate)
        device = [dict(name=device_str(i['device_id']), value=i['v']) for i in
                  device]
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

    @staticmethod
    def resolve_videos_info(ids):
        cache_key = "video_title_thumbnail_{}"
        cache_timeout = 86400  # 24 hours
        requests_timeout = 2
        api_url = "https://www.googleapis.com/youtube/v3/videos"
        details = {}
        unresolved_ids = []
        for video_id in ids:
            info = cache.get(cache_key.format(video_id))
            if info:
                details[video_id] = info
            else:
                unresolved_ids.append(video_id)

        if unresolved_ids:
            ids_string = ",".join(unresolved_ids)
            params = dict(key=settings.YOUTUBE_API_ALTERNATIVE_DEVELOPER_KEY,
                          id=ids_string,
                          part="snippet",
                          maxResults=len(ids))
            response = requests.get(url=api_url, params=params, timeout=requests_timeout)

            data = response.json()
            items = data.get("items", [])

            for item in items:
                video_id = item.get("id")
                snippet = item.get("snippet", {})
                info = dict(
                    name=snippet.get("title"),
                    thumbnail=snippet.get("thumbnails", {}).get("high", {}).get("url"),
                )
                cache.set(cache_key.format(video_id), info, cache_timeout)
                details[video_id] = info
        return details
