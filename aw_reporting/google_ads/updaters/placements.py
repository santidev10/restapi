from datetime import timedelta

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.constants import DEVICE_ENUM_TO_ID
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models.ad_words.constants import Device
from utils.datetime import now_in_default_tz


class PlacementUpdater(UpdateMixin):
    RESOURCE_NAME = "managed_placement_view"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.ad_group_criterion_type = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_channel_statistics = YTChannelStatistic.objects.filter(ad_group__campaign__account=account)
        self.existing_video_statistics = YTVideoStatistic.objects.filter(ad_group__campaign__account=account)

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")
        self.ad_group_criterion_type = client.get_type("CriterionTypeEnum", version="v2").CriterionType

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return
        self.drop_latest_stats(self.existing_channel_statistics, self.today)
        self.drop_latest_stats(self.existing_video_statistics, self.today)
        channel_saved_max_date = self.existing_channel_statistics.aggregate(max_date=Max("date")).get("max_date")
        video_saved_max_date = self.existing_video_statistics.aggregate(max_date=Max("date"),).get("max_date")

        if channel_saved_max_date and video_saved_max_date:
            saved_max_date = max(channel_saved_max_date, video_saved_max_date)
        else:
            saved_max_date = channel_saved_max_date or video_saved_max_date

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) if saved_max_date else min_acc_date
            max_date = max_acc_date

            placement_metrics = self._get_placement_metrics(min_date, max_date)
            channel_statistics, video_statistics = self._prepare_instances(placement_metrics)

            YTChannelStatistic.objects.safe_bulk_create(channel_statistics)
            YTVideoStatistic.objects.safe_bulk_create(video_statistics)

    def _get_placement_metrics(self, min_date, max_date):
        """
        Retrieve detail_placement_view resource performance
        :return: Google ads detail_placement_view resource search response
        """
        query_fields = self.format_query(constants.PLACEMENT_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE segments.date BETWEEN '{min_date}' AND '{max_date}'"
        placement_performance = self.ga_service.search(self.account.id, query=query)
        return placement_performance

    def _prepare_instances(self, placement_metrics):
        """
        Generate statistics based on placement type
        :param placement_metrics: Google ads detail_placement_view resource search response
        :return: tuple (lists) -> YTChannelStatistic, YTVideoStatistic
        """
        video_statistics = []
        channel_statistics = []
        for row in placement_metrics:
            placement_type = self.ad_group_criterion_type.Name(row.ad_group_criterion.type)
            if placement_type == constants.YOUTUBE_CHANNEL:
                model = YTChannelStatistic
                to_create = channel_statistics
                yt_id = row.ad_group_criterion.youtube_channel.channel_id.value
            elif placement_type == constants.YOUTUBE_VIDEO:
                model = YTVideoStatistic
                to_create = video_statistics
                yt_id = row.ad_group_criterion.youtube_video.video_id.value
            else:
                continue
            statistics = {
                "yt_id": yt_id,
                "date": row.segments.date.value,
                "ad_group_id": row.ad_group.id.value,
                "device_id": DEVICE_ENUM_TO_ID.get(row.segments.device, Device.COMPUTER),
                **self.get_quartile_views(row)
            }
            statistics.update(self.get_base_stats(row))
            to_create.append(model(**statistics))
        return channel_statistics, video_statistics
