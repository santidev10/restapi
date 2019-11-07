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
    MANAGED_RESOURCE_NAME = "managed_placement_view"
    GROUP_RESOURCE_NAME = "group_placement_view"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.ad_group_criterion_type = None
        self.placement_type = None
        self.max_acc_date = None
        self.min_acc_date = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.refresh_date = self.today - timedelta(days=7)
        self.existing_channel_statistics = YTChannelStatistic.objects.filter(ad_group__campaign__account=account)
        self.existing_video_statistics = YTVideoStatistic.objects.filter(ad_group__campaign__account=account)

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")
        self.ad_group_criterion_type = client.get_type("CriterionTypeEnum", version="v2").CriterionType
        self.placement_type = client.get_type("PlacementTypeEnum", version="v2").PlacementType
        self.min_acc_date, self.max_acc_date = self.get_account_border_dates(self.account)

        self._update_managed_statistics()
        self._update_group_statistics()

    def _update_managed_statistics(self):
        if self.max_acc_date is None:
            return
        channel_managed_stats = self.existing_channel_statistics.filter(placement_type=0)
        video_managed_stats = self.existing_video_statistics.filter(placement_type=0)

        self.drop_custom_stats(channel_managed_stats, self.refresh_date, self.today)
        self.drop_custom_stats(video_managed_stats, self.refresh_date, self.today)
        channel_saved_max_date = channel_managed_stats.aggregate(max_date=Max("date")).get("max_date")
        video_saved_max_date = video_managed_stats.aggregate(max_date=Max("date"), ).get("max_date")

        if channel_saved_max_date and video_saved_max_date:
            saved_max_date = max(channel_saved_max_date, video_saved_max_date)
        else:
            saved_max_date = channel_saved_max_date or video_saved_max_date

        if saved_max_date is None or saved_max_date < self.max_acc_date:
            min_date = saved_max_date + timedelta(days=1) if saved_max_date else self.min_acc_date
            max_date = self.max_acc_date

            # As Out-Of-Memory errors prevention we have to download this report individually
            # and save to the DB only the necessary records at each load.
            # Any accumulation of such a report in memory is unacceptable because it may cause Out-Of-Memory error.
            placement_metrics = self._get_managed_placement_metrics(min_date, max_date)
            channel_statistics_generator = self._prepare_managed_instances(placement_metrics, YTChannelStatistic, stat_type=constants.YOUTUBE_CHANNEL)
            YTChannelStatistic.objects.safe_bulk_create(channel_statistics_generator)

            placement_metrics = self._get_managed_placement_metrics(min_date, max_date)
            video_statistics_generator = self._prepare_managed_instances(placement_metrics, YTVideoStatistic, stat_type=constants.YOUTUBE_VIDEO)
            YTVideoStatistic.objects.safe_bulk_create(video_statistics_generator)

    def _update_group_statistics(self):
        if self.max_acc_date is None:
            return
        group_channel_stats = self.existing_channel_statistics.filter(placement_type=1)
        video_group_stats = self.existing_video_statistics.filter(placement_type=1)

        self.drop_custom_stats(group_channel_stats, self.refresh_date, self.today)
        self.drop_custom_stats(video_group_stats, self.refresh_date, self.today)
        channel_saved_max_date = group_channel_stats.aggregate(max_date=Max("date")).get("max_date")
        video_saved_max_date = video_group_stats.aggregate(max_date=Max("date"), ).get("max_date")

        if channel_saved_max_date and video_saved_max_date:
            saved_max_date = max(channel_saved_max_date, video_saved_max_date)
        else:
            saved_max_date = channel_saved_max_date or video_saved_max_date

        if saved_max_date is None or saved_max_date < self.max_acc_date:
            min_date = saved_max_date + timedelta(days=1) if saved_max_date else self.min_acc_date
            max_date = self.max_acc_date

            placement_metrics = self._get_group_placement_metrics(min_date, max_date)
            channel_statistics_generator = self._prepare_group_instances(placement_metrics, YTChannelStatistic, stat_type=constants.YOUTUBE_CHANNEL)
            YTChannelStatistic.objects.safe_bulk_create(channel_statistics_generator, batch_size=1000)

            placement_metrics = self._get_group_placement_metrics(min_date, max_date)
            video_statistics_generator = self._prepare_group_instances(placement_metrics, YTVideoStatistic, stat_type=constants.YOUTUBE_VIDEO)
            YTVideoStatistic.objects.safe_bulk_create(video_statistics_generator, batch_size=1000)

    def _get_managed_placement_metrics(self, min_date, max_date):
        """
        Retrieve detail_placement_view resource performance
        :return: Google ads detail_placement_view resource search response
        """
        query_fields = self.format_query(constants.PLACEMENT_PERFORMANCE_FIELDS["managed"])
        query = f"SELECT {query_fields} FROM {self.MANAGED_RESOURCE_NAME} WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{max_date}'"
        placement_performance = self.ga_service.search(self.account.id, query=query)
        return placement_performance

    def _get_group_placement_metrics(self, min_date, max_date):
        """
        Retrieve group_placement_view resource performance for automatic placement metrics
        :return: Google ads detail_placement_view resource search response
        """
        query_fields = self.format_query(constants.PLACEMENT_PERFORMANCE_FIELDS["group"])
        query = f"SELECT {query_fields} FROM {self.GROUP_RESOURCE_NAME} WHERE metrics.impressions > 50 AND segments.date BETWEEN '{min_date}' AND '{max_date}'"
        placement_performance = self.ga_service.search(self.account.id, query=query)
        return placement_performance

    def _prepare_managed_instances(self, managed_placement_metrics, model, stat_type=constants.YOUTUBE_CHANNEL):
        """
        Generate statistics based on placement type
        :param managed_placement_metrics: Google ads detail_placement_view resource search response
        :return: tuple (lists) -> YTChannelStatistic, YTVideoStatistic
        """
        for row in managed_placement_metrics:
            placement_type = self.ad_group_criterion_type.Name(row.ad_group_criterion.type)
            if placement_type == stat_type:
                if placement_type == constants.YOUTUBE_CHANNEL:
                    yt_id = row.ad_group_criterion.youtube_channel.channel_id.value
                elif placement_type == constants.YOUTUBE_VIDEO:
                    yt_id = row.ad_group_criterion.youtube_video.video_id.value
                else:
                    continue
            else:
                continue
            statistics = {
                "yt_id": yt_id,
                "date": row.segments.date.value,
                "ad_group_id": row.ad_group.id.value,
                "device_id": DEVICE_ENUM_TO_ID.get(row.segments.device, Device.COMPUTER),
                "placement_type": 0,
                **self.get_quartile_views(row)
            }
            statistics.update(self.get_base_stats(row))
            yield model(**statistics)

    def _prepare_group_instances(self, group_placement_metrics, model, stat_type=constants.YOUTUBE_CHANNEL):
        """
        Generate statistics based on placement type
        :param group_placement_metrics: Google ads group_placement_view resource search response
        :return: tuple (lists) -> YTChannelStatistic, YTVideoStatistic
        """
        for row in group_placement_metrics:
            placement_type = self.placement_type.Name(row.group_placement_view.placement_type)
            if placement_type == stat_type:
                yt_id = row.group_placement_view.placement.value
            else:
                continue
            statistics = {
                "yt_id": yt_id,
                "date": row.segments.date.value,
                "ad_group_id": row.ad_group.id.value,
                "placement_type": 1,
                "device_id": DEVICE_ENUM_TO_ID.get(row.segments.device, Device.COMPUTER),
            }
            statistics.update(self.get_base_stats(row))
            yield model(**statistics)
