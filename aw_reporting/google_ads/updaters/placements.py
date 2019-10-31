from datetime import timedelta

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.utils import AD_WORDS_STABILITY_STATS_DAYS_COUNT
from aw_reporting.google_ads.constants import DEVICE_ENUM_TO_ID
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models.ad_words.constants import Device
from utils.datetime import now_in_default_tz


class PlacementUpdater(UpdateMixin):
    RESOURCE_NAME = "managed_placement_view"
    UPDATE_FIELDS = constants.BASE_STATISTIC_MODEL_UPDATE_FIELDS

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
        channel_saved_max_date = self.existing_channel_statistics.aggregate(max_date=Max("date")).get("max_date")
        video_saved_max_date = self.existing_video_statistics.aggregate(max_date=Max("date"),).get("max_date")

        if channel_saved_max_date and video_saved_max_date:
            saved_max_date = max(channel_saved_max_date, video_saved_max_date)
        else:
            saved_max_date = channel_saved_max_date or video_saved_max_date

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = (saved_max_date if saved_max_date else min_acc_date) - timedelta(days=AD_WORDS_STABILITY_STATS_DAYS_COUNT)
            max_date = max_acc_date

            # As Out-Of-Memory errors prevention we have to download this report twice
            # and save to the DB only the necessary records at each load.
            # Any accumulation of such a report in memory is unacceptable because it may cause Out-Of-Memory error.
            placement_metrics = self._get_placement_metrics(min_date, max_date)
            self._create_instances(placement_metrics, YTChannelStatistic, self.existing_channel_statistics, min_date, stat_type=constants.YOUTUBE_CHANNEL)

            placement_metrics = self._get_placement_metrics(min_date, max_date)
            self._create_instances(placement_metrics, YTVideoStatistic, self.existing_video_statistics, min_date, stat_type=constants.YOUTUBE_VIDEO)

    def _get_placement_metrics(self, min_date, max_date):
        """
        Retrieve detail_placement_view resource performance
        :return: Google ads detail_placement_view resource search response
        """
        query_fields = self.format_query(constants.PLACEMENT_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{max_date}'"
        placement_performance = self.ga_service.search(self.account.id, query=query)
        return placement_performance

    def _create_instances(self, placement_metrics, model, existing_stats, min_date, stat_type=constants.YOUTUBE_CHANNEL):
        """
        Generate statistics based on placement type
        :param placement_metrics: Google ads detail_placement_view resource search response
        :return: tuple (lists) -> YTChannelStatistic, YTVideoStatistic
        """
        existing_stats_from_min_date = {
            (s.ad_group_id, s.yt_id, s.device_id, str(s.date)): s.id for s
            in existing_stats.filter(date__gte=min_date)
        }
        stats_to_create = []
        stats_to_update = []
        for row in placement_metrics:
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
                "ad_group_id": str(row.ad_group.id.value),
                "device_id": DEVICE_ENUM_TO_ID.get(row.segments.device, Device.COMPUTER),
                **self.get_quartile_views(row)
            }
            statistics.update(self.get_base_stats(row))

            stat_obj = model(**statistics)
            stat_unique_constraint = (stat_obj.ad_group_id, stat_obj.yt_id, stat_obj.device_id, stat_obj.date)
            stat_id = existing_stats_from_min_date.get(stat_unique_constraint)

            if stat_id is None:
                stats_to_create.append(stat_obj)
            else:
                stat_obj.id = stat_id
                stats_to_update.append(stat_obj)
        model.objects.safe_bulk_create(stats_to_create)
        model.objects.bulk_update(stats_to_create, fields=self.UPDATE_FIELDS)
