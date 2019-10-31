from datetime import timedelta

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.google_ads.utils import AD_WORDS_STABILITY_STATS_DAYS_COUNT
from aw_reporting.models import AdGroup
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from utils.datetime import now_in_default_tz


class VideoUpdater(UpdateMixin):
    RESOURCE_NAME = "video"
    UPDATE_FIELDS = constants.BASE_STATISTIC_MODEL_UPDATE_FIELDS

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_statistics = VideoCreativeStatistic.objects.filter(ad_group__campaign__account=account)
        self.existing_video_creative_ids = set(VideoCreative.objects.values_list("id", flat=True))
        self.existing_ad_group_ids = set([int(_id) for _id in AdGroup.objects.filter(campaign__account=self.account).values_list("id", flat=True)])

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return
        saved_max_date = self.existing_statistics.aggregate(max_date=Max("date"))["max_date"]
        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = (saved_max_date if saved_max_date else min_acc_date) - timedelta(days=AD_WORDS_STABILITY_STATS_DAYS_COUNT)
            max_date = max_acc_date

            video_performance = self._get_video_performance(min_date, max_date)
            self._create_instances(video_performance, min_date)

    def _get_video_performance(self, min_date, max_date):
        """
        Retrieve Google ads video resource metrics
        :return: Google ads video resource search response
        """
        query_fields = self.format_query(constants.VIDEO_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{max_date}'"
        video_performance = self.ga_service.search(self.account.id, query=query)
        return video_performance

    def _create_instances(self, video_performance, min_date):
        """
        Prepare VideoCreative and VideoCreativeStatistic instances to create
        :param video_performance: Google ads video resource search response
        :return: tuple (list) -> VideoCreative, VideoCreativeStatistic to create
        """
        existing_stats_from_min_date = {
            (int(s.ad_group_id), s.creative_id, str(s.date)): s.id for s
            in self.existing_statistics.filter(date__gte=min_date)
        }
        stats_to_create = []
        stats_to_update = []
        video_creative_to_create = []
        for row in video_performance:
            video_id = row.video.id.value
            ad_group_id = row.ad_group.id.value
            if video_id not in self.existing_video_creative_ids:
                self.existing_video_creative_ids.add(video_id)
                video_creative_to_create.append(
                    VideoCreative(id=video_id, duration=row.video.duration_millis.value)
                )
            if ad_group_id not in self.existing_ad_group_ids:
                continue
            statistics = {
                "creative_id": video_id,
                "ad_group_id": ad_group_id,
                "date": row.segments.date.value,
                **self.get_base_stats(row, quartiles=True)
            }
            stat_obj = VideoCreativeStatistic(**statistics)
            stat_unique_constraint = (stat_obj.ad_group_id, stat_obj.creative_id, stat_obj.date)
            stat_id = existing_stats_from_min_date.get(stat_unique_constraint)
            if stat_id is None:
                stats_to_create.append(stat_obj)
            else:
                stat_obj.id = stat_id
                stats_to_update.append(stat_obj)
        VideoCreative.objects.safe_bulk_create(video_creative_to_create)
        VideoCreativeStatistic.objects.safe_bulk_create(stats_to_create)
        VideoCreativeStatistic.objects.bulk_update(stats_to_update, fields=self.UPDATE_FIELDS)
