from datetime import timedelta

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import AdGroup
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from utils.datetime import now_in_default_tz


class VideoUpdater(UpdateMixin):
    RESOURCE_NAME = "video"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_statistics = VideoCreativeStatistic.objects.filter(ad_group__campaign__account=account)
        self.existing_video_creative_ids = set(VideoCreative.objects.values_list("id", flat=True))
        self.existing_ad_group_ids = set([int(_id) for _id in AdGroup.objects.filter(campaign__account=self.account).values_list("id", flat=True)])

    def update(self, client) -> None:
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return
        self.drop_latest_stats(self.existing_statistics, self.today)
        saved_max_date = self.existing_statistics.aggregate(max_date=Max("date"))["max_date"]

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) if saved_max_date else min_acc_date
            max_date = max_acc_date

            video_performance = self._get_video_performance(min_date, max_date)
            statistic_generator = self._prepare_instances(video_performance)
            VideoCreativeStatistic.objects.safe_bulk_create(statistic_generator)

    def _get_video_performance(self, min_date, max_date):
        """
        Retrieve Google ads video resource metrics
        :return: Google ads video resource search response
        """
        query_fields = self.format_query(constants.VIDEO_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE segments.date BETWEEN '{min_date}' AND '{max_date}'"
        video_performance = self.ga_service.search(self.account.id, query=query)
        return video_performance

    def _prepare_instances(self, video_performance) -> tuple:
        """
        Prepare VideoCreative and VideoCreativeStatistic instances to create
        :param video_metrics: Google ads video resource search response
        :return: tuple (list) -> VideoCreative, VideoCreativeStatistic to create
        """
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
                **self.get_base_stats(row)
            }
            yield VideoCreativeStatistic(**statistics)
        VideoCreative.objects.safe_bulk_create(video_creative_to_create)
