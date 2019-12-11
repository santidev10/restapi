from datetime import timedelta

from django.db.models import Max

from aw_reporting.adwords_reports import video_performance_report
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import AdGroup
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.update.adwords_utils import get_base_stats
from utils.datetime import now_in_default_tz


class VideoUpdater(UpdateMixin):
    RESOURCE_NAME = "video"

    def __init__(self, account):
        self.account = account
        self.today = now_in_default_tz().date()

    def update(self, client):
        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        stats_queryset = VideoCreativeStatistic.objects.filter(
            ad_group__campaign__account=self.account
        )
        self.drop_latest_stats(stats_queryset, self.today)

        saved_max_date = stats_queryset.aggregate(
            max_date=Max("date"))["max_date"]

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) \
                if saved_max_date else min_acc_date
            max_date = max_acc_date

            v_ids = list(
                VideoCreative.objects.all().values_list("id", flat=True)
            )
            ad_group_ids = set(
                AdGroup.objects.filter(
                    campaign__account=self.account
                ).values_list("id", flat=True)
            )
            dates = (min_date, max_date)
            reports = video_performance_report(client, dates=dates)
            create = []
            create_creative = []
            for row_obj in reports:
                video_id = row_obj.VideoId.strip()
                if video_id not in v_ids:
                    v_ids.append(video_id)
                    create_creative.append(
                        VideoCreative(
                            id=video_id,
                            duration=row_obj.VideoDuration,
                        )
                    )

                ad_group_id = row_obj.AdGroupId
                if ad_group_id not in ad_group_ids:
                    continue

                stats = dict(
                    creative_id=video_id,
                    ad_group_id=ad_group_id,
                    date=row_obj.Date,
                    **get_base_stats(row_obj, quartiles=True)
                )
                create.append(
                    VideoCreativeStatistic(**stats)
                )

            if create_creative:
                VideoCreative.objects.safe_bulk_create(create_creative)

            if create:
                VideoCreativeStatistic.objects.safe_bulk_create(create)
