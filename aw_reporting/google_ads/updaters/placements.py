from datetime import datetime
from datetime import timedelta

from django.db.models import Max
import pytz

from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.adwords_reports import placement_performance_report
from aw_reporting.update.adwords_utils import get_base_stats
from aw_reporting.models.ad_words.constants import get_device_id_by_name


class PlacementUpdater(UpdateMixin):
    def __init__(self, account):
        self.account = account
        self.today = datetime.now(tz=pytz.timezone(account.timezone)).date()

    def update(self, client):
        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        channel_stats_queryset = YTChannelStatistic.objects.filter(
            ad_group__campaign__account=self.account
        )
        video_stats_queryset = YTVideoStatistic.objects.filter(
            ad_group__campaign__account=self.account
        )
        self.drop_latest_stats(channel_stats_queryset, self.today)
        self.drop_latest_stats(video_stats_queryset, self.today)

        channel_saved_max_date = channel_stats_queryset.aggregate(
            max_date=Max("date"),
        ).get("max_date")
        video_saved_max_date = video_stats_queryset.aggregate(
            max_date=Max("date"),
        ).get("max_date")

        if channel_saved_max_date and video_saved_max_date:
            saved_max_date = max(channel_saved_max_date, video_saved_max_date)
        else:
            saved_max_date = channel_saved_max_date or video_saved_max_date

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) \
                if saved_max_date else min_acc_date
            max_date = max_acc_date

            # As Out-Of-Memory errors prevention we have to download this report twice
            # and save to the DB only the necessary records at each load.
            # Any accumulation of such a report in memory is unacceptable because it may cause Out-Of-Memory error.
            report = placement_performance_report(client, dates=(min_date, max_date))
            generator = self._generate_stat_instances(YTChannelStatistic, "/channel/", report)
            YTChannelStatistic.objects.safe_bulk_create(generator)

            report = placement_performance_report(client, dates=(min_date, max_date))
            generator = self._generate_stat_instances(YTVideoStatistic, "/video/", report)
            YTVideoStatistic.objects.safe_bulk_create(generator)

    def _generate_stat_instances(self, model, contains, report):
        for row_obj in report:
            display_name = row_obj.DisplayName

            if contains not in display_name:
                continue

            criteria = row_obj.Criteria.strip()

            # only youtube ids we need in criteria
            if "youtube.com/" in criteria:
                criteria = criteria.split("/")[-1]

            stats = {
                "yt_id": criteria,
                "date": row_obj.Date,
                "ad_group_id": int(row_obj.AdGroupId),
                "device_id": get_device_id_by_name(row_obj.Device),
            }
            stats.update(get_base_stats(row_obj, quartiles=True))

            yield model(**stats)
