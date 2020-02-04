from datetime import timedelta
import logging

from django.db.models import Max

from aw_reporting.adwords_reports import gender_performance_report
from aw_reporting.models import GenderStatistic
from aw_reporting.models import Genders
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.update.adwords_utils import format_click_types_report
from aw_reporting.update.adwords_utils import get_base_stats
from aw_reporting.update.adwords_utils import quart_views
from aw_reporting.update.adwords_utils import update_stats_with_click_type_data
from aw_reporting.update.adwords_utils import DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS
from aw_reporting.update.adwords_utils import DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class GenderUpdater(UpdateMixin):
    RESOURCE_NAME = "gender_view"

    def __init__(self, account):
        """
        :param account: Google Ads Account
        """
        self.account = account
        self.today = now_in_default_tz().date()

    def update(self, client):
        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        stats_queryset = GenderStatistic.objects.filter(
            ad_group__campaign__account=self.account
        )
        self.drop_latest_stats(stats_queryset, self.today)
        saved_max_date = stats_queryset.aggregate(
            max_date=Max("date")).get("max_date")

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) \
                if saved_max_date else min_acc_date
            max_date = max_acc_date
            report = gender_performance_report(
                client, dates=(min_date, max_date),
            )
            click_type_report = gender_performance_report(
                client, dates=(min_date, max_date), fields=DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS)
            click_type_data = format_click_types_report(
                click_type_report, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
            generator = self._generate_stat_instances(GenderStatistic, Genders, report, click_type_data)
            GenderStatistic.objects.safe_bulk_create(generator)

    def _generate_stat_instances(self, stats_model, gender_model, report, click_type_data):
        for row_obj in report:
            stats = {
                "gender_id": gender_model.index(row_obj.Criteria),
                "date": row_obj.Date,
                "ad_group_id": int(row_obj.AdGroupId),
                "video_views_25_quartile": quart_views(row_obj, 25),
                "video_views_50_quartile": quart_views(row_obj, 50),
                "video_views_75_quartile": quart_views(row_obj, 75),
                "video_views_100_quartile": quart_views(row_obj, 100),
            }
            stats.update(get_base_stats(row_obj))
            update_stats_with_click_type_data(
                stats, click_type_data, row_obj, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
            yield stats_model(**stats)
