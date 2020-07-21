import logging
from datetime import timedelta

from django.db.models import Max

from aw_reporting.adwords_reports import topics_performance_report
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.update.adwords_utils import DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS
from aw_reporting.update.adwords_utils import DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME
from aw_reporting.update.adwords_utils import format_click_types_report
from aw_reporting.update.adwords_utils import get_base_stats
from aw_reporting.update.adwords_utils import update_stats_with_click_type_data
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class TopicUpdater(UpdateMixin):
    RESOURCE_NAME = "topic_view"

    def __init__(self, account):
        self.account = account
        self.today = now_in_default_tz().date()

    def update(self, client):
        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        stats_queryset = TopicStatistic.objects.filter(ad_group__campaign__account=self.account)

        self.drop_latest_stats(stats_queryset, self.today)

        saved_max_date = stats_queryset.aggregate(max_date=Max("date")).get("max_date")

        topic_ids = set(Topic.objects.values_list("id", flat=True))

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) if saved_max_date else min_acc_date
            max_date = max_acc_date

            report = topics_performance_report(client, dates=(min_date, max_date), )

            click_type_report = topics_performance_report(
                client, dates=(min_date, max_date), fields=DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS)

            click_type_data = format_click_types_report(
                click_type_report, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
            generator = self._generate_stat_instances(TopicStatistic, topic_ids, report, click_type_data)
            TopicStatistic.objects.safe_bulk_create(generator)

    def _generate_stat_instances(self, model, topics, report, click_type_data):
        for row_obj in report:
            topic_name = row_obj.Criteria
            topic_id = int(row_obj.VerticalId)
            if topic_id not in topics:
                logger.warning(f"topic not found: id={topic_id}, name={topic_name}")
                continue
            stats = {
                "topic_id": topic_id,
                "date": row_obj.Date,
                "ad_group_id": int(row_obj.AdGroupId),
            }
            stats.update(get_base_stats(row_obj, quartiles=True))
            update_stats_with_click_type_data(
                stats, click_type_data, row_obj, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
            yield model(**stats)
