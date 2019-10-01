from datetime import timedelta
import logging

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class TopicUpdater(UpdateMixin):
    RESOURCE_NAME = "topic_view"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_topics = dict(Topic.objects.values_list("id", "name"))
        self.existing_statistics = TopicStatistic.objects.filter(ad_group__campaign__account=account)

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return
        self.drop_latest_stats(self.existing_statistics, self.today)
        saved_max_date = self.existing_statistics.aggregate(max_date=Max("date")).get("max_date")

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) if saved_max_date else min_acc_date
            max_date = max_acc_date

            click_type_data = self.get_clicks_report(
                self.client, self.ga_service, self.account,
                min_date, max_date,
                resource_name=self.RESOURCE_NAME
            )
            topic_performance = self._get_topic_performance(min_date, max_date)
            generator = self._instance_generator(topic_performance, click_type_data)
            TopicStatistic.objects.safe_bulk_create(generator)

    def _get_topic_performance(self, min_date, max_date):
        """
        Retrieve topic performance
        :return:
        """
        query_fields = self.format_query(constants.TOPIC_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE segments.date BETWEEN '{min_date}' AND '{max_date}'"
        topic_performance = self.ga_service.search(self.account.id, query=query)
        return topic_performance

    def _instance_generator(self, topic_performance, click_type_data):
        for row in topic_performance:
            try:
                topic_id = int(row.ad_group_criterion.topic.topic_constant.value.split("/")[-1])
                topic_name = row.ad_group_criterion.topic.path[-1].value
            except (ValueError, IndexError):
                logger.error(f"Unable to extract topic for cid: {self.account.id} ad_group_id: {row.ad_group.id.value} criterion_id: {row.ad_group_criterion.criterion_id.value}")
                continue
            else:
                if topic_id not in self.existing_topics:
                    logger.error(f"Topic not found in existing topics: {topic_id}, {topic_name}")
                    continue
                # Check if topic name should be updated
                if self.existing_topics[topic_id] != topic_name:
                    Topic.objects.filter(id=topic_id).update(name=topic_name)
                statistics = {
                    "topic_id": topic_id,
                    "date": row.segments.date.value,
                    "ad_group_id": row.ad_group.id.value,
                    **self.get_quartile_views(row)
                }
                statistics.update(self.get_base_stats(row))
                click_data = self.get_stats_with_click_type_data(statistics, click_type_data, row, resource_name=self.RESOURCE_NAME)
                statistics.update(click_data)
                yield TopicStatistic(**statistics)