import logging
from datetime import date
from datetime import timedelta

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import RemarkList
from aw_reporting.models import Audience
from aw_reporting.google_ads.update_mixin import UpdateMixin
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class GoogleAdsAudienceTypes:
    USER_LIST = "USER_LIST"
    USER_INTEREST = "USER_INTEREST"
    CUSTOM_AFFINITY = "CUSTOM_AFFINITY"
    TOPIC = "TOPIC"
    CUSTOM_INTENT = "CUSTOM_INTENT"


class InterestUpdater(UpdateMixin):
    RESOURCE_NAME = "ad_group_audience_view"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.criterion_type_enum = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_audience_stats = AudienceStatistic.objects.filter(
            ad_group__campaign__account=account
        )
        self.existing_remark_stats = RemarkStatistic.objects.filter(
            ad_group__campaign__account=account
        )
        # Coerce to ints since Google ads api response for id fields are ints
        self.existing_remarketing_ids = set([int(_id) for _id in RemarkList.objects.values_list("id", flat=True)])
        self.existing_audience_interest_ids = set([int(_id) for _id in Audience.objects.values_list("id", flat=True)])
        self.audience_statistics_to_create = []
        self.remarketing_to_create = []
        self.remarketing_statistics_to_create = []
        self.custom_audiences_to_create = []

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")
        self.criterion_type_enum = self.client.get_type("CriterionTypeEnum", version="v2").CriterionType

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        self.drop_latest_stats(self.existing_audience_stats, self.today)
        self.drop_latest_stats(self.existing_remark_stats, self.today)

        # Get most recent statistics dates
        audience_max_date = self.existing_audience_stats.aggregate(max_date=Max("date")).get("max_date")
        remark_max_date = self.existing_remark_stats.aggregate(max_date=Max("date")).get("max_date")

        # Get date range to query for
        if audience_max_date and remark_max_date:
            saved_max_date = max(audience_max_date, remark_max_date)
        else:
            saved_max_date = audience_max_date or remark_max_date
        min_date = saved_max_date + timedelta(days=1) if saved_max_date else min_acc_date
        max_date = max_acc_date

        click_type_data = self.get_clicks_report(
            self.client, self.ga_service, self.account,
            min_date, max_date,
            resource_name=self.RESOURCE_NAME
        )
        audience_user_lists = self._get_audience_user_lists()
        audience_performance = self._get_audience_performance(min_date, max_date)
        self._process(audience_user_lists, audience_performance, click_type_data)

    def _process(self, audience_user_lists, audience_performance, click_type_data):
        user_list_names = {
            row.user_list.id.value: row.user_list.name.value
            for row in audience_user_lists
        }

        for row in audience_performance:
            ad_group_id = row.ad_group.id.value
            statistics = {
                "date": row.segments.date.value,
                "ad_group_id": ad_group_id,
                **self.get_quartile_views(row),
                **self.get_base_stats(row)
            }
            click_data = self.get_stats_with_click_type_data(statistics, click_type_data, row, resource_name=self.RESOURCE_NAME)
            statistics.update(click_data)

            audience_type = self.criterion_type_enum.Name(row.ad_group_criterion.type)
            if audience_type == GoogleAdsAudienceTypes.USER_LIST:
                audience_id = self._extract_audience_id(row.ad_group_criterion.user_list.user_list.value)
                user_list_name = user_list_names.get(audience_id)
                self._handle_user_list(statistics, audience_id, user_list_name)
            elif audience_type == GoogleAdsAudienceTypes.USER_INTEREST:
                audience_id = self._extract_audience_id(row.ad_group_criterion.user_interest.user_interest_category.value)
                try:
                    self._handle_user_interest(statistics, audience_id)
                except InterestUpdaterMissingAudienceException:
                    logger.error(f"Audience {audience_id} not found for cid: {self.account.id}, ad_group_id: {ad_group_id}")
                    continue
            elif audience_type == GoogleAdsAudienceTypes.CUSTOM_AFFINITY:
                audience_id = self._extract_audience_id(row.ad_group_criterion.custom_affinity.custom_affinity.value)
                self._handle_custom_affinity(statistics, audience_id)

            elif audience_type == GoogleAdsAudienceTypes.CUSTOM_INTENT:
                audience_id = self._extract_audience_id(row.ad_group_criterion.custom_intent.custom_intent.value)
                self._handle_custom_intent(statistics, audience_id)

            else:
                logger.error(
                    f"Undefined criteria. ad_group_id: {ad_group_id}, criterion_id: {row.ad_group_criterion.criterion_id.value}, audience_type: {audience_type}"
                )

        RemarkList.objects.safe_bulk_create(self.remarketing_to_create)
        RemarkStatistic.objects.safe_bulk_create(self.remarketing_statistics_to_create)
        Audience.objects.safe_bulk_create(self.custom_audiences_to_create)
        AudienceStatistic.objects.safe_bulk_create(self.audience_statistics_to_create)

    def _handle_user_list(self, statistics, audience_id, user_list_name):
        """
        Mutates statistics and adds RemarkStatistic instances to create
        :param statistics: dict -> Statistics of Google ads row being processed
        :param audience_id: int -> Google ads audience id
        :param user_list_name: str -> User list name on Google ads
        """
        # If the audience type is USER_LIST, then implicitly determine criterion is remarketing since
        # user lists are only used for remarketing
        statistics["remark_id"] = audience_id
        if audience_id not in self.existing_remarketing_ids:
            self.existing_remarketing_ids.add(audience_id)
            self.remarketing_to_create.append(RemarkList(id=audience_id, name=user_list_name))
        self.remarketing_statistics_to_create.append(RemarkStatistic(**statistics))

    def _handle_user_interest(self, statistics, audience_id):
        """
        Mutates statistics and adds AudienceStatistic instances to create
        :param statistics: dict -> Statistics of Google ads row being processed
        :param audience_id: int -> Google ads audience id
        :return: Google ads search response
        """
        if audience_id not in self.existing_audience_interest_ids:
            raise InterestUpdaterMissingAudienceException
        statistics.update(audience_id=audience_id)
        self.audience_statistics_to_create.append(AudienceStatistic(**statistics))

    def _handle_custom_affinity(self, statistics, audience_id):
        """
        Mutates statistics and adds Audience, AudienceStatistic instances to create
        :param statistics: dict -> Statistics of Google ads row being processed
        :param audience_id: int -> Google ads audience id
        """
        if audience_id not in self.existing_audience_interest_ids:
            name = f"customaffinity::{audience_id}"
            self.existing_audience_interest_ids.add(audience_id)
            self.custom_audiences_to_create.append(
                Audience(id=audience_id, name=name, type=Audience.CUSTOM_AFFINITY_TYPE)
            )
        statistics.update(audience_id=audience_id)
        self.audience_statistics_to_create.append(AudienceStatistic(**statistics))

    def _handle_custom_intent(self, statistics, audience_id):
        """
        Mutates statistics and adds Audience, AudienceStatistic instances to create
        :param statistics: dict -> Statistics of Google ads row being processed
        :param audience_id: int -> Google ads audience id
        """
        if audience_id not in self.existing_audience_interest_ids:
            name = f"customintent::{audience_id}"
            self.existing_audience_interest_ids.add(audience_id)
            self.custom_audiences_to_create.append(
                Audience(id=audience_id, name=name, type=Audience.CUSTOM_INTENT_TYPE)
            )
        statistics.update(audience_id=audience_id)
        self.audience_statistics_to_create.append(AudienceStatistic(**statistics))

    def _get_audience_user_lists(self):
        """
        Query for ad_group_audience view user lists
        :return: Google ads search response
        """
        query_fields = self.format_query(constants.AUDIENCE_PERFORMANCE_FIELDS["user_list"])
        query = f"SELECT {query_fields} FROM user_list"
        audience_performance = self.ga_service.search(self.account.id, query=query)
        return audience_performance

    def _get_audience_performance(self, min_date, max_date):
        """
        Query for ad_group_audience view user performance
        :return: Google ads search response
        """
        query_fields = self.format_query(constants.AUDIENCE_PERFORMANCE_FIELDS["performance"])
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{max_date}'"
        audience_performance = self.ga_service.search(self.account.id, query=query)
        return audience_performance

    def _extract_audience_id(self, user_interest_resource_name: str):
        """
        Extract User interest audience id
        :param user_interest_resource_name: "customers/9936665850/userInterests/92901"
        :return: int
        """
        interest_id = None
        if not user_interest_resource_name:
            return interest_id
        interest_id = int(user_interest_resource_name.split("/")[-1])
        return interest_id


class InterestUpdaterMissingAudienceException(Exception):
    pass
