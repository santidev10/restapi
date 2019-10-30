from datetime import timedelta

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.google_ads.utils import AD_WORDS_STABILITY_STATS_DAYS_COUNT
from aw_reporting.models import Ad
from aw_reporting.models import AdStatistic
from utils.datetime import now_in_default_tz


class AdUpdater(UpdateMixin):
    RESOURCE_NAME = "ad_group_ad"
    UPDATE_FIELDS = constants.STATS_MODELS_COMBINED_UPDATE_FIELDS

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.ad_group_criterion_status_enum = None
        self.ad_group_ad_status_enum = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_statistics = AdStatistic.objects.filter(ad__ad_group__campaign__account=account)
        self.existing_ad_ids = set([int(_id) for _id in Ad.objects.filter(ad_group__campaign__account=account).values_list("id", flat=True)])

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")
        self.ad_group_criterion_status_enum = client.get_type("AdGroupCriterionApprovalStatusEnum",
                                                              version="v2").AdGroupCriterionApprovalStatus
        self.ad_group_ad_status_enum = client.get_type("AdGroupAdStatusEnum", version="v2").AdGroupAdStatus

        # Get oldest and newest AdGroup statistics dates for account
        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        # Get newest Ad statistics dates for account
        saved_max_date = self.existing_statistics.aggregate(max_date=Max("date")).get("max_date")

        # Only update if Ad statistics is older than AdGroup statistics
        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = (saved_max_date if saved_max_date else min_acc_date) - timedelta(days=AD_WORDS_STABILITY_STATS_DAYS_COUNT)
            max_date = max_acc_date

            click_type_data = self.get_clicks_report(
                self.client, self.ga_service, self.account,
                min_date, max_date,
                resource_name=self.RESOURCE_NAME
            )
            ad_performance = self._get_ad_performance(min_date, max_date)
            self._generate_instances(ad_performance, click_type_data, min_date)

    def _get_ad_performance(self, min_date, max_date):
        """
        Retrieve ads performance
        :param min_date: str -> 2012-01-01
        :param max_date: str -> 2012-12-31
        :return: Google Ads search response
        """
        query_fields = self.format_query(constants.AD_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{max_date}'"
        ad_performance = self.ga_service.search(self.account.id, query=query)
        return ad_performance

    def _generate_instances(self, ad_performance: iter, click_type_data: dict, min_stat_date):
        """
        Generator that yields GenderStatistic instances
        :param ad_performance: iter -> Google ads ad_group_ad resource search response
        :return:
        """
        updated_ad_ids = set()
        ads_to_create = []
        stats_to_update = []
        stats_to_create = []
        existing_stats_from_min_date = {
            (int(s.ad_id), str(s.date)): s.id for s
            in self.existing_statistics.filter(date__gte=min_stat_date)
        }
        for row in ad_performance:
            ad = row.ad_group_ad.ad
            ad_id = ad.id.value
            if ad_id not in updated_ad_ids:
                updated_ad_ids.add(ad_id)
                ad_data = {
                    "headline": ad.text_ad.headline.value,
                    "creative_name": ad.name.value,
                    "display_url": ad.display_url.value,
                    "status": "enabled" if self.ad_group_ad_status_enum.Name(row.ad_group_ad.status).lower() == "enabled" else "disabled",
                    "is_disapproved": self.ad_group_criterion_status_enum.Name(row.ad_group_ad.policy_summary.approval_status) == "DISAPPROVED"
                }
                kwargs = {"id": ad_id, "ad_group_id": row.ad_group.id.value}
                if ad_id in self.existing_ad_ids:
                    Ad.objects.filter(**kwargs).update(**ad_data)
                else:
                    self.existing_ad_ids.add(ad_id)
                    ad_data.update(kwargs)
                    ads_to_create.append(Ad(**ad_data))
            statistics = {
                "date": row.segments.date.value,
                "ad_id": ad_id,
                "average_position": 0.0,
                **self.get_quartile_views(row)
            }
            statistics.update(self.get_base_stats(row))
            # Update statistics with click performance obtained in get_clicks_report
            click_data = self.get_stats_with_click_type_data(statistics, click_type_data, row, resource_name=self.RESOURCE_NAME)
            statistics.update(click_data)

            stat_obj = AdStatistic(**statistics)
            stat_unique_constraint = (statistics["ad_id"], statistics["date"])
            stat_id = existing_stats_from_min_date.get(stat_unique_constraint)

            if stat_id is not None:
                stat_obj.id = stat_id
                stats_to_update.append(stat_obj)
            else:
                stats_to_create.append(stat_obj)
        Ad.objects.safe_bulk_create(ads_to_create)
        AdStatistic.objects.safe_bulk_create(stats_to_create)
        AdStatistic.objects.bulk_update(stats_to_update, fields=self.UPDATE_FIELDS)
