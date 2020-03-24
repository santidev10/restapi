from datetime import datetime
from datetime import timedelta

from django.db.models import Max
import pytz

from aw_reporting.adwords_reports import ad_performance_report
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Ad
from aw_reporting.models import AdStatistic
from aw_reporting.update.adwords_utils import format_click_types_report
from aw_reporting.update.adwords_utils import get_base_stats
from aw_reporting.update.adwords_utils import update_stats_with_click_type_data


class AdUpdater(UpdateMixin):
    RESOURCE_NAME = "ad_group_ad"

    def __init__(self, account):
        self.account = account
        self.today = datetime.now(tz=pytz.timezone(account.timezone)).date()

    def update(self, client):
        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return
        click_type_report_fields = (
            "AdGroupId",
            "Date",
            "Id",
            "Clicks",
            "ClickType",
        )
        report_unique_field_name = "Id"
        stats_queryset = AdStatistic.objects.filter(
            ad__ad_group__campaign__account=self.account)
        self.drop_latest_stats(stats_queryset, self.today)

        saved_max_date = stats_queryset.aggregate(
            max_date=Max("date")).get("max_date")

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) \
                if saved_max_date else min_acc_date
            max_date = max_acc_date

            ad_ids = list(
                Ad.objects.filter(
                    ad_group__campaign__account=self.account
                ).values_list("id", flat=True)
            )

            report = ad_performance_report(
                client,
                dates=(min_date, max_date),
            )
            click_type_report = ad_performance_report(client, dates=(min_date, max_date),
                                                      fields=click_type_report_fields)
            click_type_data = format_click_types_report(click_type_report, report_unique_field_name)
            create_ad = []
            create_stat = []
            updated_ads = []
            for row_obj in report:
                ad_id = int(row_obj.Id)
                # update ads
                if ad_id not in updated_ads:
                    updated_ads.append(ad_id)

                    stats = {
                        "headline": row_obj.Headline,
                        "creative_name": row_obj.ImageCreativeName,
                        "display_url": row_obj.DisplayUrl,
                        "status": row_obj.Status,
                        "is_disapproved": self.is_ad_disapproved(row_obj)
                    }
                    kwargs = {
                        "id": ad_id, "ad_group_id": int(row_obj.AdGroupId)
                    }

                    if ad_id in ad_ids:
                        Ad.objects.filter(**kwargs).update(**stats)
                    else:
                        ad_ids.append(ad_id)
                        stats.update(kwargs)
                        create_ad.append(Ad(**stats))
                # -- update ads
                # insert stats
                stats = {
                    "date": row_obj.Date,
                    "ad_id": ad_id,
                    "average_position": 0,
                }
                stats.update(
                    get_base_stats(row_obj, quartiles=True)
                )
                update_stats_with_click_type_data(stats, click_type_data, row_obj, report_unique_field_name)
                create_stat.append(AdStatistic(**stats))

            if create_ad:
                Ad.objects.safe_bulk_create(create_ad)

            if create_stat:
                AdStatistic.objects.safe_bulk_create(create_stat)

    def is_ad_disapproved(self, campaign_row):
        return campaign_row.CombinedApprovalStatus == "disapproved" \
            if hasattr(campaign_row, "CombinedApprovalStatus") \
            else False
