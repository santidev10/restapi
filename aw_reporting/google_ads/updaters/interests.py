import logging
from datetime import timedelta

from django.db.models import Max

from aw_reporting.adwords_reports import audience_performance_report
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import RemarkList
from aw_reporting.models import Audience
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.update.adwords_utils import DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS
from aw_reporting.update.adwords_utils import DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME
from aw_reporting.update.adwords_utils import format_click_types_report
from aw_reporting.update.adwords_utils import get_base_stats
from aw_reporting.update.adwords_utils import quart_views
from aw_reporting.update.adwords_utils import update_stats_with_click_type_data
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class AudienceAWType:
    REMARK = "boomuserlist"
    USER_VERTICAL = "uservertical"
    CUSTOM_AFFINITY = "customaffinity"
    CUSTOM_INTENT = "custominmarket"


class InterestUpdater(UpdateMixin):
    RESOURCE_NAME = "ad_group_audience_view"

    def __init__(self, account):
        self.account = account
        self.today = now_in_default_tz().date()

    def update(self, client):
        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        audience_stats_queryset = AudienceStatistic.objects.filter(
            ad_group__campaign__account=self.account
        )
        remark_stats_queryset = RemarkStatistic.objects.filter(
            ad_group__campaign__account=self.account
        )
        self.drop_latest_stats(audience_stats_queryset, self.today)
        self.drop_latest_stats(remark_stats_queryset, self.today)

        aud_max_date = audience_stats_queryset.aggregate(
            max_date=Max("date"),
        ).get("max_date")
        remark_max_date = remark_stats_queryset.aggregate(
            max_date=Max("date"),
        ).get("max_date")
        if aud_max_date and remark_max_date:
            saved_max_date = max(aud_max_date, remark_max_date)
        else:
            saved_max_date = aud_max_date or remark_max_date

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) \
                if saved_max_date else min_acc_date
            max_date = max_acc_date

            report = audience_performance_report(
                client, dates=(min_date, max_date))
            click_type_report = audience_performance_report(
                client, dates=(min_date, max_date), fields=DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS)
            click_type_data = format_click_types_report(click_type_report,
                                                        DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
            remark_ids = set(RemarkList.objects.values_list("id", flat=True))
            interest_ids = set(Audience.objects.values_list("id", flat=True))
            bulk_aud_stats = []
            bulk_remarks = []
            bulk_rem_stats = []
            bulk_custom_audiences = []
            for row_obj in report:
                stats = dict(
                    date=row_obj.Date,
                    ad_group_id=int(row_obj.AdGroupId),
                    video_views_25_quartile=quart_views(row_obj, 25),
                    video_views_50_quartile=quart_views(row_obj, 50),
                    video_views_75_quartile=quart_views(row_obj, 75),
                    video_views_100_quartile=quart_views(row_obj, 100),
                    **get_base_stats(row_obj)
                )
                update_stats_with_click_type_data(
                    stats, click_type_data, row_obj, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
                au_type, au_id, *_ = row_obj.Criteria.split("::")
                if au_type == AudienceAWType.REMARK:
                    stats.update(remark_id=au_id)
                    bulk_rem_stats.append(RemarkStatistic(**stats))
                    if au_id not in remark_ids:
                        remark_ids.update({au_id})
                        bulk_remarks.append(
                            RemarkList(id=au_id, name=row_obj.UserListName)
                        )

                elif au_type == AudienceAWType.USER_VERTICAL:
                    if int(au_id) not in interest_ids:
                        logger.warning("Audience %s not found" % au_id)
                        continue

                    stats.update(audience_id=au_id)
                    bulk_aud_stats.append(AudienceStatistic(**stats))

                elif au_type == AudienceAWType.CUSTOM_AFFINITY:
                    if int(au_id) not in interest_ids:
                        interest_ids |= {int(au_id)}
                        bulk_custom_audiences.append(Audience(
                            id=au_id, name=row_obj.Criteria,
                            type=Audience.CUSTOM_AFFINITY_TYPE
                        ))

                    stats.update(audience_id=au_id)
                    bulk_aud_stats.append(AudienceStatistic(**stats))

                elif au_type == AudienceAWType.CUSTOM_INTENT:
                    if int(au_id) not in interest_ids:
                        interest_ids |= {int(au_id)}
                        bulk_custom_audiences.append(Audience(
                            id=au_id, name=row_obj.Criteria,
                            type=Audience.CUSTOM_INTENT_TYPE
                        ))

                    stats.update(audience_id=au_id)
                    bulk_aud_stats.append(AudienceStatistic(**stats))

                else:
                    logger.warning(
                        "Undefined criteria = %s" % row_obj.Criteria)

            if bulk_remarks:
                RemarkList.objects.safe_bulk_create(bulk_remarks)

            if bulk_rem_stats:
                RemarkStatistic.objects.safe_bulk_create(
                    bulk_rem_stats)

            if bulk_custom_audiences:
                Audience.objects.safe_bulk_create(bulk_custom_audiences)

            if bulk_aud_stats:
                AudienceStatistic.objects.safe_bulk_create(
                    bulk_aud_stats)
