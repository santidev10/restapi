from datetime import timedelta

from aw_reporting.adwords_reports import geo_location_report
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Campaign
from aw_reporting.models import GeoTargeting
from aw_reporting.update.adwords_utils import get_base_stats
from utils.datetime import now_in_default_tz


class CampaignLocationTargetUpdater(UpdateMixin):
    RESOURCE_NAME = "location_view"

    def __init__(self, account):
        self.account = account
        self.today = now_in_default_tz().date()

    def update(self, client):
        saved_targeting = set(
            GeoTargeting.objects.filter(campaign__account=self.account).values_list("campaign_id", "geo_target_id")
        )

        _, max_acc_date = self.get_account_border_dates(self.account)
        yesterday = now_in_default_tz().date() - timedelta(days=1)
        week_ago = yesterday - timedelta(days=7)
        if saved_targeting and (max_acc_date is None or max_acc_date < week_ago):
            # don"t update if there is no data or the data is old, just optimization
            return

        campaign_ids = set(Campaign.objects.filter(account=self.account).values_list("id", flat=True))

        report = geo_location_report(client)
        generator = self._generate_stat_instances(GeoTargeting, report, campaign_ids, saved_targeting)
        GeoTargeting.objects.safe_bulk_create(generator)

    def _generate_stat_instances(self, model, report, campaign_ids, saved_targeting):
        for row_obj in report:
            campaign_id = int(row_obj.CampaignId)
            if campaign_id not in campaign_ids or not row_obj.Id.isnumeric():
                continue
            item_id = int(row_obj.Id)
            uid = (campaign_id, item_id)
            stats = dict(
                is_negative=row_obj.IsNegative == "true",
                **get_base_stats(row_obj)
            )
            if len(row_obj.Id) > 7:  # this is a custom location
                continue
            if uid in saved_targeting:
                model.objects.filter(campaign_id=campaign_id, geo_target_id=item_id).update(**stats)
                continue
            else:
                yield model(campaign_id=campaign_id, geo_target_id=item_id, **stats)
