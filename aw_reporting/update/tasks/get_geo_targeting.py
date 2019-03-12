from datetime import timedelta

from aw_reporting.update.tasks.utils.get_account_border_dates import get_account_border_dates
from aw_reporting.update.tasks.utils.get_base_stats import get_base_stats
from utils.datetime import now_in_default_tz


def _generate_stat_instances(model, report, campaign_ids, saved_targeting):
    for row_obj in report:
        if row_obj.CampaignId not in campaign_ids or not row_obj.Id.isnumeric():
            continue
        uid = (row_obj.CampaignId, int(row_obj.Id))
        stats = dict(
            is_negative=row_obj.IsNegative == "true",
            **get_base_stats(row_obj)
        )
        if len(row_obj.Id) > 7:  # this is a custom location
            continue
        if uid in saved_targeting:
            model.objects.filter(campaign_id=row_obj.CampaignId, geo_target_id=row_obj.Id).update(**stats)
            continue
        else:
            yield model(campaign_id=row_obj.CampaignId, geo_target_id=row_obj.Id, **stats)


def get_geo_targeting(ad_client, account, *_):
    from aw_reporting.models import Campaign
    from aw_reporting.models import GeoTargeting
    from aw_reporting.adwords_reports import geo_location_report

    saved_targeting = set(
        GeoTargeting.objects.filter(campaign__account=account).values_list("campaign_id", "geo_target_id")
    )

    _, max_acc_date = get_account_border_dates(account)
    yesterday = now_in_default_tz().date() - timedelta(days=1)
    week_ago = yesterday - timedelta(days=7)
    if saved_targeting and (max_acc_date is None or max_acc_date < week_ago):
        # don"t update if there is no data or the data is old, just optimization
        return

    campaign_ids = set(Campaign.objects.filter(account=account).values_list("id", flat=True))

    report = geo_location_report(ad_client)
    generator = _generate_stat_instances(GeoTargeting, report, campaign_ids, saved_targeting)
    GeoTargeting.objects.safe_bulk_create(generator)
