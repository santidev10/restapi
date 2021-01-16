from email_reports.reports.daily_apex_disney_campaign_report import DailyApexDisneyCampaignEmailReport
from aw_reporting.models import Account
from aw_reporting.models import Ad
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.updaters.ads import AdUpdater
from aw_reporting.adwords_reports import ad_performance_report


class AdTrackingURLTemplateWithoutStatisticsUpdater(AdUpdater):

    def update(self, client):
        """
        Update only Ad objects' `creative_tracking_url_template`, and not AdStatistics.
        :param client:
        :return:
        """
        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        report = ad_performance_report(client, dates=(min_acc_date, max_acc_date))

        updated_ad_ids = []
        for row in report:
            ad_id = int(row.Id)

            if ad_id in updated_ad_ids:
                continue

            updated_ad_ids.append(ad_id)

            Ad.objects.update_or_create(
                id=ad_id, ad_group_id=int(row.AdGroupId),
                defaults={"creative_tracking_url_template": self.get_creative_tracking_url_template(row)}
            )


def update_disney_account_ads_tracking_url_template():
    """
    updates creative_tracking_url_template value for all ads belonging to accounts
    associated with the DailyApexDisneyCampaignEmailReport
    :return:
    """
    # a = Account.objects.get(id="9594358144") # does not have template url dat
    # a = Account.objects.get(id="7853748121") # Disney+ mandalorian

    apex_disney_report = DailyApexDisneyCampaignEmailReport(host="", debug=True)
    account_ids = apex_disney_report.get_account_ids()
    accounts = Account.objects.filter(id__in=account_ids)
    for account in accounts:
        updater = GoogleAdsUpdater(account, (AdTrackingURLTemplateWithoutStatisticsUpdater,))
        updater.update_all_except_campaigns()
