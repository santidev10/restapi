from django.core.management.base import BaseCommand
from aw_reporting.utils import command_single_process_lock
from aw_reporting.models import Account, CampaignHourlyStatistic, Campaign
from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.adwords_reports import campaign_performance_report,\
    main_statistics
import pytz
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

HOURS_CLEAR = 5


class Command(BaseCommand):

    @command_single_process_lock("aw_hourly_update")
    def handle(self, *args, **options):
        accounts = Account.objects.filter(can_manage_clients=False)
        logger.info('Total accounts: {}'.format(len(accounts)))

        updater = AWDataLoader(datetime.now().date())
        from time import sleep
        sleep(1000)
        for account in accounts:
            updater.run_task_with_any_manager(
                self.load_hourly_stats, account,
            )

    @staticmethod
    def load_hourly_stats(client, account, *_):
        queryset = CampaignHourlyStatistic.objects.filter(
            campaign__account=account)

        today = datetime.now(tz=pytz.timezone(account.timezone)).date()
        min_date = today - timedelta(days=10)

        # delete very old stats
        queryset.filter(date__lt=min_date).delete()

        last_entry = queryset.order_by('-date', '-hour').first()
        # delete last 3 hours saved data
        hour, date = 0, min_date  # default dummy data
        if last_entry:
            hour = last_entry.hour
            date = last_entry.date
            if hour >= HOURS_CLEAR:
                hour -= HOURS_CLEAR
            else:
                queryset.filter(date=date).delete()
                hour = 24 + hour - HOURS_CLEAR
                date -= timedelta(days=1)
            queryset.filter(date=date, hour__gte=hour).delete()

        #  get report
        report = campaign_performance_report(
            client,
            dates=(date, today),
            fields=[
               'CampaignId', 'CampaignName',
               'Date', 'HourOfDay',
            ] + main_statistics[:4]
        )
        if report:
            campaign_ids = list(
                account.campaigns.values_list('id', flat=True)
            )
            create_campaign = []
            create_stat = []
            for row in report:
                row_date = row.Date
                row_hour = int(row.HourOfDay)
                if row_date == str(date) and row_hour < hour:
                    continue  # this row is already saved

                campaign_id = row.CampaignId
                if campaign_id not in campaign_ids:
                    campaign_ids.append(campaign_id)
                    create_campaign.append(
                        Campaign(
                            id=campaign_id,
                            name=row.CampaignName,
                            account=account,
                            start_date=date,
                        )
                    )

                create_stat.append(
                    CampaignHourlyStatistic(
                        date=row_date,
                        hour=row.HourOfDay,
                        campaign_id=row.CampaignId,
                        video_views=row.VideoViews,
                        impressions=row.Impressions,
                        clicks=row.Clicks,
                        cost=float(row.Cost)/1000000,
                    )
                )
            if create_campaign:
                Campaign.objects.bulk_create(create_campaign)

            if create_stat:
                CampaignHourlyStatistic.objects.bulk_create(create_stat)
