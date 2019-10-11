from django.core.management import BaseCommand

from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_reporting.models import Account
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--ids",
            help="Comma separated CID account ids to update"
        )

    def handle(self, *args, **options):
        cid_ids = options["ids"].split(",")
        for _id in cid_ids:
            account = Account.objects.get(id=_id)
            mcc_accounts = account.managers.all()
            for mcc in mcc_accounts:
                try:
                    updater = GoogleAdsUpdater(mcc_account=mcc)
                    updater.full_update(account, any_permission=True)
                except Exception:
                    continue

        add_relation_between_report_and_creation_campaigns()
        add_relation_between_report_and_creation_ad_groups()
        add_relation_between_report_and_creation_ads()
