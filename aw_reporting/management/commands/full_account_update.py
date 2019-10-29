import logging

from django.core.management import BaseCommand

from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_reporting.models import Account
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--ids",
            help="Comma separated CID account ids to update"
        )
        parser.add_argument(
            "--mcc",
            help="Comma separated MCC account ids to update",
            default="3386233102"
        )

    def handle(self, *args, **options):
        cid_ids = options["ids"].split(",")
        mcc_ids = options["mcc"].split(",")
        for mcc in Account.objects.filter(id__in=mcc_ids):
            GoogleAdsUpdater().update_accounts_for_mcc(mcc_account=mcc)
        for _id in cid_ids:
            account = Account.objects.get(id=_id)
            mcc_accounts = account.managers.all()
            for mcc in mcc_accounts:
                try:
                    updater = GoogleAdsUpdater()
                    updater.full_update(account, any_permission=True)
                except Exception as e:
                    logger.error(f"Exception while executing full_account_update for CID: {_id}. {e}")
                    continue

        add_relation_between_report_and_creation_campaigns()
        add_relation_between_report_and_creation_ad_groups()
        add_relation_between_report_and_creation_ads()
