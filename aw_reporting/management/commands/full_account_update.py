import logging

from django.core.management import BaseCommand

from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.models import Account

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--cids",
            help="Comma separated CID account ids to update"
        )
        parser.add_argument(
            "--mccs",
            help="Comma separated MCC account ids to update",
        )

    def handle(self, *args, **options):
        cid_ids = (options["cids"] or "").split(",")
        mcc_ids = (options["mccs"] or "").split(",")

        mcc_updater = GoogleAdsUpdater(None)
        for mcc in Account.objects.filter(id__in=mcc_ids):
            mcc_updater.update_accounts_as_mcc(mcc_account=mcc)

        cid_updater = GoogleAdsUpdater(None)
        for cid in cid_ids:
            account = Account.objects.get(id=cid)
            cid_updater.account = account
            cid_updater.full_update()

        add_relation_between_report_and_creation_campaigns()
        add_relation_between_report_and_creation_ad_groups()
        add_relation_between_report_and_creation_ads()
