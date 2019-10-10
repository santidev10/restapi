from django.core.management import BaseCommand

from aw_reporting.models import Account, Campaign
from aw_reporting.google_ads.updaters.campaigns import CampaignUpdater
from aw_reporting.google_ads.google_ads_api import get_client
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.updaters.ad_groups import AdGroupUpdater
from aw_reporting.google_ads.updaters.ads import AdUpdater


class Command(BaseCommand):
    def handle(self, *args, **options):
        client = get_client()
        # account = Account.objects.get(name="Lego Cadreon Denmark Technic Q3-Q4'19 OP003878")
        account = Account.objects.get(id=3779438711)
        updater = GoogleAdsUpdater()
        updater.full_update(account, client)
        # CampaignUpdater(account).update(client)
        # print('updating ad gorup')
        # AdGroupUpdater(account).update(client)
        # print('updating ads')
        # AdUpdater(account).update(client)


