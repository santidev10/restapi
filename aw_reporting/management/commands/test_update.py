from django.core.management import BaseCommand

from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.google_ads.google_ads_api import get_client
from aw_reporting.models import Account
from aw_reporting.google_ads.updaters.age_range import AgeRangeUpdater
from aw_reporting.google_ads.updaters.campaigns import CampaignUpdater
from aw_reporting.google_ads.updaters.genders import GenderUpdater
from aw_reporting.google_ads.updaters.parents import ParentUpdater

class Command(BaseCommand):

    def handle(self, *args, **options):
        client = get_client()
        account = Account.objects.get(id=2220218798)
        # updater = AgeRangeUpdater(account)
        # updater.update(client)
        #
        # CampaignUpdater(account).update(client)
        # GenderUpdater(account).update(client)
        ParentUpdater(account).update(client)
