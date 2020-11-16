from collections import namedtuple
from collections import defaultdict

from django.core.management import BaseCommand

from performiq.models import IQCampaign
from performiq.models import OAuthAccount
from performiq.api.serializers.query_serializer import IQCampaignQuerySerializer
from performiq.tasks.start_analysis import start_analysis_task
from performiq.tasks.utils.get_dv360_data import get_dv360_data
from performiq.tasks.dv360.sync_dv_records import sync_dv_partners
from performiq.tasks.dv360.sync_dv_records import sync_dv_advertisers
from performiq.tasks.dv360.sync_dv_records import sync_dv_campaigns


class Command(BaseCommand):
    def handle(self, *args, **options):

        # oauth_account_id = 7

        # sync_dv_partners(sync_advertisers=True)
        # sync_dv_campaigns()
        # update_campaigns_task(5)
        # analyze(1)

        iq_campaign_id = 2 # dv360
        # iq_campaign_id = 1 # campaign.id = 11530553742
        # get_dv360_data(IQCampaign.objects.get(id=iq_campaign_id), oauth_account_id=7)

        # analyze(iq_campaign_id)
        # get_dv360_data(dv360_campaign_id, oauth_account_id=oauth_account_id)
        start_analysis_task(9)

    def test_params(self):
        data = {
            "average_cpm": 1.0,
            "average_cpv": 0.4,
            "score_threshold": 1,
            "content_categories": []
        }
        s = IQCampaignQuerySerializer(data=data)
        s.is_valid(raise_exception=True)
        formatted_params = s.validated_data
        print(formatted_params)
        return