from collections import defaultdict
import csv
import logging
import json
from multiprocessing import Pool
from time import time
from datetime import datetime, timedelta

from django.conf import settings
from django.core.management import BaseCommand
from django.db.models import F
from django.db.models import FloatField
from django.db.models import ExpressionWrapper
from uuid import uuid4

# from aw_creation.api.serializers.media_buying.account_serializer import AccountSerializerTwo
from aw_creation.models import AccountCreation

from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from ads_analyzer.reports.account_targeting_report.export_report import account_targeting_export
from aw_creation.api.views.media_buying.constants import REPORT_CONFIG

from brand_safety.tasks.channel_discovery import channel_discovery_scheduler
from brand_safety.tasks.video_discovery import video_discovery_scheduler

from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.updaters.ad_groups import AdGroupUpdater
from aw_reporting.google_ads.updaters.campaigns import CampaignUpdater
from aw_reporting.google_ads.updaters.ad_group_criteria import AdGroupCriteriaUpdater
from aw_reporting.google_ads.tasks.update_campaigns import setup_update_campaigns
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.adwords_reports import placement_performance_report, campaign_performance_report
from aw_reporting.models import FlightPacingAllocation
from aw_reporting.models.ad_words.calculations import get_average_cpm
from aw_reporting.models.ad_words.calculations import get_average_cpv
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
from aw_reporting.update.update_salesforce_data import update_salesforce_data
from aw_reporting.adwords_api import get_web_app_client
from aw_reporting.google_ads.updaters.ad_group_criteria import AdGroupCriteriaUpdater
from aw_reporting.google_ads.tasks.update_campaigns import cid_campaign_update
# from aw_reporting.adwords_reports import audience_performance_report, age_range_performance_report, criteria_performance_report

from brand_safety.tasks.channel_update import channel_update
from brand_safety.tasks.video_discovery import video_update
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit

from es_components.constants import Sections
from es_components.models import Channel
from es_components.models import Video
from es_components.managers import ChannelManager
from es_components.query_builder import QueryBuilder

from segment.models import CustomSegment
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.tasks.generate_vetted_segment import generate_vetted_segment
from segment.segment_list_generator import SegmentListGenerator
# from segment.tasks.generate_with_source_list import generate_with_source
from segment.tasks.generate_segment import generate_segment
# from ads_analyzer.models.utils.update_ad_group_targeting_statistics import AdTargetingStatisticsUpdater
# from ads_analyzer.api.serializers.ad_group_targeting_serializer import AdGroupTargetingSerializer
# from ads_analyzer.models import AdGroupTargeting
# from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from ads_analyzer.reports.opportunity_targeting_report.create_report import create_opportunity_targeting_report
import ads_analyzer.reports.account_targeting_report.constants as names
from aw_reporting.models import CriteriaTypeEnum
from userprofile.models import UserProfile
from utils.utils import chunks_generator

logger = logging.getLogger(__name__)

# Must be top level for mp to work
def score(ids):
    print(ids)
    BrandSafetyAudit(ignore_vetted_channels=False).process_channels(ids)


# UC16zsWYcZvWLU1-V_rm1Gxw
class Command(BaseCommand):
    def handle(self, *args, **options):
        # if "prod" in settings.DATABASES["default"]["HOST"]:
        #     # raise ValueError("IN PRODUCTION. YOU SURE?")
        #     confirm = input("In PRODUCTION. Type 'CONFIRM' to continue: ")
        #     if confirm != "CONFIRM":
        #         raise ValueError("Exiting...")
        # self._create_p_segment()
        # self.report()
        self.bs()
        # self.rescore()
        # self.mapping()

        # self.custom()
        # self.sf()


    def report(self):
        dfrom = datetime.now() - timedelta(days=10)
        dto = datetime.now()
        start = time()
        client = get_web_app_client(
            refresh_token='1/dFSYu09IZl43oA8pPOLE_NbkSDgO-Wm5LwA_dlkQoWsNoYWpKb856YvPe91IqL9t',
            client_customer_id='6364880784',
        )
        data = placement_performance_report(client, dates=(dfrom, dto))
        # data = campaign_performance_report(client, dates=(dfrom, dto), additional_fields=("Device", "Date"))
        # data = campaign_performance_report(client, dates=(dfrom, dto), fields=["CampaignId", "CampaignName", "StartDate", "EndDate",
        #             "AdvertisingChannelType", "Amount", "CampaignStatus",
        #             "ServingStatus", "Date", "HourOfDay", "VideoViews", "Cost", "Clicks", "Impressions"
        #             ])
        print(len(data))
        end = time()
        print('time took: ', end - start)

    def _create_p_segment(self):
        SegmentListGenerator(0).run()

    def bs(self):
        # video_discovery_scheduler()
        # channel_discovery_scheduler()
        BrandSafetyAudit(ignore_vetted_videos=False).process_videos(["2Ux8ZThspZk"])


    def custom(self):
        campaigns = Campaign.objects.filter(id__in=[10506207096])
        all_updated_campaign_budgets = defaultdict(dict)
        for campaign in campaigns:
            account_id = campaign.account.id
            all_updated_campaign_budgets[account_id][campaign.id] = campaign.budget
        pass

    def sf(self):
        update_salesforce_data(do_get=True, do_delete=False, do_update=False)

    def update_google(self):
        cid_campaign_update(1654228384)
        account = Account.objects.get(id=1164085984)
        GoogleAdsUpdater(account).update_all_except_campaigns()
        recalculate_de_norm_fields_for_account(4566376872)

    def rescore(self):
        m = ChannelManager(sections=[Sections.BRAND_SAFETY])
        # c = m.search(m.forced_filters(), sort=("-stats.subscribers",)).execute()
        q = QueryBuilder().build().must().exists().field("task_us_data").get() \
            & QueryBuilder().build().must_not().exists().field("brand_safety.overall_score").get() \
            & QueryBuilder().build().must_not().exists().field("deleted").get()
        c = m.search(q, sort=("-stats.subscribers",)).execute()
        ids = [
            doc.main.id for doc in c if getattr(doc.brand_safety, "overall_score", None) is None
                                        and doc.main.id != 'UCsA_vkmuyIRlYYXeJueyIJQ'
        ]
        args = [list(arg) for arg in chunks_generator(ids, size=10)]
        with Pool(8) as pool:
            try:
                pool.map(score, args)
            except Exception as e:
                print(e)
