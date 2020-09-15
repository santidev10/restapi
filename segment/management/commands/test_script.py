from collections import defaultdict
import csv
import logging
import json
import itertools
from multiprocessing import Pool
from datetime import datetime, timedelta
from time import time

from celery import chain
from django.conf import settings
from django.db.models import Q
from django.core.management import BaseCommand
from django.db.models import F
from django.db.models import Avg
from django.db.models import Sum
from django.db.models import FloatField
from django.db.models import ExpressionWrapper
from googleads import adwords
from uuid import uuid4

# from aw_creation.api.serializers.media_buying.account_serializer import AccountSerializerTwo
from aw_creation.models import AccountCreation

from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from ads_analyzer.reports.account_targeting_report.export_report import account_targeting_export
from aw_creation.api.views.media_buying.constants import REPORT_CONFIG

from brand_safety.tasks.channel_discovery import channel_discovery_scheduler
from brand_safety.tasks.video_discovery import video_discovery_scheduler

from aw_reporting.adwords_api import get_all_customers
from aw_reporting.adwords_api import get_customers
from aw_reporting.adwords_api import load_web_app_settings

from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.google_ads_updater import AccountUpdater
from aw_reporting.google_ads.updaters.ad_groups import AdGroupUpdater
from aw_reporting.google_ads.updaters.campaigns import CampaignUpdater
from aw_reporting.google_ads.updaters.ad_group_criteria import AdGroupCriteriaUpdater
from aw_reporting.google_ads.tasks.update_campaigns import setup_update_campaigns
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import Opportunity
from aw_reporting.models import OpPlacement
from aw_reporting.adwords_reports import placement_performance_report, campaign_performance_report
# from aw_reporting.models import FlightPacingAllocation
from aw_reporting.models.ad_words.calculations import get_average_cpm
from aw_reporting.models.ad_words.calculations import get_average_cpv
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.reports.pacing_report import get_pacing_from_flights
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
from aw_reporting.update.recalculate_de_norm_fields import _recalculate_de_norm_fields_for_account_campaigns_and_groups
from aw_reporting.update.update_salesforce_data import update_salesforce_data
from aw_reporting.adwords_api import get_web_app_client
from aw_reporting.google_ads.updaters.ad_group_criteria import AdGroupCriteriaUpdater
# from aw_reporting.adwords_reports import account_performance

from aw_reporting.google_ads.tasks.update_campaigns import cid_campaign_update
# from aw_reporting.tasks import update_opportunities_task
from aw_reporting.google_ads.tasks.update_campaigns import finalize_campaigns_update
from aw_reporting.google_ads.tasks.update_campaigns import setup_cid_update_tasks
from aw_reporting.google_ads.updaters.accounts import AccountUpdater
# from aw_reporting.adwords_reports import audience_performance_report, age_range_performance_report, criteria_performance_report
# from audit_tool.tasks.export_blocklist import export_blocklist_task
from es_components.connections import init_es_connection
from elasticsearch_dsl import connections
from brand_safety.tasks.channel_update import channel_update
from brand_safety.tasks.video_discovery import video_update
from brand_safety.tasks.video_discovery import video_discovery_scheduler
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from brand_safety.tasks.channel_update_helper import channel_update_helper
from brand_safety.tasks.constants import Schedulers

from channel.tasks.export_data import export_channels_data
from cache.models import CacheItem
from cache.tasks import cache_research_videos_defaults
# from dashboard.tasks import update_opportunity_performance_task
# from dashboard.tasks import update_account_performance_task


from es_components.constants import Sections
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.models import Channel
from es_components.models import Video
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.migration import init_mapping

from performiq.tasks.update_campaigns import update_mcc_campaigns_task
from performiq.tasks.update_campaigns import update_campaigns as performiq_campaign
from performiq.tasks.update_campaigns import update_campaigns_task
from performiq.models import OAuthAccount

from segment.models import CustomSegment
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.tasks.generate_vetted_segment import generate_vetted_segment
from segment.segment_list_generator import SegmentListGenerator
from segment.tasks.generate_segment import generate_segment
# from segment.utils.query_builder import SegmentQueryBuilder
from userprofile.models import UserProfile
from utils.utils import chunks_generator
from utils.datetime import now_in_default_tz


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
        self.test_performiq()
        # self.score()
        # self.bs_task()
        # self.bs()
        # cache_research_videos_defaults()
        # a_id = 5453761695
        # update_campaigns_task(a_id, historical=True)
        # performiq_campaign(5453761695)


        pass

    def test_performiq(self):
        oauth_account = OAuthAccount.objects.get(id=6)
        customers = get_customers(
            oauth_account.refresh_token,
            **load_web_app_settings()
        )
        mcc_accounts = []
        cid_accounts = []
        for account in customers:
            if account["canManageClients"] and not account["testAccount"]:
                container = mcc_accounts
            else:
                container = cid_accounts
            container.append(account)
        if mcc_accounts:
            # DO the stuff
            first = mcc_accounts[0]
            update_mcc_campaigns_task(first["customerId"], oauth_account.id)
        elif cid_accounts:
            oauth_account.name = cid_accounts[0]["descriptiveName"]
            oauth_account.save(update_fields=["name"])

    def score(self):
        audit = BrandSafetyAudit()

        # audit.process_videos(["y7d9VLRO3vc"])
        audit.process_channels(["UCEFNzT2RoVqGkV4e3Osyx4A"])

    def bs(self):
        video_manager = VideoManager(["brand_safety", Sections.CUSTOM_PROPERTIES])
        with_rescore = video_manager.forced_filters() & QueryBuilder().build().must().term().field(
            f"{Sections.BRAND_SAFETY}.rescore").value(True).get()
        video_res = video_manager.search(with_rescore).execute()
        print(len(video_res))

        channel_manager = ChannelManager([Sections.BRAND_SAFETY, Sections.CUSTOM_PROPERTIES])
        base_query = channel_manager.forced_filters()
        query_with_rescore = base_query & QueryBuilder().build().must().term().field(
            f"{Sections.BRAND_SAFETY}.rescore").value(True).get()
        res = channel_manager.search(query_with_rescore).execute()
        print(len(res))

    def bs_task(self):
        # video_manager = VideoManager(["brand_safety"])
        # base_query = video_manager.forced_filters()
        # with_rescore = base_query & QueryBuilder().build().must().term().field(
        #     f"{Sections.BRAND_SAFETY}.rescore").value(True).get()
        # items = video_manager.search(with_rescore).execute()



        channel_manager = ChannelManager([Sections.BRAND_SAFETY])
        base_query = channel_manager.forced_filters()
        query_with_rescore = base_query & QueryBuilder().build().must().term().field(
            f"{Sections.BRAND_SAFETY}.rescore").value(True).get()

        channel_update_helper(Schedulers.ChannelDiscovery, query_with_rescore, "")

        # items = channel_manager.search(query_with_rescore).execute()
        pass


    def rescore(self):
        video_manager = VideoManager([Sections.BRAND_SAFETY, Sections.TASK_US_DATA])
        base_query = video_manager.forced_filters()
        with_no_score = base_query & QueryBuilder().build().must_not().exists().field(
            f"{Sections.BRAND_SAFETY}.overall_score").get() \
                        & QueryBuilder().build().must().exists().field("task_us_data").get()
        res = video_manager.search(with_no_score).params(track_total_hits=True).execute()
        ids = [doc.main.id for doc in res]
        print(ids[:25])
        BrandSafetyAudit(ignore_vetted_videos=False).process_videos(ids)

    def gads(self):
        acc = Account.objects.get(id=7368392897)
        updater = GoogleAdsUpdater(acc)
        updater.update_campaigns()

    def report(self):
        start = time()
        client = get_web_app_client(
            refresh_token='1/dFSYu09IZl43oA8pPOLE_NbkSDgO-Wm5LwA_dlkQoWsNoYWpKb856YvPe91IqL9t',
            client_customer_id='5453761695',
        )
        a = Account.objects.get(id=client.client_customer_id)
        AccountUpdater([a.id]).update(client)
        # data = account_performance(client)
        # data = placement_performance_report(client, dates=(dfrom, dto))
        # GoogleAdsUpdater(a).update_campaigns()
        # GoogleAdsUpdater().update_account_performance()
        # data = campaign_performance_report(client)
        end = time()
        print('time took: ', end - start)

    def sf(self):
        update_salesforce_data(do_get=True, do_delete=False, do_update=False)

    def update_google(self):
        cid_campaign_update(1654228384)
        account = Account.objects.get(id=1164085984)
        GoogleAdsUpdater(account).update_all_except_campaigns()
        recalculate_de_norm_fields_for_account(4566376872)

    def dashboard(self):
        account_id = 4427984000
        account = Account.objects.get(id=account_id)
        report = PacingReport()
        today = now_in_default_tz().date()

        flights = report.get_flights_data(placement__opportunity__aw_cid__contains=account.id)
        plan_cost = sum(f["total_cost"] for f in flights if f["start"] <= today)
        actual_cost = Campaign.objects.filter(account=account).aggregate(Sum("cost"))["cost__sum"]
        delivery_stats = report.get_delivery_stats_from_flights(flights)

        pacing = get_pacing_from_flights(flights)
        margin = report.get_margin_from_flights(flights, actual_cost, plan_cost)
        cpv = delivery_stats["cpv"]
        extra_data = {
            "pacing": pacing,
            "margin": margin,
            "cpv": cpv
        }
        print(extra_data)

    def init_mapping(self, model, index):
        model.init(index=index)

    def reindex(self, source, dest):
        init_es_connection()
        connection = connections.get_connection()
        body = {
            "conflicts": "proceed",
            "source": {"index": source},
            "dest": {"index": dest},
        }
        connection.reindex(
            body=body,
            wait_for_completion=False,
        )

    def update_alias(self, old, new, doc_type):
        init_es_connection()
        connection = connections.get_connection()

        actions = []
        actions.append({"remove": {"index": old, "alias": doc_type}})
        actions.append({"add": {"index": new, "alias": doc_type, "is_write_index": True}})
        connection.indices.update_aliases({"actions": actions})
