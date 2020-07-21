from datetime import datetime
from datetime import timedelta
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core import mail

from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from email_reports.reports.daily_apex_campaign_report import DATE_FORMAT
from email_reports.reports.daily_apex_campaign_report import YOUTUBE_LINK_TEMPLATE
from email_reports.tasks import send_daily_email_reports
from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models import Video
from userprofile.constants import UserSettingsKey
from utils.unittests.int_iterator import int_iterator
from utils.unittests.patch_now import patch_now
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase

TEST_DAILY_APEX_REPORT_EMAIL_ADDRESSES = ["test@test.test", "test2@test.test"]

TEST_CAMP_ID = 12345
TEST_ACCOUNT_NAME = "account_1"

TEST_APEX_CAMPAIGN_NAME_SUBSTITUTIONS = {
    TEST_ACCOUNT_NAME: f"{TEST_ACCOUNT_NAME}_substitutions"
}


class SendDailyApexCampaignEmailsTestCase(APITestCase):

    def create_campaign(self, account, today, ias_campaign_name=None):
        opportunity_id = next(int_iterator)
        opportunity = Opportunity.objects.create(
            id=opportunity_id, name=f"Opportunity_{opportunity_id}",
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
            ias_campaign_name=ias_campaign_name,
        )
        placement_id = next(int_iterator)
        placement = OpPlacement.objects.create(
            id=placement_id,
            name=f"Placement_{placement_id}",
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=2.5
        )
        campaign_id = next(int_iterator)
        campaign = Campaign.objects.create(
            id=campaign_id, account=account, name=f"campaign_{campaign_id}",
            salesforce_placement=placement
        )
        return campaign

    # pylint: disable=(too-many-statements
    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR",
           "1@mail.cz")
    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES",
           TEST_DAILY_APEX_REPORT_EMAIL_ADDRESSES)
    @patch("email_reports.reports.daily_apex_campaign_report.settings.APEX_CAMPAIGN_NAME_SUBSTITUTIONS",
           TEST_APEX_CAMPAIGN_NAME_SUBSTITUTIONS)
    def test_send_email(self):
        user = get_user_model().objects.create(id=1, username="Paul", email="1@mail.cz")
        user.aw_settings.update(**{
            UserSettingsKey.VISIBLE_ACCOUNTS: [1]
        })
        user.save()

        now = datetime(2017, 1, 1)
        today = now.date()
        yesterday = today - timedelta(days=1)
        account = Account.objects.create(id=1, name=TEST_ACCOUNT_NAME, currency_code="USD")

        campaign_1 = self.create_campaign(account, today)
        ias_campaign_name = "ias campaign 2"
        campaign_1_id = campaign_1.id
        campaign_2 = self.create_campaign(account, today, ias_campaign_name=ias_campaign_name)
        campaign_2_id = campaign_2.id

        CampaignStatistic.objects.create(date=yesterday, campaign=campaign_1, video_views=102,
                                         video_views_100_quartile=50, video_views_50_quartile=100)
        CampaignStatistic.objects.create(date=today, campaign=campaign_1, video_views=102,
                                         video_views_100_quartile=50, video_views_50_quartile=100)
        CampaignStatistic.objects.create(date=yesterday, campaign=campaign_2, video_views=102,
                                         video_views_100_quartile=50, video_views_50_quartile=100)
        CampaignStatistic.objects.create(date=today, campaign=campaign_2, video_views=102,
                                         video_views_100_quartile=50, video_views_50_quartile=100)

        ad_group_1 = AdGroup.objects.create(id=1, campaign=campaign_1)
        ad_group_2 = AdGroup.objects.create(id=2, campaign=campaign_1)
        ad_group_3 = AdGroup.objects.create(id=3, campaign=campaign_2)
        ad_group_4 = AdGroup.objects.create(id=4, campaign=campaign_2)

        creative = VideoCreative.objects.create(id=1)

        video = Video("1")
        video_title = "video_creative_1"
        video.populate_general_data(title=video_title)
        VideoManager(Sections.GENERAL_DATA).upsert([video])

        VideoCreativeStatistic.objects.create(date=yesterday, ad_group=ad_group_1, creative=creative, video_views=102,
                                              video_views_100_quartile=50, video_views_50_quartile=100)
        VideoCreativeStatistic.objects.create(date=today, ad_group=ad_group_1, creative=creative, video_views=102,
                                              video_views_100_quartile=50, video_views_50_quartile=100)
        VideoCreativeStatistic.objects.create(date=yesterday, ad_group=ad_group_2, creative=creative, video_views=25,
                                              video_views_100_quartile=5, video_views_50_quartile=20)
        VideoCreativeStatistic.objects.create(date=today, ad_group=ad_group_2, creative=creative, video_views=30,
                                              video_views_100_quartile=15, video_views_50_quartile=25)
        VideoCreativeStatistic.objects.create(date=yesterday, ad_group=ad_group_3, creative=creative, video_views=102,
                                              video_views_100_quartile=50, video_views_50_quartile=100)
        VideoCreativeStatistic.objects.create(date=today, ad_group=ad_group_3, creative=creative, video_views=102,
                                              video_views_100_quartile=50, video_views_50_quartile=100)
        VideoCreativeStatistic.objects.create(date=yesterday, ad_group=ad_group_4, creative=creative, video_views=25,
                                              video_views_100_quartile=5, video_views_50_quartile=20)
        VideoCreativeStatistic.objects.create(date=today, ad_group=ad_group_4, creative=creative, video_views=30,
                                              video_views_100_quartile=15, video_views_50_quartile=25)

        with patch_now(now):
            send_daily_email_reports(reports=["DailyApexCampaignEmailReport"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].subject, f"Daily Campaign Report for {yesterday}")
        self.assertEqual(len(mail.outbox[0].to), 2)
        self.assertEqual(mail.outbox[0].to, TEST_DAILY_APEX_REPORT_EMAIL_ADDRESSES)
        self.assertEqual(mail.outbox[0].from_email, settings.EXPORTS_EMAIL_ADDRESS)

        attachment = mail.outbox[0].attachments
        self.assertEqual(attachment[0][0], "daily_campaign_report.csv")

        csv_context = attachment[0][1]
        self.assertEqual(csv_context.count(str(campaign_1_id)), 1)
        self.assertEqual(csv_context.count(str(campaign_2_id)), 1)
        self.assertEqual(csv_context.count(video_title), 2)
        self.assertEqual(csv_context.count(TEST_APEX_CAMPAIGN_NAME_SUBSTITUTIONS.get(TEST_ACCOUNT_NAME)), 1)
        self.assertEqual(csv_context.count(ias_campaign_name), 1)
        self.assertEqual(csv_context.count(YOUTUBE_LINK_TEMPLATE.format(video.main.id)), 2)
        self.assertEqual(csv_context.count(yesterday.strftime(DATE_FORMAT)), 2)
        self.assertEqual(csv_context.count(today.strftime(DATE_FORMAT)), 0)
    # pylint: enable=(too-many-statements

    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR",
           "1@mail.cz")
    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES",
           TEST_DAILY_APEX_REPORT_EMAIL_ADDRESSES)
    @patch("email_reports.reports.daily_apex_campaign_report.settings.APEX_CAMPAIGN_NAME_SUBSTITUTIONS",
           TEST_APEX_CAMPAIGN_NAME_SUBSTITUTIONS)
    def test_send_email_selected_account(self):
        account = Account.objects.create(id=1, name=TEST_ACCOUNT_NAME, currency_code="USD")

        apex_user = get_user_model().objects.create(id=1, username="Paul", email="1@mail.cz")
        apex_user.aw_settings.update(**{
            UserSettingsKey.VISIBLE_ACCOUNTS: [1]
        })
        apex_user.save()

        now = datetime(2017, 1, 1)
        today = now.date()
        yesterday = today - timedelta(days=1)

        ias_campaign_name = "ias campaign name"
        campaign = self.create_campaign(account, today, ias_campaign_name=ias_campaign_name)
        campaign_id = campaign.id
        CampaignStatistic.objects.create(date=yesterday, campaign=campaign, video_views=102,
                                         video_views_100_quartile=50, video_views_50_quartile=100)

        ad_group_1 = AdGroup.objects.create(id=1, campaign=campaign)
        creative = VideoCreative.objects.create(id=1)
        video = Video("1")
        video_title = "video_creative_1"
        video.populate_general_data(title=video_title)
        VideoManager(Sections.GENERAL_DATA).upsert([video])
        VideoCreativeStatistic.objects.create(date=yesterday, ad_group=ad_group_1, creative=creative, video_views=102,
                                              video_views_100_quartile=50, video_views_50_quartile=100)
        VideoCreativeStatistic.objects.create(date=today, ad_group=ad_group_1, creative=creative, video_views=102,
                                              video_views_100_quartile=50, video_views_50_quartile=100)
        with patch_now(now):
            send_daily_email_reports(reports=["DailyApexCampaignEmailReport"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].subject, f"Daily Campaign Report for {yesterday}")
        self.assertEqual(len(mail.outbox[0].to), 2)
        self.assertEqual(mail.outbox[0].to, TEST_DAILY_APEX_REPORT_EMAIL_ADDRESSES)
        self.assertEqual(mail.outbox[0].from_email, settings.EXPORTS_EMAIL_ADDRESS)

        attachment = mail.outbox[0].attachments
        self.assertEqual(attachment[0][0], "daily_campaign_report.csv")

        csv_context = attachment[0][1]
        self.assertEqual(csv_context.count(str(campaign_id)), 1)
        self.assertEqual(csv_context.count(ias_campaign_name), 1)
        self.assertEqual(csv_context.count(TEST_APEX_CAMPAIGN_NAME_SUBSTITUTIONS.get(TEST_ACCOUNT_NAME)), 0)
        self.assertEqual(csv_context.count(yesterday.strftime(DATE_FORMAT)), 1)

    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR",
           "1@mail.cz")
    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES",
           TEST_DAILY_APEX_REPORT_EMAIL_ADDRESSES)
    @patch("email_reports.reports.daily_apex_campaign_report.settings.APEX_CAMPAIGN_NAME_SUBSTITUTIONS",
           TEST_APEX_CAMPAIGN_NAME_SUBSTITUTIONS)
    def test_send_email_without_selected_account(self):
        account = Account.objects.create(id=1, name=TEST_ACCOUNT_NAME, currency_code="USD")
        apex_user = get_user_model().objects.create(id=1, username="Paul", email="1@mail.cz")
        apex_user.aw_settings.update(**{
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
        })
        apex_user.save()

        now = datetime(2017, 1, 1)
        today = now.date()
        yesterday = today - timedelta(days=1)

        campaign = self.create_campaign(account, today)
        CampaignStatistic.objects.create(date=yesterday, campaign=campaign, video_views=102,
                                         video_views_100_quartile=50, video_views_50_quartile=100)

        ad_group_1 = AdGroup.objects.create(id=1, campaign=campaign)
        creative = VideoCreative.objects.create(id=1)
        video = Video("1")
        video_title = "video_creative_1"
        video.populate_general_data(title=video_title)
        VideoManager(Sections.GENERAL_DATA).upsert([video])
        VideoCreativeStatistic.objects.create(date=yesterday, ad_group=ad_group_1, creative=creative, video_views=102,
                                              video_views_100_quartile=50, video_views_50_quartile=100)
        VideoCreativeStatistic.objects.create(date=today, ad_group=ad_group_1, creative=creative, video_views=102,
                                              video_views_100_quartile=50, video_views_50_quartile=100)
        with patch_now(now):
            send_daily_email_reports(reports=["DailyApexCampaignEmailReport"], debug=False)
        self.assertEqual(len(mail.outbox), 0)
