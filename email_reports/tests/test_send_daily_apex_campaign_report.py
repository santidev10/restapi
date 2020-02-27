from datetime import datetime
from datetime import timedelta
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core import mail

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import AdGroup
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models import Video
from email_reports.tasks import send_daily_email_reports
from email_reports.reports.daily_apex_campaign_report import DATE_FORMAT
from email_reports.reports.daily_apex_campaign_report import YOUTUBE_LINK_TEMPLATE
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase
from utils.unittests.patch_now import patch_now
from userprofile.constants import UserSettingsKey

TEST_DAILY_APEX_REPORT_EMAIL_ADDRESSES = ["test@test.test", "test2@test.test"]

TEST_CAMP_ID = "12345"

class SendDailyApexCampaignEmailsTestCase(APITestCase):

    def create_campaign(self, account, today):
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id="1",
            name="Placement",
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=2.5
        )
        campaign = Campaign.objects.create(
            id=TEST_CAMP_ID, account=account, name="campaign_1",
            salesforce_placement=placement
        )
        return campaign


    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR",
           "1@mail.cz")
    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES",
           TEST_DAILY_APEX_REPORT_EMAIL_ADDRESSES)
    def test_send_email(self):
        user = get_user_model().objects.create(id="1", username="Paul", email="1@mail.cz")
        user.aw_settings.update(**{
            UserSettingsKey.VISIBLE_ACCOUNTS: ["1"]
        })
        user.save()

        now = datetime(2017, 1, 1)
        today = now.date()
        yesterday = today - timedelta(days=1)
        account = Account.objects.create(id="1", name="account_1", currency_code="USD")

        campaign = self.create_campaign(account, today)
        CampaignStatistic.objects.create(date=yesterday, campaign=campaign, video_views=102,
                                         video_views_100_quartile=50, video_views_50_quartile=100)
        CampaignStatistic.objects.create(date=today, campaign=campaign, video_views=102,
                                         video_views_100_quartile=50, video_views_50_quartile=100)

        ad_group = AdGroup.objects.create(id=1, campaign=campaign)

        creative = VideoCreative.objects.create(id=1)

        video = Video("1")
        video.populate_general_data(title="video_creative_1")
        VideoManager(Sections.GENERAL_DATA).upsert([video])

        VideoCreativeStatistic.objects.create(date=yesterday, ad_group=ad_group, creative=creative, video_views=102,
                                         video_views_100_quartile=50, video_views_50_quartile=100)
        VideoCreativeStatistic.objects.create(date=today, ad_group=ad_group, creative=creative, video_views=102,
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
        self.assertEqual(csv_context.count(TEST_CAMP_ID), 2)
        self.assertEqual(csv_context.count('video_creative_1'), 1)
        self.assertEqual(csv_context.count(YOUTUBE_LINK_TEMPLATE.format(video.main.id)), 1)
        self.assertEqual(csv_context.count(yesterday.strftime(DATE_FORMAT)), 2)
        self.assertEqual(csv_context.count(today.strftime(DATE_FORMAT)), 0)

    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR",
           "1@mail.cz")
    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES",
           TEST_DAILY_APEX_REPORT_EMAIL_ADDRESSES)
    def test_send_email_selected_account(self):
        account = Account.objects.create(id="1", name="account_1", currency_code="USD")

        apex_user = get_user_model().objects.create(id="1", username="Paul", email="1@mail.cz")
        apex_user.aw_settings.update(**{
            UserSettingsKey.VISIBLE_ACCOUNTS: ["1"]
        })
        apex_user.save()

        now = datetime(2017, 1, 1)
        today = now.date()
        yesterday = today - timedelta(days=1)

        campaign = self.create_campaign(account, today)
        CampaignStatistic.objects.create(date=yesterday, campaign=campaign, video_views=102,
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
        self.assertEqual(csv_context.count(TEST_CAMP_ID), 1)
        self.assertEqual(csv_context.count(yesterday.strftime(DATE_FORMAT)), 1)

    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR",
           "1@mail.cz")
    @patch("email_reports.reports.daily_apex_campaign_report.settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES",
           TEST_DAILY_APEX_REPORT_EMAIL_ADDRESSES)
    def test_send_email_without_selected_account(self):
        account = Account.objects.create(id="1", name="account_1", currency_code="USD")
        apex_user = get_user_model().objects.create(id="1", username="Paul", email="1@mail.cz")
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

        with patch_now(now):
            send_daily_email_reports(reports=["DailyApexCampaignEmailReport"], debug=False)
        self.assertEqual(len(mail.outbox), 0)
