from datetime import timedelta, datetime
from functools import partial

from django.contrib.auth import get_user_model
from django.core import mail
from django.core.management import call_command
from django.utils import timezone
from lxml import etree

from aw_reporting.models import SalesForceGoalType, User, Opportunity, \
    OpPlacement, Flight, Account, Campaign, CampaignStatistic
from email_reports.models import SavedEmail
from utils.utils_tests import ExtendedAPITestCase as APITestCase, \
    patch_settings, patch_now


class SendDailyEmailsTestCase(APITestCase):
    def test_send_minimum_email(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
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
        )
        Flight.objects.create(id="1", name="", placement=placement,
                              start=today.replace(day=1),
                              end=today.replace(day=28), ordered_units=1000)
        account = Account.objects.create(pk="1", name="")
        Campaign.objects.create(pk="1", name="", account=account,
                                salesforce_placement=placement)

        call_command("send_daily_email_reports", reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].subject, "Daily Update for Opportunity")
        self.assertEqual(len(mail.outbox[0].to), 1)
        self.assertEqual(mail.outbox[0].to[0], ad_ops.email)

        self.assertEqual(SavedEmail.objects.count(), 1)

    def test_do_not_send_un_subscribed(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        get_user_model().objects.create(email=ad_ops.email,
                                        is_subscribed_to_campaign_notifications=False)
        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
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
        )
        Flight.objects.create(id="1", name="", placement=placement,
                              start=today.replace(day=1),
                              end=today.replace(day=28), ordered_units=1000)
        account = Account.objects.create(pk="1", name="")
        Campaign.objects.create(pk="1", name="", account=account,
                                salesforce_placement=placement)

        call_command("send_daily_email_reports", reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 0)
        self.assertEqual(SavedEmail.objects.count(), 0)

    def test_adwords_spend(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        goal_items_factor = 1.02
        ordered_views = 1000
        goal_views = ordered_views * goal_items_factor
        test_cost_1, test_views_1 = 123, 12
        test_cost_2, test_views_2 = 124, 13
        days_left = 3
        views_left = goal_views - sum([test_views_1, test_views_2])
        today_goal = views_left / days_left
        yesterday_cpv = test_cost_2 / test_views_2
        today_cost = today_goal * yesterday_cpv
        now = datetime(2017, 1, 1)
        today = now.date()
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
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
        )
        Flight.objects.create(id="1", name="", placement=placement,
                              start=today - timedelta(days=10),
                              end=today + timedelta(days=days_left - 1),
                              ordered_units=ordered_views)
        account = Account.objects.create(pk="1", name="")
        campaign = Campaign.objects.create(pk="1", name="", account=account,
                                           salesforce_placement=placement)

        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=2),
                                         video_views=test_views_1,
                                         cost=test_cost_1)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=1),
                                         video_views=test_views_2,
                                         cost=test_cost_2)

        with patch_now(now):
            call_command("send_daily_email_reports",
                         reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        html_body = email.alternatives[0][0]
        tree = etree.HTML(html_body)

        get_value = partial(get_xpath_text, tree)

        before_yesterday_spend = get_value(
            "//div[@id='adwordsSpendBeforeYesterday']")
        yesterday_spend = get_value("//div[@id='adwordsSpendYesterday']")
        today_spend = get_value("//div[@id='adwordsSpendToday']")

        before_yesterday_views = get_value(
            "//div[@id='opportunityViewsBeforeYesterday']")
        yesterday_views = get_value("//div[@id='opportunityViewsYesterday']")
        today_views = get_value("//div[@id='opportunityViewsToday']")
        self.assertEqual(before_yesterday_spend, "${:1.2f}".format(test_cost_1))
        self.assertEqual(yesterday_spend, "${:1.2f}".format(test_cost_2))
        self.assertEqual(today_spend, "${:1.2f}".format(today_cost))

        self.assertEqual(before_yesterday_views, "{:1.0f}".format(test_views_1))
        self.assertEqual(yesterday_views, "{:1.0f}".format(test_views_2))
        self.assertEqual(today_views, "{:1.0f}".format(today_goal))

    def test_valid_urls(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        now = datetime(2017, 1, 1)
        today = now.date()
        SavedEmail.objects.all().delete()

        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        test_host = "https://host.test"
        opportunity_report_link = "{host}/reports/opportunities/{opp_id}" \
            .format(host=test_host, opp_id=opportunity.id)

        with patch_now(now), patch_settings(HOST=test_host):
            call_command("send_daily_email_reports",
                         reports="DailyCampaignReport")

        email_id = SavedEmail.objects.all().first().id
        browser_link = "{host}/api/v1/email_report_web_view/{report_id}/" \
            .format(host=test_host, report_id=email_id)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        html_body = email.alternatives[0][0]
        tree = etree.HTML(html_body)
        view_in_browser_link_nodes = tree.xpath("//a[@id='viewInBrowserLink']")
        opportunity_link_nodes = tree.xpath("//a[@id='opportunityLink']")
        self.assertEqual(len(view_in_browser_link_nodes), 1)
        self.assertEqual(len(opportunity_link_nodes), 1)
        view_in_browser_link = view_in_browser_link_nodes[0].get("href")
        opportunity_link = opportunity_link_nodes[0].get("href")
        self.assertEqual(view_in_browser_link, browser_link)
        self.assertEqual(opportunity_link, opportunity_report_link)

    def test_delivered_units_title_cpv(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        now = datetime(2017, 1, 1)
        today = now.date()
        SavedEmail.objects.all().delete()

        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        OpPlacement.objects.create(opportunity=opportunity,
                                   goal_type_id=SalesForceGoalType.CPV)

        with patch_now(now):
            call_command("send_daily_email_reports",
                         reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        html_body = email.alternatives[0][0]
        tree = etree.HTML(html_body)
        self.assertEqual(len(tree.xpath("//tr[@id='cpvUnits']")), 1)
        self.assertEqual(len(tree.xpath("//tr[@id='cpmUnits']")), 0)
        delivered_units_title = get_xpath_text(
            tree, "//tr[@id='cpvUnits']//p[@class='title']")
        self.assertEqual(delivered_units_title, "Campaign Views")

    def test_delivered_units_title_cpm(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        now = datetime(2017, 1, 1)
        today = now.date()
        SavedEmail.objects.all().delete()

        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        OpPlacement.objects.create(opportunity=opportunity,
                                   goal_type_id=SalesForceGoalType.CPM)

        with patch_now(now):
            call_command("send_daily_email_reports",
                         reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        html_body = email.alternatives[0][0]
        tree = etree.HTML(html_body)
        self.assertEqual(len(tree.xpath("//tr[@id='cpvUnits']")), 0)
        self.assertEqual(len(tree.xpath("//tr[@id='cpmUnits']")), 1)
        delivered_units_title = get_xpath_text(
            tree, "//tr[@id='cpmUnits']//p[@class='title']")
        self.assertEqual(delivered_units_title, "Campaign Impressions")

    def test_delivered_units_title_cpv_and_cpm(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        now = datetime(2017, 1, 1)
        today = now.date()
        SavedEmail.objects.all().delete()

        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        OpPlacement.objects.create(id="1",
                                   opportunity=opportunity,
                                   goal_type_id=SalesForceGoalType.CPV)
        OpPlacement.objects.create(id="2",
                                   opportunity=opportunity,
                                   goal_type_id=SalesForceGoalType.CPM)

        with patch_now(now):
            call_command("send_daily_email_reports",
                         reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        html_body = email.alternatives[0][0]
        tree = etree.HTML(html_body)
        self.assertEqual(len(tree.xpath("//tr[@id='cpvUnits']")), 1)
        self.assertEqual(len(tree.xpath("//tr[@id='cpmUnits']")), 1)

    def test_delivered_units_title_hard_cost(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        now = datetime(2017, 1, 1)
        today = now.date()
        SavedEmail.objects.all().delete()

        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        OpPlacement.objects.create(opportunity=opportunity,
                                   goal_type_id=SalesForceGoalType.HARD_COST)

        with patch_now(now):
            call_command("send_daily_email_reports",
                         reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        html_body = email.alternatives[0][0]
        tree = etree.HTML(html_body)
        self.assertEqual(len(tree.xpath("//tr[@id='cpvUnits']")), 0)
        self.assertEqual(len(tree.xpath("//tr[@id='cpmUnits']")), 0)


def get_xpath_text(tree, xpath):
    node = tree.xpath(xpath)[0]
    return "".join(node.itertext()).strip()
