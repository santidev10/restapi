from datetime import datetime
from datetime import timedelta
from functools import partial

from django.contrib.auth import get_user_model
from django.core import mail
from django.core.management import call_command
from django.test import override_settings
from django.utils import timezone
from lxml import etree

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import User
from aw_reporting.models import UserRole
from email_reports.models import SavedEmail
from email_reports.reports.daily_campaign_report import OpportunityManager
from utils.utils_tests import ExtendedAPITestCase as APITestCase
from utils.utils_tests import patch_now


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
        Campaign.objects.create(pk="1", name="",
                                salesforce_placement=placement)

        call_command("send_daily_email_reports", reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].subject, "Daily Update for Opportunity")
        self.assertEqual(len(mail.outbox[0].to), 1)
        self.assertEqual(mail.outbox[0].to[0], ad_ops.email)

        self.assertEqual(SavedEmail.objects.count(), 1)

    def test_do_not_send_un_subscribed(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        get_user_model().objects.create(
            email=ad_ops.email,
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
        campaign = Campaign.objects.create(pk="1", name="",
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
        placement = OpPlacement.objects.create(opportunity=opportunity)
        account = Account.objects.create(id="12341")
        Campaign.objects.create(salesforce_placement=placement, account=account)
        test_host = "https://host.test"
        expected_account_link = "{host}/analytics/managed_service/{account_id}/?should_redirect=true" \
            .format(host=test_host, account_id=account.id)

        with patch_now(now), override_settings(HOST=test_host):
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
        account_link_nodes = tree.xpath("//a[@id='accountLink']")
        self.assertEqual(len(view_in_browser_link_nodes), 1)
        self.assertEqual(len(account_link_nodes), 1)
        view_in_browser_link = view_in_browser_link_nodes[0].get("href")
        account_link = account_link_nodes[0].get("href")
        self.assertEqual(view_in_browser_link, browser_link)
        self.assertEqual(account_link, expected_account_link)

    def test_no_link_if_no_account(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        now = datetime(2017, 1, 1)
        today = now.date()
        SavedEmail.objects.all().delete()

        Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )

        with patch_now(now):
            call_command("send_daily_email_reports",
                         reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        html_body = email.alternatives[0][0]
        tree = etree.HTML(html_body)
        account_link_nodes = tree.xpath("//a[@id='accountLink']")
        self.assertEqual(len(account_link_nodes), 0)

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

    def test_adwords_spend_bar_width(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        goal_items_factor = 1.02
        ordered_views = 1000
        goal_views = ordered_views * goal_items_factor
        test_cost_1, test_views_1 = 123, 12
        test_cost_2, test_views_2 = 1240, 130
        days_left = 3
        views_left = goal_views - sum([test_views_1, test_views_2])
        today_goal = views_left / days_left
        yesterday_cpv = test_cost_2 / test_views_2
        today_cost = today_goal * yesterday_cpv
        max_value = max(today_cost, test_cost_1, test_cost_2)
        width_style_with_max = partial(width_style, max_value)
        width_1 = width_style_with_max(test_cost_1)
        width_2 = width_style_with_max(test_cost_2)
        width_3 = width_style_with_max(today_cost)
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
        campaign = Campaign.objects.create(pk="1", name="",
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

        get_width = partial(get_style_value, tree, "width")

        before_yesterday_spend_width = get_width(
            "//div[@id='adwordsSpendBeforeYesterday']")
        yesterday_spend_width = get_width(
            "//div[@id='adwordsSpendYesterday']")
        today_spend_width = get_width("//div[@id='adwordsSpendToday']")

        self.assertEqual(before_yesterday_spend_width, width_1)
        self.assertEqual(yesterday_spend_width, width_2)
        self.assertEqual(today_spend_width, width_3)

    def test_campaign_views_bar_width(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        goal_items_factor = 1.02
        ordered_views = 1000
        goal_views = ordered_views * goal_items_factor
        test_cost_1, test_views_1 = 123, 12
        test_cost_2, test_views_2 = 1240, 130
        days_left = 3
        views_left = goal_views - sum([test_views_1, test_views_2])
        today_goal = views_left / days_left
        max_value = max(today_goal, test_views_1, test_views_2)
        width_style_with_max = partial(width_style, max_value)
        width_1 = width_style_with_max(test_views_1)
        width_2 = width_style_with_max(test_views_2)
        width_3 = width_style_with_max(today_goal)
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
        campaign = Campaign.objects.create(pk="1", name="",
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

        get_width = partial(get_style_value, tree, "width")

        before_yesterday_views_width = get_width(
            "//div[@id='opportunityViewsBeforeYesterday']")
        yesterday_views_width = get_width(
            "//div[@id='opportunityViewsYesterday']")
        today_views_width = get_width("//div[@id='opportunityViewsToday']")
        self.assertEqual(before_yesterday_views_width, width_1)
        self.assertEqual(yesterday_views_width, width_2)
        self.assertEqual(today_views_width, width_3)

    def test_campaign_impressions_bar_width(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        goal_items_factor = 1.02
        ordered_impressions = 1000
        goal_views = ordered_impressions * goal_items_factor
        test_cost_1, test_impressions_1 = 123, 12
        test_cost_2, test_impressions_2 = 1240, 130
        days_left = 3
        views_left = goal_views - sum([test_impressions_1, test_impressions_2])
        today_goal = views_left / days_left
        max_value = max(today_goal, test_impressions_1, test_impressions_2)
        width_style_with_max = partial(width_style, max_value)
        width_1 = width_style_with_max(test_impressions_1)
        width_2 = width_style_with_max(test_impressions_2)
        width_3 = width_style_with_max(today_goal)
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
            goal_type_id=SalesForceGoalType.CPM,
        )
        Flight.objects.create(id="1", name="", placement=placement,
                              start=today - timedelta(days=10),
                              end=today + timedelta(days=days_left - 1),
                              ordered_units=ordered_impressions)
        campaign = Campaign.objects.create(pk="1", name="",
                                           salesforce_placement=placement)

        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=2),
                                         impressions=test_impressions_1,
                                         cost=test_cost_1)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=1),
                                         impressions=test_impressions_2,
                                         cost=test_cost_2)

        with patch_now(now):
            call_command("send_daily_email_reports",
                         reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        html_body = email.alternatives[0][0]
        tree = etree.HTML(html_body)

        get_width = partial(get_style_value, tree, "width")

        before_yesterday_impressions_width = get_width(
            "//div[@id='opportunityImpressionsBeforeYesterday']")
        yesterday_impressions_width = get_width(
            "//div[@id='opportunityImpressionsYesterday']")
        today_impressions_width = get_width(
            "//div[@id='opportunityImpressionsToday']")
        self.assertEqual(before_yesterday_impressions_width, width_1)
        self.assertEqual(yesterday_impressions_width, width_2)
        self.assertEqual(today_impressions_width, width_3)

    def test_send_report_only_to_account_manager(self):
        now = datetime(2017, 1, 15)
        am_role = UserRole.objects.create(id="1",
                                          name=UserRole.ACCOUNT_MANAGER_NAME)
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz",
                                     role=am_role)
        am = User.objects.create(id="2", name="Paul", email="2@mail.cz",
                                 role=am_role)
        Opportunity.objects.create(
            id="1",
            ad_ops_manager=ad_ops,
            account_manager=am,
            start=now - timedelta(days=3),
            end=now + timedelta(days=2),
            probability=100)
        with patch_now(now):
            call_command("send_daily_email_reports",
                         reports="DailyCampaignReport",
                         roles=OpportunityManager.ACCOUNT_MANAGER)
        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        self.assertEqual(email.to, [am.email])

    def test_receivers_no_sales(self):
        ad_ops = User.objects.create(id=1, email="AdOps@channelfactory.com")
        sm = User.objects.create(id=2, email="SM@channelfactory.com")

        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            sales_manager=sm,
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
        Campaign.objects.create(pk="1", name="",
                                salesforce_placement=placement)

        call_command("send_daily_email_reports", reports="DailyCampaignReport")

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        receivers = email.to + email.cc + email.bcc
        receivers_mails = (r[1] if isinstance(r, tuple) else r
                           for r in receivers)
        self.assertNotIn(sm.email, receivers_mails)

def get_xpath_text(tree, xpath):
    node = tree.xpath(xpath)[0]
    return "".join(node.itertext()).strip()


def get_own_styles(tree, xpath):
    node = tree.xpath(xpath)[0]
    styles = [split_and_strip(v, ":") for v in node.get("style").split(";")]
    styles = filter(lambda pair: len(pair) == 2, styles)
    return dict(styles)


def split_and_strip(string: str, divider: str):
    return tuple(s.strip() for s in string.split(divider))


def get_style_value(tree, key, xpath):
    return get_own_styles(tree, xpath).get(key)


def width_style(max_value, value):
    return str(round(value * 100 / max_value)) + "%"
