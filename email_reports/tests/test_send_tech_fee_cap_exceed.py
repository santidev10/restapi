from datetime import timedelta

from django.conf import settings
from django.core import mail
from django.utils import timezone

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import User
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from email_reports.tasks import send_daily_email_reports
from utils.datetime import now_in_default_tz
from utils.unittests.patch_now import patch_now
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase


class SendDailyEmailsTestCase(APITestCase):

    def test_do_not_send_cpv(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        account_manager = User.objects.create(id="2", name="Mirage",
                                              email="2@mail.be")
        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="solo", name="<ExceedOpportunity>", ad_ops_manager=ad_ops,
            account_manager=account_manager,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id="1", name="<ExceedPlacement>",
            start=opportunity.start, end=opportunity.end,
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee_cap=0.07, tech_fee_type=OpPlacement.TECH_FEE_CPV_TYPE,
        )
        Flight.objects.create(id="1", name="", placement=placement,
                              start=today.replace(day=1),
                              end=today.replace(day=28),
                              ordered_units=1000)
        account = Account.objects.create(pk="1", name="")
        stats = dict(video_views=100, cost=7)
        Campaign.objects.create(pk="1", name="", account=account,
                                salesforce_placement=placement, **stats)

        send_daily_email_reports(reports=["TechFeeCapExceeded"], debug=False)

        self.assertEqual(len(mail.outbox), 0)

    def test_send_cpv(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        account_manager = User.objects.create(id="2", name="Mirage",
                                              email="2@mail.be")
        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="solo", name="<ExceedOpportunity>", ad_ops_manager=ad_ops,
            account_manager=account_manager,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id="1", name="<ExceedPlacement>",
            start=opportunity.start, end=opportunity.end,
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee_cap=0.07, tech_fee_type=OpPlacement.TECH_FEE_CPV_TYPE,
        )
        Flight.objects.create(id="1", name="", placement=placement,
                              start=today.replace(day=1),
                              end=today.replace(day=28),
                              ordered_units=1000)
        account = Account.objects.create(pk="1", name="")
        stats = dict(video_views=100, cost=7.1)
        Campaign.objects.create(pk="1", name="", account=account,
                                salesforce_placement=placement, **stats)

        send_daily_email_reports(reports=["TechFeeCapExceeded"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        message = mail.outbox[0]
        self.assertEqual(message.from_email, settings.EXPORTS_EMAIL_ADDRESS)
        self.assertEqual(message.subject, "Tech Fee Cap Exceeded")
        expected_body = "Paul,\n<ExceedOpportunity> is exceeding its tech fee" \
                        " cap of 0.0700 in the <ExceedPlacement> placement." \
                        " Please adjust immediately."
        self.assertEqual(message.body, expected_body)
        self.assertEqual(message.to, [ad_ops.email])
        self.assertEqual(set(message.cc), set(settings.CF_AD_OPS_DIRECTORS + [account_manager.email]))

    def test_do_not_send_cpm(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        account_manager = User.objects.create(id="2", name="Mirage",
                                              email="2@mail.be")
        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="solo", name="<ExceedOpportunity>", ad_ops_manager=ad_ops,
            account_manager=account_manager,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id="1", name="<ExceedPlacement>",
            start=opportunity.start, end=opportunity.end,
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee_cap=7, tech_fee_type=OpPlacement.TECH_FEE_CPM_TYPE,
        )
        Flight.objects.create(id="1", name="", placement=placement,
                              start=today.replace(day=1),
                              end=today.replace(day=28),
                              ordered_units=1000)
        account = Account.objects.create(pk="1", name="")
        stats = dict(impressions=1000, cost=7)
        Campaign.objects.create(pk="1", name="", account=account,
                                salesforce_placement=placement, **stats)

        send_daily_email_reports(reports=["TechFeeCapExceeded"], debug=False)

        self.assertEqual(len(mail.outbox), 0)

    def test_send_cpm(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        account_manager = User.objects.create(id="2", name="Mirage",
                                              email="2@mail.be")
        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="solo", name="<ExceedOpportunity>", ad_ops_manager=ad_ops,
            account_manager=account_manager,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id="1", name="<ExceedPlacement>",
            start=opportunity.start, end=opportunity.end,
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee_cap=7, tech_fee_type=OpPlacement.TECH_FEE_CPM_TYPE,
        )
        Flight.objects.create(id="1", name="", placement=placement,
                              start=today.replace(day=1),
                              end=today.replace(day=28), ordered_units=1000)
        account = Account.objects.create(pk="1", name="")
        stats = dict(impressions=1000, cost=7.1)
        Campaign.objects.create(pk="1", name="", account=account,
                                salesforce_placement=placement, **stats)

        send_daily_email_reports(reports=["TechFeeCapExceeded"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        message = mail.outbox[0]
        self.assertEqual(message.from_email, settings.EXPORTS_EMAIL_ADDRESS)
        self.assertEqual(message.subject, "Tech Fee Cap Exceeded")
        expected_body = "Paul,\n<ExceedOpportunity> is exceeding its tech fee" \
                        " cap of 7.0000 in the <ExceedPlacement> placement." \
                        " Please adjust immediately."
        self.assertEqual(message.body, expected_body)
        self.assertEqual(message.to, [ad_ops.email])
        self.assertEqual(set(message.cc), set([account_manager.email] + settings.CF_AD_OPS_DIRECTORS))

    def test_receivers_no_sales(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        account_manager = User.objects.create(id="2", name="Mirage",
                                              email="2@mail.be")
        sm = User.objects.create(id="3", name="David", email="3@mail.com")

        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="solo", name="<ExceedOpportunity>",
            ad_ops_manager=ad_ops,
            account_manager=account_manager,
            sales_manager=sm,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id="1", name="<ExceedPlacement>",
            start=opportunity.start, end=opportunity.end,
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee_cap=0.07, tech_fee_type=OpPlacement.TECH_FEE_CPV_TYPE,
        )
        Flight.objects.create(id="1", name="", placement=placement,
                              start=today.replace(day=1),
                              end=today.replace(day=28),
                              ordered_units=1000)
        account = Account.objects.create(pk="1", name="")
        stats = dict(video_views=100, cost=7.1)
        Campaign.objects.create(pk="1", name="", account=account,
                                salesforce_placement=placement, **stats)

        send_daily_email_reports(reports=["TechFeeCapExceeded"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        message = mail.outbox[0]
        self.assertEqual(message.from_email, settings.EXPORTS_EMAIL_ADDRESS)
        self.assertEqual(message.subject, "Tech Fee Cap Exceeded")
        expected_body = "Paul,\n<ExceedOpportunity> is exceeding its tech fee" \
                        " cap of 0.0700 in the <ExceedPlacement> placement." \
                        " Please adjust immediately."
        self.assertEqual(message.body, expected_body)
        self.assertEqual(message.to, [ad_ops.email])
        self.assertEqual(set(message.cc), set([account_manager.email] + settings.CF_AD_OPS_DIRECTORS))

    def test_send_to_account_by_timezone(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        account_manager = User.objects.create(id="2", name="Mirage",
                                              email="2@mail.be")
        sm = User.objects.create(id="3", name="David", email="3@mail.com")

        now = now_in_default_tz()
        today = now.date()

        account = Account.objects.create(id=1, timezone=settings.DEFAULT_TIMEZONE)
        opportunity = Opportunity.objects.create(
            id=1, name="<ExceedOpportunity>",
            ad_ops_manager=ad_ops,
            account_manager=account_manager,
            sales_manager=sm,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
            aw_cid=account.id
        )
        placement = OpPlacement.objects.create(
            id=1, name="<ExceedPlacement>",
            start=opportunity.start, end=opportunity.end,
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee_cap=0.07, tech_fee_type=OpPlacement.TECH_FEE_CPV_TYPE,
        )
        Flight.objects.create(id=1, name="", placement=placement,
                              start=today.replace(day=1),
                              end=today.replace(day=28),
                              ordered_units=1000)
        stats = dict(video_views=100, cost=7.1)
        Campaign.objects.create(pk=1, name="", account=account,
                                salesforce_placement=placement, **stats)

        now_manila_timezone = now_in_default_tz(tz_str="Asia/Manila")
        today_manila_timezone = now_manila_timezone.date()

        account_2 = Account.objects.create(id=2, timezone="Asia/Manila")
        opportunity_2 = Opportunity.objects.create(
            id=2, name="<ExceedOpportunity>",
            ad_ops_manager=ad_ops,
            account_manager=account_manager,
            sales_manager=sm,
            start=today_manila_timezone - timedelta(days=2),
            end=today_manila_timezone + timedelta(days=2),
            probability=100,
            aw_cid=account_2.id
        )
        placement_2 = OpPlacement.objects.create(
            id=2, name="<ExceedPlacement>",
            start=opportunity_2.start, end=opportunity_2.end,
            opportunity=opportunity_2,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee_cap=0.07, tech_fee_type=OpPlacement.TECH_FEE_CPV_TYPE,
        )
        Flight.objects.create(id=2, name="", placement=placement_2,
                              start=today_manila_timezone.replace(day=1),
                              end=today_manila_timezone.replace(day=28),
                              ordered_units=1000)
        stats = dict(video_views=100, cost=7.1)
        Campaign.objects.create(pk=2, name="", account=account_2,
                                salesforce_placement=placement_2, **stats)

        with patch_now(now_in_default_tz(tz_str="America/Vancouver")):
            # check if no mails will be sent for non existence timezone
            send_daily_email_reports(reports=["TechFeeCapExceeded"], debug=False, timezone_name="America/Vancouver")
            self.assertEqual(len(mail.outbox), 0)

        with patch_now(now):
            send_daily_email_reports(reports=["TechFeeCapExceeded"], debug=False,
                                     timezone_name=settings.DEFAULT_TIMEZONE)

        self.assertEqual(len(mail.outbox), 1)
        message = mail.outbox[0]
        self.assertEqual(message.from_email, settings.EXPORTS_EMAIL_ADDRESS)
        self.assertEqual(message.subject, "Tech Fee Cap Exceeded")
        expected_body = "Paul,\n<ExceedOpportunity> is exceeding its tech fee" \
                        " cap of 0.0700 in the <ExceedPlacement> placement." \
                        " Please adjust immediately."
        self.assertEqual(message.body, expected_body)
        self.assertEqual(message.to, [ad_ops.email])
        self.assertEqual(set(message.cc), set([account_manager.email] + settings.CF_AD_OPS_DIRECTORS))
