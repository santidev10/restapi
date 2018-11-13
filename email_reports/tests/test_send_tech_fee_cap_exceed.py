from datetime import timedelta

from django.conf import settings
from django.core import mail
from django.core.management import call_command
from django.utils import timezone

from aw_reporting.models import User, Opportunity, \
    OpPlacement, Flight, Campaign, Account
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase


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

        call_command("send_daily_email_reports", reports="TechFeeCapExceeded")

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

        call_command("send_daily_email_reports", reports="TechFeeCapExceeded")

        self.assertEqual(len(mail.outbox), 1)
        message = mail.outbox[0]
        self.assertEqual(message.subject, "Tech Fee Cap Exceeded")
        expected_body = "Paul,\n<ExceedOpportunity> is exceeding its tech fee" \
                        " cap of 0.0700 in the <ExceedPlacement> placement." \
                        " Please adjust immediately."
        self.assertEqual(message.body, expected_body)
        self.assertEqual(message.to, [ad_ops.email])
        self.assertEqual(message.cc, [settings.CF_AD_OPS_DIRECTORS[0],
                                      account_manager.email])

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

        call_command("send_daily_email_reports", reports="TechFeeCapExceeded")

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

        call_command("send_daily_email_reports", reports="TechFeeCapExceeded")

        self.assertEqual(len(mail.outbox), 1)
        message = mail.outbox[0]
        self.assertEqual(message.subject, "Tech Fee Cap Exceeded")
        expected_body = "Paul,\n<ExceedOpportunity> is exceeding its tech fee" \
                        " cap of 7.0000 in the <ExceedPlacement> placement." \
                        " Please adjust immediately."
        self.assertEqual(message.body, expected_body)
        self.assertEqual(message.to, [ad_ops.email])
        self.assertEqual(message.cc, [settings.CF_AD_OPS_DIRECTORS[0],
                                      account_manager.email])

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

        call_command("send_daily_email_reports", reports="TechFeeCapExceeded")

        self.assertEqual(len(mail.outbox), 1)
        message = mail.outbox[0]
        self.assertEqual(message.subject, "Tech Fee Cap Exceeded")
        expected_body = "Paul,\n<ExceedOpportunity> is exceeding its tech fee" \
                        " cap of 0.0700 in the <ExceedPlacement> placement." \
                        " Please adjust immediately."
        self.assertEqual(message.body, expected_body)
        self.assertEqual(message.to, [ad_ops.email])
        self.assertEqual(set(message.cc), {settings.CF_AD_OPS_DIRECTORS[0],
                                           account_manager.email})
