import logging
from datetime import timedelta

from django.conf import settings
from django.core.mail import EmailMultiAlternatives

from aw_reporting.models import Opportunity
from aw_reporting.reports.pacing_report import PacingReport
from email_reports.reports.base_campaign_pacing_report import BaseCampaignEmailReport

logger = logging.getLogger(__name__)


class CampaignUnderMargin(BaseCampaignEmailReport):

    def __init__(self, *args, **kwargs):
        super(CampaignUnderMargin, self).__init__(*args, **kwargs)

        self.margin_bound = .1
        if self.kwargs.get('margin_bound') is not None:
            self.margin_bound = kwargs.get('margin_bound')

        self.days_to_end = 7
        if self.kwargs.get('days_to_end') is not None:
            self.days_to_end = kwargs.get('days_to_end')

    def send(self):
        opportunities = Opportunity.objects.filter(
            probability=100,
            end__gte=self.today,  # If a campaign is ending within 7 days
            end__lte=self.today + timedelta(days=self.days_to_end - 1),
        ).values("id", "name", "ad_ops_manager__email", "ad_ops_manager__name")

        if self.timezone_accounts() is not None:
            opportunities = opportunities.filter(aw_cid__in=self.timezone_accounts())

        messages = dict()

        pacing_report = PacingReport(self.today)
        for opp in opportunities:
            flights_data = pacing_report.get_flights_data(
                placement__opportunity_id=opp["id"])
            plan_stats = pacing_report.get_plan_stats_from_flights(flights_data)
            margin = pacing_report.get_margin_from_flights(
                flights_data, plan_stats["cost"], plan_stats["plan_cost"])

            if margin is not None and margin < self.margin_bound:
                ad_ops_manager = (opp['ad_ops_manager__name'], opp["ad_ops_manager__email"])

                messages[ad_ops_manager] = messages.get(ad_ops_manager, "") + \
                                           "{} is under margin at {:.2f}%.\n".format(opp["name"], margin * 100)

        for ad_ops_manager, message in messages.items():
            name, to_email = ad_ops_manager

            msg = EmailMultiAlternatives(
                f"{name} Opportunities Under Margin Report",
                message + "Please adjust IMMEDIATELY.",
                from_email=settings.EXPORTS_EMAIL_ADDRESS,
                to=self.get_to([to_email]),
                cc=self.get_cc(settings.CF_AD_OPS_DIRECTORS),
                bcc=self.get_bcc(),
                headers={'X-Priority': 2},
                reply_to="",
            )

            msg.send(fail_silently=False)
