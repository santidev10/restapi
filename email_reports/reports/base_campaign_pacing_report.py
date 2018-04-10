from datetime import timedelta

from django.conf import settings
from django.core.mail import EmailMultiAlternatives

from aw_reporting.models import Opportunity
from aw_reporting.reports.pacing_report import PacingReport
from email_reports.reports.base import BaseEmailReport
from utils.datetime import now_in_default_tz


class BaseCampaignPacingEmailReport(BaseEmailReport):
    _problem_str = None

    def __init__(self, *args, **kwargs):
        super(BaseCampaignPacingEmailReport, self).__init__(*args, **kwargs)
        self.pacing_bound = kwargs.get("pacing_bound", .1)
        self.days_to_end = kwargs.get("pacing_bound", 3)

    def _is_risky_pacing(self, pacing):
        raise NotImplementedError

    def send(self):
        today = now_in_default_tz().date()
        date_end = today + timedelta(days=self.days_to_end - 1)
        opportunities = Opportunity.objects.filter(
            probability=100,
            end__gte=today,
            end__lte=date_end,
        )

        for opp in opportunities:
            risky_flights = self._get_risky_flights(opp)
            self._send_for_flights(opp, risky_flights, date_end)

    def _send_for_flights(self, opportunity, flights_with_pacing, date_end):
        if len(flights_with_pacing) == 0:
            return

        msg = EmailMultiAlternatives(
            self._get_subject(opportunity),
            self._build_body(opportunity, flights_with_pacing, date_end),
            settings.EMAIL_HOST_USER,
            to=self._get_to(opportunity),
            cc=self._get_cc(opportunity),
            bcc=self.get_bcc(),
            headers={'X-Priority': 2}
        )
        msg.send(fail_silently=False)

    def _get_subject(self, opportunity):
        return "FLIGHT {problem} PACING for {opportunity}".format(
            opportunity=opportunity.name,
            problem=self._problem_str.upper()
        )

    def _get_risky_flights(self, opportunity):
        pacing_report = PacingReport()
        flights_data = pacing_report.get_flights_data(
            placement__opportunity_id=opportunity.id)
        flights_with_pacing = [
            dict(
                pacing=pacing_report.get_pacing_from_flights([flight]),
                flight=flight)
            for flight in flights_data
        ]
        attention_flights = filter(
            lambda item: self._is_risky_pacing(item["pacing"]),
            flights_with_pacing
        )
        return list(attention_flights)

    def _get_to(self, opportunity):
        to_recipients = []
        if opportunity.ad_ops_manager.email:
            to_recipients.append(opportunity.ad_ops_manager.email)
        return self.get_to(to_recipients)

    def _get_cc(self, opportunity):
        cc = [(u.name, u.email)
              for u in [opportunity.sales_manager, opportunity.account_manager]
              if u is not None]
        return self.get_cc(cc + settings.CF_AD_OPS_DIRECTORS)

    def _build_body(self, opportunity, flights_with_pacing, date_end):
        flight_descriptions = [
            self._flight_description(item["flight"], item["pacing"], date_end)
            for item in flights_with_pacing
        ]
        body_header = "Hi {ms_campaign_manager},".format(
            ms_campaign_manager=opportunity.ad_ops_manager.name
            if opportunity.ad_ops_manager else ""
        )
        main_body = "\n".join(flight_descriptions) \
                    + "\nPlease check and adjust IMMEDIATELY."
        return "\n\n".join([
            body_header,
            main_body,
            "Best,",
            "Channel Factory IQ Team"
        ])

    def _flight_description(self, flight, pacing, date_end):
        return "The flight {flight_name} is {problem} pacing" \
               " by {pacing_diff_percent:.2f}%" \
               " and ends on {flight_end_date:%B %d}." \
            .format(flight_name=flight["name"],
                    problem=self._problem_str.lower(),
                    pacing_diff_percent=abs(1. - pacing) * 100,
                    flight_end_date=date_end)
