import logging
from datetime import timedelta

from django.conf import settings
from django.core.mail import EmailMultiAlternatives

from aw_reporting.models import Account
from aw_reporting.models import Opportunity
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.reports.pacing_report import get_pacing_from_flights
from email_reports.reports.base import BaseEmailReport
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class BaseCampaignEmailReport(BaseEmailReport):
    def __init__(self, *args, timezone_name=None, **kwargs):
        super(BaseCampaignEmailReport, self).__init__(*args, **kwargs)

        self.today = now_in_default_tz(tz_str=timezone_name).date()
        self.timezone_name = timezone_name
        self.__timezone_accounts = None

    def send(self):
        raise NotImplementedError

    def timezone_accounts(self):
        if self.__timezone_accounts is None and self.timezone_name is not None:
            self.__timezone_accounts = list(
                Account.objects.filter(timezone=self.timezone_name).values_list("id", flat=True).distinct()
            )
        return self.__timezone_accounts


class BaseCampaignPacingEmailReport(BaseCampaignEmailReport):
    _problem_str = None

    def __init__(self, *args, **kwargs):
        super(BaseCampaignPacingEmailReport, self).__init__(*args, **kwargs)
        self.pacing_bound = kwargs.get("pacing_bound", .25)
        self.days_to_end = kwargs.get("pacing_bound", 3)
        self.date_end = self.today + timedelta(days=self.days_to_end)

    def _is_risky_pacing(self, pacing):
        raise NotImplementedError

    def send(self):
        opportunities = Opportunity.objects.filter(
            probability=100,
            end__gte=self.date_end,
        )
        if self.timezone_accounts() is not None:
            opportunities = opportunities.filter(aw_cid__in=self.timezone_accounts())
        messages = dict()

        for opp in opportunities:
            if not opp.ad_ops_manager:
                continue

            ad_ops_manager = (opp.ad_ops_manager.name, opp.ad_ops_manager.email,)
            risky_flights = self._get_risky_flights(opp)
            messages[ad_ops_manager] = messages.get(ad_ops_manager, []) + \
                                       self.get_flight_descriptions(risky_flights, self.date_end)

        for ad_ops_manager, _messages in messages.items():
            self._send_for_flights(ad_ops_manager, _messages)

    def _send_for_flights(self, ad_ops_manager, messages):
        if len(messages) == 0:
            return

        name, email = ad_ops_manager

        msg = EmailMultiAlternatives(
            self._get_subject(name),
            self._build_body(name, messages),
            from_email=settings.EXPORTS_EMAIL_ADDRESS,
            to=self.get_to([email]),
            cc=self.get_cc(settings.CF_AD_OPS_DIRECTORS),
            bcc=self.get_bcc(),
            headers={"X-Priority": 2},
            reply_to="",
        )

        msg.send(fail_silently=False)

    def _get_subject(self, ad_ops_manager_name):
        return f"{ad_ops_manager_name} Opportunities {self._problem_str.upper()} Pacing Report"

    def _get_risky_flights(self, opportunity):
        pacing_report = PacingReport(self.today)
        flights_data = pacing_report.get_flights_data(
            placement__opportunity_id=opportunity.id,
            end=self.date_end,
        )
        flights_with_pacing = [
            dict(
                pacing=get_pacing_from_flights([flight]),
                flight=flight
            ) for flight in flights_data
        ]
        flights_with_pacing = [
            r for r in flights_with_pacing if r.get("pacing")
        ]
        attention_flights = filter(
            lambda item: self._is_risky_pacing(item["pacing"]),
            flights_with_pacing
        )
        return list(attention_flights)

    def _build_body(self, ad_ops_manager_name, messages):

        body_header = f"Hi {ad_ops_manager_name},"
        main_body = "\n".join(messages) \
                    + "\nPlease check and adjust IMMEDIATELY."
        return "\n\n".join([
            body_header,
            main_body,
            "Best,",
            "Channel Factory ViewIQ Team"
        ])

    def get_flight_descriptions(self, flights_with_pacing, date_end):
        return [
            self._flight_description(item["flight"], item["pacing"], date_end)
            for item in flights_with_pacing
        ]

    def _flight_description(self, flight, pacing, date_end):
        return "The flight {flight_name} is {problem} pacing" \
               " by {pacing_diff_percent:.2f}%" \
               " and ends on {flight_end_date:%B %d}." \
            .format(flight_name=flight["name"],
                    problem=self._problem_str.lower(),
                    pacing_diff_percent=abs(1. - pacing) * 100,
                    flight_end_date=date_end)
