import hashlib

from datetime import timedelta

from django.conf import settings
from django.core.mail import EmailMessage
from django.db.models import Count

from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatus
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from email_reports.reports.base_campaign_pacing_report import BaseCampaignEmailReport
from email_reports.models import SavedEmail
from es_components.utils import safe_div


class FlightDeliveredReport(BaseCampaignEmailReport):

    def __init__(self, *args, **kwargs):
        super(FlightDeliveredReport, self).__init__(*args, **kwargs)

        self.yesterday = self.today - timedelta(days=1)
        self.before_yesterday = self.yesterday - timedelta(days=1)

    def is_on_going_flight(self, f):
        return None not in (f.get("start"), f.get("end")) \
               and f["start"] <= self.today \
               and f["end"] >= self.before_yesterday

    def send(self):
        report = PacingReport(self.today)
        opportunities = Opportunity.objects.filter(
            probability=100,
            start__lte=self.today,
            end__gte=self.today
        ).select_related("ad_ops_manager").prefetch_related("placements")

        if self.timezone_accounts() is not None:
            opportunities = opportunities.filter(aw_cid__in=self.timezone_accounts())

        opportunities = opportunities.annotate(campaigns=Count("placements__adwords_campaigns")) \
            .exclude(campaigns__lte=0)

        for opportunity in opportunities:

            flight_alerts = self.get_flight_alerts(opportunity, report)

            for flight_alert in filter(lambda alert: alert is not None, flight_alerts):
                msg = EmailMessage(
                    subject=flight_alert.subject,
                    body=flight_alert.body,
                    from_email=settings.EXPORTS_EMAIL_ADDRESS,
                    to=self.get_to([opportunity.ad_ops_manager.email]),
                    cc=self.get_cc(settings.CF_AD_OPS_DIRECTORS),
                    bcc=self.get_bcc(),
                )
                msg.send(fail_silently=False)


    def get_flight_alerts(self, opportunity, report):
        flight_alerts = []
        placements = opportunity.placements.filter(
            start__lte=self.today, start__isnull=False,
            end__gte=self.before_yesterday, end__isnull=False,
            goal_type_id__lt=2, goal_type_id__isnull=False
        )

        for placement in placements:

            flights = report.get_flights(placement)

            active_campaign_count = Campaign.objects.filter(salesforce_placement_id=placement.id).\
                exclude(status=CampaignStatus.ENDED.value).count()

            if active_campaign_count > 0:

                for flight in flights:
                    if self.is_on_going_flight(flight):
                        flight_alerts.append(
                            self.get_flight_delivery_alert(flight, opportunity)
                        )
        return flight_alerts

    def get_flight_delivery_alert(self, flight_data, opportunity):
        alert_percentage = None

        control_percentages = sorted(
            [int(percentage) for percentage in settings.PACING_NOTIFICATIONS],
            reverse=True
        )

        for control_percentage in control_percentages:

            if self.check_flight_delivered(flight_data, control_percentage):
                alert_percentage = control_percentage
                break

        if alert_percentage is not None:
            flight_alert = FlightAlert(flight_name=flight_data.get("name"), opportunity_name=opportunity.name,
                                       control_percentage=alert_percentage)
            mail, created = SavedEmail.objects.get_or_create(id=flight_alert.__hash__())

            if created is True:
                mail.html = f"{flight_alert.subject}\n{flight_alert.body}"
                mail.save()

                return flight_alert

    def check_flight_delivered(self, flight, control_percentage):
        try:
            percentage = 0
            if flight.get("goal_type_id") == SalesForceGoalType.CPM:
                percentage = safe_div(flight.get("impressions", 0), flight.get("plan_impressions", 0)) or 0
            elif flight.get("goal_type_id") == SalesForceGoalType.CPV:
                percentage = safe_div(flight.get("video_views", 0), flight.get("plan_video_views", 0)) or 0
            return percentage * 100 >= control_percentage
        except:
            pass


class FlightAlert:
    def __init__(self, flight_name, opportunity_name, control_percentage):
        self.subject = "{control_percentage}% DELIVERY - {flight_name}".format(
            control_percentage=control_percentage, flight_name=flight_name)
        self.body = "{flight_name} in {opportunity_name} has delivered {control_percentage}% of its ordered units".format(
            flight_name=flight_name, control_percentage=control_percentage, opportunity_name=opportunity_name
        )

    def __hash__(self):
        return hashlib.md5(str(self.body).encode()).hexdigest()