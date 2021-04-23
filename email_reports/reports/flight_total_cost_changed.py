from django.conf import settings

from administration.notifications import send_email_with_headers
from email_reports.reports.base import BaseEmailReport


class FlightTotalCostChangedEmail(BaseEmailReport):

    def __init__(self, opportunity_name, placement_name, flight_name, old_total_cost, new_total_cost, recipients):
        super(FlightTotalCostChangedEmail, self).__init__(host=None, debug=settings.DEBUG_EMAIL_NOTIFICATIONS)
        self.opportunity_name = opportunity_name
        self.placement_name = placement_name
        self.flight_name = flight_name
        self.old_total_cost = old_total_cost
        self.new_total_cost = new_total_cost
        self.recipients = recipients

    def send(self):
        sender = settings.EXPORTS_EMAIL_ADDRESS
        subject = "{opportunity_name} Total Client Cost has changed".format(opportunity_name=self.opportunity_name)
        to = self.get_to(self.recipients or settings.SALESFORCE_UPDATES_ADDRESSES)
        bcc = self.get_bcc()
        text = "Flight: {flight_name}\n\n" \
               "Placement: {placement_name}\n\n" \
               "Change: The total client cost was changed from {old_value} to {new_value}" \
               "".format(flight_name=self.flight_name,
                         placement_name=self.placement_name,
                         old_value=self.old_total_cost,
                         new_value=self.new_total_cost)

        send_email_with_headers(
            subject=subject,
            body=text,
            from_email=sender,
            to=to,
            bcc=bcc
        )
