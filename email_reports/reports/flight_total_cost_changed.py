from django.conf import settings
from django.core.mail import EmailMessage

from email_reports.reports.base import BaseEmailReport


class FlightTotalCostChangedEmail(BaseEmailReport):

    def __init__(self, opportunity_name, placement_name, flight_name, old_total_cost, new_total_cost, ad_ops_email):
        super(FlightTotalCostChangedEmail, self).__init__(host=None, debug=settings.DEBUG)
        self.opportunity_name = opportunity_name
        self.placement_name = placement_name
        self.flight_name = flight_name
        self.old_total_cost = old_total_cost
        self.new_total_cost = new_total_cost
        self.ad_ops_email = ad_ops_email

    def send(self):
        sender = settings.SENDER_EMAIL_ADDRESS
        to = self.get_to([self.ad_ops_email])
        bcc = self.get_bcc()
        subject = "{opportunity_name} Total Client Cost has changed".format(opportunity_name=self.opportunity_name)
        text = "Flight: {flight_name}\n\n" \
               "Placement: {placement_name}\n\n" \
               "Change: The total client cost was changed from {old_value} to {new_value}" \
               "".format(flight_name=self.flight_name,
                         placement_name=self.placement_name,
                         old_value=self.old_total_cost,
                         new_value=self.new_total_cost)

        msg = EmailMessage(
            subject=subject,
            body=text,
            from_email=sender,
            to=to,
            bcc=bcc,
        )
        msg.send()
