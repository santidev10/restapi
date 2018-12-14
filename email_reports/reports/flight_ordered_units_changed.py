from django.conf import settings
from django.core.mail import EmailMessage

from email_reports.reports.base import BaseEmailReport


class FlightOrderedUnitsChangedEmail(BaseEmailReport):

    def __init__(self, opportunity_name, placement_name, flight_name, old_ordered_units,
                 new_ordered_units, recipients):
        super(FlightOrderedUnitsChangedEmail, self).__init__(host=None, debug=settings.DEBUG)
        self.opportunity_name = opportunity_name
        self.placement_name = placement_name
        self.flight_name = flight_name
        self.old_ordered_units = old_ordered_units
        self.new_ordered_units = new_ordered_units
        self.recipients = recipients

    def send(self):
        """
            Send Flight ordered units changed email
            """
        sender = settings.SENDER_EMAIL_ADDRESS
        to = self.get_to(self.recipients or settings.SALESFORCE_UPDATES_ADDRESSES)
        bcc = self.get_bcc()
        subject = "{opportunity_name} Ordered Units has changed".format(opportunity_name=self.opportunity_name)
        text = "Flight: {flight_name}\n\n" \
               "Placement: {placement_name}\n\n" \
               "Change: The ordered units were changed from {old_value} to {new_value}" \
               "".format(flight_name=self.flight_name,
                         placement_name=self.placement_name,
                         old_value=self.old_ordered_units,
                         new_value=self.new_ordered_units)

        msg = EmailMessage(
            subject=subject,
            body=text,
            from_email=sender,
            to=to,
            bcc=bcc,
        )
        msg.send()
