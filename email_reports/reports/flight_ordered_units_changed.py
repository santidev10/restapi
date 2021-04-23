from django.conf import settings

from administration.notifications import send_email
from email_reports.reports.base import BaseEmailReport


class FlightOrderedUnitsChangedEmail(BaseEmailReport):

    def __init__(self, opportunity_name, placement_name, flight_name, old_ordered_units,
                 new_ordered_units, recipients):
        super(FlightOrderedUnitsChangedEmail, self).__init__(host=None, debug=settings.DEBUG_EMAIL_NOTIFICATIONS)
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
        sender = settings.EXPORTS_EMAIL_ADDRESS
        to = self.recipients or settings.SALESFORCE_UPDATES_ADDRESSES
        if self.debug:
            to = to + settings.DEBUG_EMAIL_ADDRESSES
        subject = "{opportunity_name} Ordered Units has changed".format(opportunity_name=self.opportunity_name)
        text = "Flight: {flight_name}\n\n" \
               "Placement: {placement_name}\n\n" \
               "Change: The ordered units were changed from {old_value} to {new_value}" \
               "".format(flight_name=self.flight_name,
                         placement_name=self.placement_name,
                         old_value=self.old_ordered_units,
                         new_value=self.new_ordered_units)

        send_email(
            subject=subject,
            message=text,
            from_email=sender,
            recipient_list=to
        )
