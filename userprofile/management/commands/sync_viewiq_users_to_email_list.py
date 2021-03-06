"""
Command to import users from ViewIq into AutoPilot
"""
import json
import logging

import requests
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    model = get_user_model()
    lists = {
        "agency": "324bcfdb-5368-423c-99f0-50935683d5fb",
        "brand": "ff1efb0-eabf-48a2-b1ed-145b28b9c3b3",
        "creator": "ad715036-b107-4f98-9b9a-b60f5fb16fef",
    }  # avp test lists
    headers = {
        "autopilotapikey": settings.AUTOPILOT_API_KEY,
        "Content-Type": "application/json"
    }
    server = "api2.autopilothq.com"  # production

    # server = "private-anon-cc59eabbd0-autopilot.apiary-mock.com" #mock

    def handle(self, *args, **options):
        logger.info("Start")
        query = self.model.objects.filter(synced_with_email_campaign=False, user_type__isnull=False)
        count = self.add_contacts_to_autopilot(query)
        logger.info("DONE %s users processed.", count)

    def add_contacts_to_autopilot(self, users):
        count = 0
        for user in users[:200]:
            # we only want to send 200 max at a time, we can't use bulk API
            # because bulk does not support directly adding to specific lists.
            values = {"contact": {
                "FirstName": user.first_name,
                "LastName": user.last_name,
                "Email": user.email,
                "_autopilot_list": "contactlist_{}".format(self.lists[user.user_type]),
            }}
            r = requests.post("https://{}/v1/contact".format(self.server), headers=self.headers,
                              data=json.dumps(values))
            if r.status_code == 200:
                user.synced_with_email_campaign = True
                user.save(update_fields=["synced_with_email_campaign"])
            count += 1
        return count
