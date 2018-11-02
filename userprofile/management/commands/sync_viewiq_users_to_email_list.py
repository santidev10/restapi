"""
Command to import users from ViewIq into AutoPilot
"""
import logging
from django.contrib.auth import get_user_model
from django.core.management import BaseCommand
from django.conf import settings
import requests
import json

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    model = get_user_model()
    lists = {
        "agency": "ad5cfee0-5d1a-4a89-b597-3cac50edcc86",
        "brand": "339e4e1c-2b9b-47c3-ab06-2b35cd6551a5",
        "creator": "21f984e7-587f-4405-8cd4-57a1ac24ab1c",
    } # avp test lists
    headers = {
        'autopilotapikey': settings.AUTOPILOT_API_KEY,
        'Content-Type': 'application/json'
    }
    server = "api2.autopilothq.com" #production
    # server = "private-anon-cc59eabbd0-autopilot.apiary-mock.com" #mock

    def handle(self, *args, **options):
        logger.info("Start")
        query = self.model.objects.filter(synced_with_email_campaign=False, user_type__isnull=False)
        count = self.add_contacts_to_autopilot(query)
        logger.info("DONE {} users processed.".format(count))

    def add_contacts_to_autopilot(self, users):
        count = 0
        for user in users[:200]:
            # we only want to send 200 max at a time, we can't use bulk API
            # because bulk does not support directly adding to specific lists.
            values = {'contact': {
              "FirstName": user.first_name,
              "LastName": user.last_name,
              "Email": user.email,
              "_autopilot_list": "contactlist_{}".format(self.lists[user.user_type]),
            }}
            r = requests.post('https://{}/v1/contact'.format(self.server), headers=self.headers, data=json.dumps(values))
            if r.status_code == 200:
                user.synced_with_email_campaign=True
                user.save(update_fields=['synced_with_email_campaign'])
            count+=1
        return count