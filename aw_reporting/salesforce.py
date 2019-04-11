import logging
import os
from itertools import chain

import requests
import yaml
from simple_salesforce import Salesforce

from aw_reporting.models import SalesforceFields
from aw_reporting.models.salesforce import Activity
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


def sf_auth():
    with open('aw_reporting/salesforce.yaml', 'r') as f:
        conf = yaml.load(f)
    res = requests.post(
        'https://login.salesforce.com/services/oauth2/token',
        {
            'grant_type': 'refresh_token',
            'client_id': conf.get('consumer_key'),
            'client_secret': conf.get('consumer_secret'),
            'refresh_token': conf.get('refresh_token'),
        }
    )
    return res.json()


class Connection:
    def __init__(self):
        access_data = sf_auth()

        self.sf = Salesforce(instance_url=access_data['instance_url'],
                             session_id=access_data['access_token'])

    def meta(self, name):
        return getattr(self.sf, name).metadata()

    def describe(self, name=None):
        if name:
            return getattr(self.sf, name).describe()
        else:
            return self.sf.describe()

    def list(self, name, limit=10):

        return self.sf.query_all(
            "SELECT * FROM %s LIMIT %d" % (name, limit)
        )

    def get(self, name, uid):
        return getattr(self.sf, name).get(uid)

    def update(self, name, uid, **kwargs):
        return getattr(self.sf, name).update(uid, kwargs)

    def get_report(self, uid, form='csv'):
        response = requests.get(
            "https://na10.salesforce.com/%s"
            "?view=d&snip&export=1&enc=UTF-8&xf=%s" % (uid, form),
            headers=self.sf.headers,
            cookies={'sid': self.sf.session_id}
        )
        return response.content.decode('UTF-8')

    # --
    def get_categories(self):
        describe = self.describe('Opportunity')

        for f in describe.get('fields', []):
            if f['name'] == 'Client_Vertical__c':
                return f['picklistValues']
        return None

    def get_opportunities(self, where=None):
        items = self.get_items(
            'Opportunity',
            SalesforceFields.Opportunity.values(),
            where=where
        )
        return items

    def get_user_roles(self, where=None):
        items = self.get_items(
            'UserRole',
            SalesforceFields.UserRole.values(),
            where=where
        )
        return items

    def get_users(self, where=None, save_photo=False):
        items = self.get_items(
            'User',
            SalesforceFields.User.values(),
            where=where
        )
        if save_photo:
            items = list(items)
            for u in items:
                u['photo_id'] = self.save_img(u[SalesforceFields.User.SMALL_PHOTO_URL.value])
                del u['SmallPhotoUrl']
        return items

    def save_img(self, url):
        path = self.get_image_path(url)
        if not os.path.isfile(path):
            r = requests.get(
                url,
                headers={
                    'Authorization': "Bearer {}".format(
                        self.sf.session_id
                    )
                },
                stream=True
            )
            if r.status_code == 200:
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(1024):
                        f.write(chunk)
        return self.get_image_id(url)

    def get_image_path(self, url):
        path = os.path.join('static', 'img', 'sf')
        if not os.path.exists(path):
            os.makedirs(path)
        file_name = "{}.{}".format(self.get_image_id(url), 'jpg')
        return os.path.join(path, file_name)

    @staticmethod
    def get_image_id(url):
        # https://c.na43.content.force.com/profilephoto/729F00000005d1o/T
        return url.split('/')[-2]

    def get_accounts(self, where=None):
        items = self.get_items(
            'Account',
            SalesforceFields.SFAccount.values(),
            where=where
        )
        return items

    def get_contacts(self, where=None):
        items = self.get_items(
            'Contact',
            SalesforceFields.Contact.values(),
            where=where
        )
        return items

    def get_items(self, name, fields, where):
        """

        :param name:
        :param fields: list of fields to get
        :param where:
        :return:
        """
        get_fields = set(fields)
        describe = getattr(self.sf, name).describe()
        all_fields = set(f['name'] for f in describe['fields'])
        missed = get_fields - all_fields
        if missed:
            logger.critical(
                "There are no %s fields in %s object" % (missed, name)
            )
            logger.info("The fields are {}".format(all_fields))
            get_fields = get_fields & all_fields

        if where:
            where = "WHERE %s " % where
        else:
            where = ""
        response = self.sf.query(
            "SELECT %s "
            "FROM %s %s" % (", ".join(get_fields), name, where)
        )
        for r in response['records']:
            yield r
        while 'nextRecordsUrl' in response:
            response = self.sf.query_more(response['nextRecordsUrl'], True)
            for r in response['records']:
                yield r

    def get_placements(self, where=None):
        items = self.get_items(
            "Placement__c",
            SalesforceFields.Placement.values(),
            where=where
        )
        return items

    def get_flights(self, where=None):
        items = self.get_items(
            'Flight__c',
            SalesforceFields.Flight.values(),
            where=where
        )
        return items

    def get_enabled_placements(self):
        date = now_in_default_tz().date()
        where = "Placement_Start_Date__c <= {date} AND " \
                "Placement_End_Date__c >= {date} ".format(date=date)
        items = self.get_placements(where=where)
        return items

    def get_activities(self, where=None):
        where = "{}{}".format(
            "WhoId != '' AND ActivityDate != NULL",
            " AND {}".format(where) if where else "",
        )
        SalesforceFields.Activity.values()
        fields = set(SalesforceFields.Activity.values())
        events = self.get_items('Event', fields, where=where)
        tasks = self.get_items('Task', fields, where=where)

        def add_type(items, item_type):
            for i in items:
                i['type'] = item_type
                yield i

        activities = chain(
            add_type(events, Activity.MEETING_TYPE),
            add_type(tasks, Activity.EMAIL_TYPE),
        )
        return activities
