import logging
import os
from itertools import chain

import requests
import yaml
from simple_salesforce import Salesforce

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
            [
                'Id', 'Name', 'Client_Vertical__c', 'Territory1__c',
                'Grand_Total__c', 'Projected_Launch_Date__c',
                'MIN_Placement_Start_Date__c', 'MAX_Placement_End_Date__c',
                'Rate_Type__c', 'Cost_Method__c',

                'CPV_Units_Purchased__c',
                'CPM_Impression_Units_Purchased__c',
                'CPV_Total_Client_Cost__c',
                'CPM_Total_Client_Cost__c',
                'Quoted_CPM_Price__c',
                'Avg_Cost_Per_Unit__c',

                'StageName',
                'Ad_Ops_Campaign_Manager_UPDATE__c', 'Ad_Ops_QA_Manager__c',
                'OwnerId',
                'Account_Manager__c',
                'OPP_ID_Number__c',
                'AdWords_CID__c',
                'Agency_Contact__c', 'AccountId', 'Brand_Test__c',
                'CPM_Rate__c', 'CPV_Rate__c',
                'DO_NOT_STRAY_FROM_DELIVERY_SCHEDULE__c',
                'Probability', 'CreatedDate', 'CloseDate',
                'Renewal_Approved__c', 'Reason_for_Close_Lost__c',
                'Date_Proposal_Submitted__c',

                'Demo_TEST__c', 'Geo_Targeting_Country_State_City__c', 
                'Targeting_Tactics__c', 'Tags__c',
                'Types_of__c',
                "APEX_Deal__c",
                "Bill_off_3p_Numbers__c"
            ],
            where=where
        )
        return items

    def get_user_roles(self, where=None):
        items = self.get_items(
            'UserRole',
            ['Id', 'Name'],
            where=where
        )
        return items

    def get_users(self, where=None, save_photo=False):
        items = self.get_items(
            'User',
            ['Id', 'Name', 'SmallPhotoUrl', 'Email', 'UserRoleId', 'IsActive'],
            where=where
        )
        if save_photo:
            items = list(items)
            for u in items:
                u['photo_id'] = self.save_img(u['SmallPhotoUrl'])
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
            ['Id', 'Name', 'ParentId'],
            where=where
        )
        return items

    def get_contacts(self, where=None):
        items = self.get_items(
            'Contact',
            ['Id', 'FirstName', 'LastName'],
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
            [
                "Id", "Name", "Placement_Start_Date__c",
                "Placement_End_Date__c", "Total_Ordered_Units__c",
                "Net_Cost__c", "Cost_Method__c", "Total_Actual_Clicks__c",
                "Views_Purchased__c", "Account_Manager__c", "Sales_Rep__c",
                "Account_Name__c", "Opportunity_Name__c", "Insertion_Order__c",
                "Ordered_Cost_Per_Unit__c", "PLACEMENT_ID_Number__c",
                "Total_Client_Costs__c", "Adwords_Placement_IQ__c",
                "Incremental__c", "Placement_Type__c", "Dynamic_Placement__c",
                "Tech_Fee_if_applicable__c", "Tech_Fee_Cap_if_applicable__c",
                "Tech_Fee_Type__c",
            ],
            where=where
        )
        return items

    def get_flights(self, where=None):
        items = self.get_items(
            'Flight__c',
            [
                'Id', 'Name', 'Placement__c',
                'Flight_Start_Date__c', 'Flight_End_Date__c',
                'Ordered_Amount__c', 'Ordered_Units__c',
                'Delivered_Ad_Ops__c', 'Total_Flight_Cost__c',
                'Flight_Month__c',
                'Flight_Value__c',
            ],
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
        fields = [
            'Id', 'Subject', 'WhatId', 'OwnerId', 'AccountId',
            'ActivityDate',
        ]
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
