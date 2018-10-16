import csv
from datetime import datetime

import pytz
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from administration.api.urls.names import AdministrationPathName
from administration.api.views.user_list_export import CSV_COLUMN_ORDER
from administration.api.views.user_list_export import UserExportColumn
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace
from userprofile.models import UserChannel
from userprofile.models import UserProfile
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import int_iterator
from utils.utils_tests import patch_now
from utils.utils_tests import reverse


class UserListExportAPITestCase(ExtendedAPITestCase):
    def _request(self):
        url = reverse(AdministrationPathName.USER_LIST_EXPORT, [Namespace.ADMIN])
        return self.client.get(url)

    def test_not_authorized(self):
        response = self._request()
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_not_admin(self):
        self.create_test_user()
        response = self._request()
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success(self):
        self.create_admin_user()
        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "text/csv")

    def test_filename(self):
        hour_in_the_tz = 12
        test_now_in_the_tz = datetime(2018, 2, 3, hour_in_the_tz, 23, 14, tzinfo=pytz.timezone("America/Los_Angeles"))
        test_now_in_utc = test_now_in_the_tz.astimezone(pytz.utc)
        self.assertNotEqual(test_now_in_the_tz.hour, test_now_in_utc.hour)
        expected_timestamp = test_now_in_utc.strftime("%Y%m%d %H%M%S")
        expected_filename = "User List {}.csv".format(expected_timestamp)
        self.create_admin_user()
        with patch_now(test_now_in_the_tz):
            response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Disposition"], "attachment; filename='{}'".format(expected_filename))

    def test_headers(self):
        self.create_admin_user()
        response = self._request()
        csv_data = get_data_from_csv_response(response)
        headers = next(csv_data)
        self.assertEqual(headers, [
            "First name",
            "Last name",
            "Company",
            "Phone",
            "Email",
            "Registered date",
            "Last login date",
            "AW accounts",
            "User Type",
            "Annual Ad Spend",
            "Has Oauth youtube channel",
            "Is subscribed",
        ])

    def test_users_count(self):
        for _ in range(5):
            UserProfile.objects.create(email="test+{}@mail.com".format(next(int_iterator)))
        self.create_admin_user()
        user_count = UserProfile.objects.all().count()
        response = self._request()
        data_rows = get_data_rows(response)
        self.assertEqual(len(data_rows), user_count)

    def test_first_name(self):
        expected_first_name = "First Name"
        self.create_admin_user(first_name=expected_first_name)
        response = self._request()
        user_row = get_data_rows(response)[0]
        first_name = get_value(user_row, UserExportColumn.FIRST_NAME)
        self.assertEqual(first_name, expected_first_name)

    def test_last_name(self):
        expected_last_name = "Last Name"
        self.create_admin_user(last_name=expected_last_name)
        response = self._request()
        user_row = get_data_rows(response)[0]
        last_name = get_value(user_row, UserExportColumn.LAST_NAME)
        self.assertEqual(last_name, expected_last_name)

    def test_is_subscribed_true(self):
        expected_value = str(True)
        self.create_admin_user(is_subscribed=True)
        response = self._request()
        user_row = get_data_rows(response)[0]
        last_name = get_value(user_row, UserExportColumn.IS_SUBSCRIBED)
        self.assertEqual(last_name, expected_value)

    def test_is_subscribed_false(self):
        expected_value = str(False)
        self.create_admin_user(is_subscribed=False)
        response = self._request()
        user_row = get_data_rows(response)[0]
        last_name = get_value(user_row, UserExportColumn.IS_SUBSCRIBED)
        self.assertEqual(last_name, expected_value)

    def test_has_oauth_youtube_channel_true(self):
        expected_value = str(True)
        user = self.create_admin_user()
        UserChannel.objects.create(channel_id="test", user=user)
        response = self._request()
        user_row = get_data_rows(response)[0]
        has_oauth_youtube_channel = get_value(user_row, UserExportColumn.HAS_OAUTH_YOUTUBE_CHANNEL)
        self.assertEqual(has_oauth_youtube_channel, expected_value)

    def test_has_oauth_youtube_channel_false(self):
        expected_value = str(False)
        self.create_admin_user()
        response = self._request()
        user_row = get_data_rows(response)[0]
        has_oauth_youtube_channel = get_value(user_row, UserExportColumn.HAS_OAUTH_YOUTUBE_CHANNEL)
        self.assertEqual(has_oauth_youtube_channel, expected_value)

    def test_company(self):
        expected_company = "Some company name"
        self.create_admin_user(company=expected_company)
        response = self._request()
        user_row = get_data_rows(response)[0]
        company = get_value(user_row, UserExportColumn.COMPANY)
        self.assertEqual(company, expected_company)

    def test_email(self):
        user = self.create_admin_user()
        response = self._request()
        user_row = get_data_rows(response)[0]
        email = get_value(user_row, UserExportColumn.EMAIL)
        self.assertEqual(email, user.email)

    def test_date_joined(self):
        test_date_join = datetime(2018, 10, 1, tzinfo=pytz.utc)
        date_joined = str(test_date_join.date())
        self.create_admin_user(date_joined=test_date_join)
        response = self._request()
        user_row = get_data_rows(response)[0]
        registered_date = get_value(user_row, UserExportColumn.REGISTERED_DATE)
        self.assertEqual(registered_date, date_joined)

    def test_last_login(self):
        test_last_login = datetime(2018, 10, 1, tzinfo=pytz.utc)
        expected_last_login = str(test_last_login.date())
        self.create_admin_user(last_login=test_last_login)
        response = self._request()
        user_row = get_data_rows(response)[0]
        last_login = get_value(user_row, UserExportColumn.LAST_LOGIN_DATE)
        self.assertEqual(last_login, expected_last_login)

    def test_user_type(self):
        expected_user_type = "Test user type"
        self.create_admin_user(user_type=expected_user_type)
        response = self._request()
        user_row = get_data_rows(response)[0]
        user_type = get_value(user_row, UserExportColumn.USER_TYPE)
        self.assertEqual(user_type, expected_user_type)

    def test_annual_spend(self):
        expected_annual_ad_spend = "Test annual ad spend"
        self.create_admin_user(annual_ad_spend=expected_annual_ad_spend)
        response = self._request()
        user_row = get_data_rows(response)[0]
        annual_ad_spend = get_value(user_row, UserExportColumn.ANNUAL_AD_SPEND)
        self.assertEqual(annual_ad_spend, expected_annual_ad_spend)

    def test_aw_accounts(self):
        user = self.create_admin_user()

        def add_mcc_account(name):
            account = Account.objects.create(id=next(int_iterator), name=name)
            connection = AWConnection.objects.create(email="test+{}@mail.com".format(next(int_iterator)))
            AWAccountPermission.objects.create(account=account, aw_connection=connection)
            AWConnectionToUserRelation.objects.create(user=user, connection=connection)

        add_mcc_account("Test account 1")
        add_mcc_account("Test account 2")
        connected_accounts = Account.user_mcc_objects(user)
        self.assertGreater(connected_accounts.count(), 1)
        expected_aw_accounts = ",".join(connected_accounts.values_list("name", flat=True))
        response = self._request()
        user_row = get_data_rows(response)[0]
        aw_accounts = get_value(user_row, UserExportColumn.AW_ACCOUNTS)
        self.assertEqual(aw_accounts, expected_aw_accounts)

    def test_order_by_email(self):
        user = self.create_admin_user()
        UserProfile.objects.create(email="test+1@mail.com", last_name="test+1")
        UserProfile.objects.create(email="test+4@mail.com", last_name="test+4")
        UserProfile.objects.create(email="test+3@mail.com", last_name="test+3")
        users = UserProfile.objects.all()
        sorted_users = sorted(users, key=lambda u: u.email)
        self.assertNotEqual(user, sorted_users)
        response = self._request()
        rows = get_data_rows(response)
        user_last_names = [get_value(row, UserExportColumn.LAST_NAME) for row in rows]
        expected_last_names = [user.last_name for user in sorted_users]
        self.assertEqual(user_last_names, expected_last_names)


def get_data_from_csv_response(response):
    return csv.reader((row.decode("utf-8") for row in response.streaming_content))


def get_data_rows(response):
    csv_data = get_data_from_csv_response(response)
    next(csv_data)  # skip headers
    return list(csv_data)


def get_value(row, key):
    index = CSV_COLUMN_ORDER.index(key)
    return row[index]
