from rest_framework.permissions import IsAdminUser
from rest_framework.views import APIView

from aw_reporting.models import Account
from userprofile.models import UserProfile
from utils.csv_export import BaseCSVStreamResponseGenerator


class UserExportColumn:
    USERNAME = "username"
    COMPANY = "company"
    EMAIL = "email"
    REGISTERED_DATE = "registered_date"
    LAST_LOGIN_DATE = "last_login_date"
    AW_ACCOUNTS = "aw_accounts"
    USER_TYPE = "user_type"
    ANNUAL_AD_SPEND = "annual_ad_spend"


CSV_COLUMN_ORDER = (
    UserExportColumn.USERNAME,
    UserExportColumn.COMPANY,
    UserExportColumn.EMAIL,
    UserExportColumn.REGISTERED_DATE,
    UserExportColumn.LAST_LOGIN_DATE,
    UserExportColumn.AW_ACCOUNTS,
    UserExportColumn.USER_TYPE,
    UserExportColumn.ANNUAL_AD_SPEND,
)

REPORT_HEADERS = {
    UserExportColumn.USERNAME: "Username",
    UserExportColumn.COMPANY: "Company",
    UserExportColumn.EMAIL: "Email",
    UserExportColumn.REGISTERED_DATE: "Registered date",
    UserExportColumn.LAST_LOGIN_DATE: "Last login date",
    UserExportColumn.AW_ACCOUNTS: "AW accounts",
    UserExportColumn.USER_TYPE: "User Type",
    UserExportColumn.ANNUAL_AD_SPEND: "Annual Ad Spend",
}


class UserListCSVExport(BaseCSVStreamResponseGenerator):
    def __init__(self):
        super(UserListCSVExport, self).__init__(CSV_COLUMN_ORDER, self.users_list(), REPORT_HEADERS)

    def users_list(self):
        for user in UserProfile.objects.all():
            yield {
                UserExportColumn.USERNAME: user.get_full_name(),
                UserExportColumn.COMPANY: user.company,
                UserExportColumn.EMAIL: user.email,
                UserExportColumn.REGISTERED_DATE: user.date_joined.date() if user.date_joined else None,
                UserExportColumn.LAST_LOGIN_DATE: user.last_login.date() if user.last_login else None,
                UserExportColumn.USER_TYPE: user.user_type,
                UserExportColumn.ANNUAL_AD_SPEND: user.annual_ad_spend,
                UserExportColumn.AW_ACCOUNTS: ",".join(Account.user_mcc_objects(user).values_list("name", flat=True)),
            }

    def get_filename(self):
        return "users_list.csv"


class UserListExportApiView(APIView):
    permission_classes = (IsAdminUser,)

    def get(self, *_):
        csv_generator = UserListCSVExport()
        return csv_generator.prepare_csv_file_response()
