import pytz
from rest_framework.permissions import IsAdminUser
from rest_framework.views import APIView

from aw_reporting.models import Account
from userprofile.models import UserProfile
from utils.csv_export import BaseCSVStreamResponseGenerator
from utils.datetime import now_in_default_tz


class UserExportColumn:
    FIRST_NAME = "first_name"
    LAST_NAME = "last_name"
    COMPANY = "company"
    PHONE = "phone_number"
    EMAIL = "email"
    REGISTERED_DATE = "registered_date"
    LAST_LOGIN_DATE = "last_login_date"
    AW_ACCOUNTS = "aw_accounts"
    USER_TYPE = "user_type"
    ANNUAL_AD_SPEND = "annual_ad_spend"
    HAS_OAUTH_YOUTUBE_CHANNEL = "has_oauth_youtube_channel"
    STATUS = "status"


CSV_COLUMN_ORDER = (
    UserExportColumn.FIRST_NAME,
    UserExportColumn.LAST_NAME,
    UserExportColumn.COMPANY,
    UserExportColumn.PHONE,
    UserExportColumn.EMAIL,
    UserExportColumn.REGISTERED_DATE,
    UserExportColumn.LAST_LOGIN_DATE,
    UserExportColumn.AW_ACCOUNTS,
    UserExportColumn.USER_TYPE,
    UserExportColumn.ANNUAL_AD_SPEND,
    UserExportColumn.HAS_OAUTH_YOUTUBE_CHANNEL,
    UserExportColumn.STATUS,
)

REPORT_HEADERS = {
    UserExportColumn.FIRST_NAME: "First name",
    UserExportColumn.LAST_NAME: "Last name",
    UserExportColumn.COMPANY: "Company",
    UserExportColumn.PHONE: "Phone",
    UserExportColumn.EMAIL: "Email",
    UserExportColumn.REGISTERED_DATE: "Registered date",
    UserExportColumn.LAST_LOGIN_DATE: "Last login date",
    UserExportColumn.AW_ACCOUNTS: "AW accounts",
    UserExportColumn.USER_TYPE: "User Type",
    UserExportColumn.ANNUAL_AD_SPEND: "Annual Ad Spend",
    UserExportColumn.HAS_OAUTH_YOUTUBE_CHANNEL: "Has Oauth youtube channel",
    UserExportColumn.STATUS: "Status",
}


class UserListCSVExport(BaseCSVStreamResponseGenerator):
    def __init__(self):
        super(UserListCSVExport, self).__init__(CSV_COLUMN_ORDER, self.users_list(), REPORT_HEADERS)

    def users_list(self):
        for user in UserProfile.objects.all().order_by("last_name"):
            yield {
                UserExportColumn.FIRST_NAME: user.first_name,
                UserExportColumn.LAST_NAME: user.last_name,
                UserExportColumn.COMPANY: user.company,
                UserExportColumn.PHONE: user.phone_number,
                UserExportColumn.EMAIL: user.email,
                UserExportColumn.REGISTERED_DATE: user.date_joined.date() if user.date_joined else None,
                UserExportColumn.LAST_LOGIN_DATE: user.last_login.date() if user.last_login else None,
                UserExportColumn.USER_TYPE: user.user_type,
                UserExportColumn.ANNUAL_AD_SPEND: user.annual_ad_spend,
                UserExportColumn.AW_ACCOUNTS: ",".join(Account.user_mcc_objects(user).values_list("name", flat=True)),
                UserExportColumn.HAS_OAUTH_YOUTUBE_CHANNEL: user.channels.exists(),
                UserExportColumn.STATUS: user.status,
            }

    def get_filename(self):
        now = now_in_default_tz()
        now_utc = now.astimezone(pytz.utc)
        timestamp = now_utc.strftime("%Y%m%d %H%M%S")
        return "User List {}.csv".format(timestamp)


# fixme: use utils.api.file_list_api_view.FileListApiView instead
class UserListExportApiView(APIView):
    permission_classes = (IsAdminUser,)

    def get(self, *_):
        csv_generator = UserListCSVExport()
        return csv_generator.prepare_csv_file_response()
