import pytz
from rest_framework_csv.renderers import CSVStreamingRenderer

from aw_reporting.models import Account
from userprofile.constants import StaticPermissions
from userprofile.models import UserProfile
from utils.api.file_list_api_view import FileListApiView
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


class UserListCSVRendered(CSVStreamingRenderer):
    header = (
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
    labels = {
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


class UserListExportApiView(FileListApiView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.USER_MANAGEMENT),)
    renderer_classes = (UserListCSVRendered,)
    queryset = UserProfile.objects.all().order_by("last_name")

    @property
    def filename(self):
        now = now_in_default_tz()
        now_utc = now.astimezone(pytz.utc)
        timestamp = now_utc.strftime("%Y%m%d %H%M%S")
        return "User List {}.csv".format(timestamp)

    def data_generator(self, *_, **__):
        queryset = self.filter_queryset(self.get_queryset())
        for user in queryset:
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
