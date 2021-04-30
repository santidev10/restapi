from datetime import timedelta

from dateutil import parser
from django.conf import settings

from administration.notifications import send_html_email
from oauth.constants import OAuthData
from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from utils.datetime import now_in_default_tz

NOTIFY_HOURS_THRESHOLD = 24


def segment_gads_oauth_notify_task():
    """
    This function filters for OAuth Accounts that have not completed the CustomSegment oauth flow and emails
    these users prompting them to complete the process
    :return:
    """
    oauth_accounts = OAuthAccount.objects.filter(oauth_type=int(OAuthType.GOOGLE_ADS), is_enabled=True, synced=True,
                                                 revoked_access=False)
    for oauth in oauth_accounts:
        should_notify = False
        gads_oauth_timestamp = oauth.data.get(OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP)
        try:
            if gads_oauth_timestamp is not True and \
                    parser.parse(gads_oauth_timestamp) < now_in_default_tz() - timedelta(hours=NOTIFY_HOURS_THRESHOLD):
                should_notify = True
        except (TypeError, parser.ParserError):
            pass
        if should_notify is True:
            _send_email(oauth)


def _send_email(oauth_account):
    subject = "ViewIQ: Your Google Ads authorization is not complete"
    text_content = "You recently started an authorization process for ViewIQ Placement Targeting within Build. " \
                   "Please complete the synchronization of your account to enable the ability to upload Placement " \
                   "Targeting from our platform."
    send_html_email(
        subject=subject,
        to=[oauth_account.user.email],
        text_header=subject,
        text_content=text_content,
        from_email=settings.EXPORTS_EMAIL_ADDRESS
    )
