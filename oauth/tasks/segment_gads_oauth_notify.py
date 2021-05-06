from datetime import timedelta

from dateutil import parser
from django.conf import settings

from administration.notifications import send_html_email
from oauth.constants import OAuthData
from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from saas import celery_app
from utils.datetime import now_in_default_tz
from utils.celery.tasks import celery_lock

NOTIFY_HOURS_THRESHOLD = 24


@celery_app.task(bind=True)
@celery_lock("segment_gads_oauth_notify_task", expire=3600, max_retries=0)
def segment_gads_oauth_notify_task():
    """
    This task filters for OAuth Accounts that have not completed the CustomSegment oauth flow and emails
    these users prompting them to complete the process
    """
    oauth_accounts = OAuthAccount.objects.filter(oauth_type=int(OAuthType.GOOGLE_ADS), is_enabled=True, synced=True,
                                                 revoked_access=False)
    notify_threshold = now_in_default_tz() - timedelta(hours=NOTIFY_HOURS_THRESHOLD)
    for oauth in oauth_accounts:
        should_notify = False
        gads_oauth_timestamp = oauth.data.get(OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP)
        try:
            if isinstance(gads_oauth_timestamp, str) and \
                    parser.parse(gads_oauth_timestamp) < notify_threshold:
                should_notify = True
                # Set to False to avoid sending multiple notification emails
                oauth.update_data(OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP, False)
        except (TypeError, parser.ParserError):
            pass
        if should_notify is True:
            _send_email(oauth)


def _send_email(oauth_account: OAuthAccount) -> None:
    """
    Send notification email to OAuthAccount user prompting them to finish segment oauth process
    :param oauth_account: OAuthAccount
    :return: None
    """
    subject = "ViewIQ: Your Google Ads authorization is not complete"
    text_content = "You recently started an authorization process for ViewIQ Placement Targeting within Build. " \
                   "Please complete the synchronization of your account to enable the ability to upload Placement " \
                   "Targeting from our platform."
    link = f"\n\nYou can review the steps necessary at the below link. " \
           f"\n<a href={settings.HOST}/build/>Click here</a>"
    send_html_email(
        subject=subject,
        to=[oauth_account.user.email],
        text_header=subject,
        text_content=text_content + link,
        from_email=settings.EXPORTS_EMAIL_ADDRESS
    )
