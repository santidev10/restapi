from datetime import datetime
from datetime import timedelta

from django.conf import settings
from django.utils.log import AdminEmailHandler

from utils.redis import get_redis_client
from administration.notifications import send_email_using_alternative_smtp


class LimitExhausted(Exception):
    pass


class LimitedAdminEmailHandler(AdminEmailHandler):
    REDIS_KEY = "admin_email_limiter"

    def check_limits(self):
        client = get_redis_client()
        key = client.get(self.REDIS_KEY)

        if client.incr(self.REDIS_KEY) >= settings.ADMIN_EMAIL_LIMIT:
            raise LimitExhausted

        if not key:
            key_expire_at = datetime.utcnow().replace(hour=0, minute=0, second=0) + timedelta(days=1)
            client.expireat(self.REDIS_KEY, key_expire_at)

    def emit(self, record):
        try:
            self.check_limits()
        except LimitExhausted:
            return
        super(LimitedAdminEmailHandler, self).emit(record)

    def send_mail(self, subject, message, *args, **kwargs):
        """
        Send a message to the admins, as defined by the ADMINS setting using the alternative SMTP to the default one.
        This function is used to report exceptions to Admins.
        """
        if not settings.ADMINS or not settings.SERVER_EMAIL:
            return
        html_message = kwargs["html_message"] if "html_message" in kwargs else None
        send_email_using_alternative_smtp(subject=subject, message=message, recipient_list=settings.ADMINS,
                                          html_message=html_message)
