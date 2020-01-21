from datetime import datetime
from datetime import timedelta

from django.conf import settings
from django.utils.log import AdminEmailHandler

from utils.redis import get_redis_client


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
