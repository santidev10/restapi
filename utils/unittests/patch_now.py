from contextlib import contextmanager
from datetime import date
from datetime import datetime
from unittest.mock import patch

import pytz
from django.conf import settings

from utils.datetime import Time


@contextmanager
def patch_now(now):
    # pylint: disable=unidiomatic-typecheck
    if type(now) == date:
        now = datetime.combine(now, datetime.min.time())
    # pylint: enable=unidiomatic-typecheck
    if now.tzinfo is None:
        now = now.replace(tzinfo=pytz.timezone(settings.DEFAULT_TIMEZONE))
    with patch.object(Time, "now", return_value=now):
        yield
