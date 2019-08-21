import logging
from datetime import timedelta

import requests

logger = logging.getLogger(__name__)


def get_dates_range(date_from, date_to):
    delta = date_to - date_from
    for i in range(delta.days + 1):
        yield date_from + timedelta(days=i)


def get_google_access_token_info(token):
    url = "https://www.googleapis.com/oauth2/v3/tokeninfo?" \
          "access_token={}".format(token)
    token_info = requests.get(url).json()
    return token_info


def safe_max(sequence):
    """
    Simple max function with None values ignoring in sequence
    """
    sequence = [item for item in sequence if item is not None]
    if not sequence:
        return None
    return max(sequence)
