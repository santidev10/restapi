import logging
from datetime import datetime
from datetime import timedelta

from django.utils import timezone

from aw_reporting.models import *
from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException
from userprofile.constants import UserSettingsKey
from utils.lang import pick_dict

__all__ = [
    "DEMO_ACCOUNT_ID",
    "DEMO_AD_GROUPS",
    "DEMO_CAMPAIGNS_COUNT",
    "DEMO_DATA_HOURLY_LIMIT",
    "DEMO_DATA_PERIOD_DAYS",
    "DEMO_NAME",
    "DEMO_SF_ACCOUNT",
    "DEMO_BRAND",
]

logger = logging.getLogger(__name__)

DEMO_ACCOUNT_ID = "demo"
DEMO_NAME = "Demo"
DEMO_CAMPAIGNS_COUNT = 2
DEMO_DATA_PERIOD_DAYS = 20
DEMO_AD_GROUPS = (
    "Topics", "Interests", "Keywords", "Channels", "Videos"
)
DEMO_BRAND = "Demo Brand"
DEMO_COST_METHOD = ["CPM", "CPV"]
DEMO_SF_ACCOUNT = "Initiative LA"
DEMO_DATA_HOURLY_LIMIT = 13
