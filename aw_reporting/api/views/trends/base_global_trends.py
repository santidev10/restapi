from aw_reporting.models import Account
from aw_reporting.settings import InstanceSettings, InstanceSettingsKey


def get_account_queryset():
    global_trends_accounts_id = InstanceSettings() \
        .get(InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS)
    return Account.objects \
        .filter(managers__id__in=global_trends_accounts_id)