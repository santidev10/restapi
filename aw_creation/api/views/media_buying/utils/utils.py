from django.http import Http404
from rest_framework.exceptions import ValidationError

from aw_creation.api.views.media_buying.constants import REPORT_CONFIG
from aw_creation.models import AccountCreation
from userprofile.constants import UserSettingsKey


def validate_targeting(value, valid_targeting, should_raise=True):
    errs = []
    if not isinstance(value, str):
        errs.append(f"Invalid targeting value: {value}. Must be singular string value.")
    if value not in valid_targeting:
        errs.append(f"Invalid targeting value: {value}. Valid targeting: {valid_targeting}")
    if errs:
        if should_raise:
            raise ValidationError(errs)
        config = None
    else:
        config = REPORT_CONFIG[value]
    return config


def get_account_creation(user, pk, should_raise=True):
    queryset = AccountCreation.objects.all()
    user_settings = user.get_aw_settings()
    if not user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS):
        visible_accounts = user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
        queryset = queryset.filter(account__id__in=visible_accounts)
    try:
        account_creation = queryset.get(pk=pk)
    except AccountCreation.DoesNotExist:
        if should_raise is True:
            raise Http404
        account_creation = None
    return account_creation
