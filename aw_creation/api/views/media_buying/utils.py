from django.http import Http404
from rest_framework.exceptions import ValidationError

from aw_creation.models import AccountCreation
from aw_creation.api.views.media_buying.constants import REPORT_CONFIG


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
    user = user
    try:
        account = AccountCreation.objects.user_related(user).get(pk=pk)
    except AccountCreation.DoesNotExist:
        if should_raise:
            raise Http404
        account = None
    return account
