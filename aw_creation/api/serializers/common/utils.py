from aw_reporting.models import Account
from aw_reporting.models import Opportunity


def get_currency_code(account_creation):
    opportunities = Opportunity.objects.filter(placements__adwords_campaigns__account__account_creation=account_creation) \
        .distinct()
    try:
        currency_code = opportunities.first().currency_code
    except AttributeError:
        currency_code = None
    if currency_code is None:
        try:
            currency_code = account_creation.account.currency_code
        except (Account.DoesNotExist, AttributeError):
            pass
    return currency_code
