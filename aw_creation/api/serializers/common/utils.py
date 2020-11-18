from aw_reporting.models import Account
from aw_reporting.models import Opportunity


def get_currency_code(account_creation, show_aw_rates):
    """
    Get currency code to display
    If show_aw_rates is True, use the currency code defined on the Google Ads Account
    else, use the currency code defined on the Opportunity
    :param account_creation: AccountCreation
    :param show_aw_rates: bool
    :return:
    """
    currency_code = None
    if show_aw_rates:
        try:
            currency_code = account_creation.account.currency_code
        except (Account.DoesNotExist, AttributeError):
            pass
    else:
        try:
            opportunities = Opportunity.objects.filter(
                placements__adwords_campaigns__account__account_creation=account_creation).distinct()
            currency_code = opportunities.first().currency_code
        except AttributeError:
            pass
    return currency_code
