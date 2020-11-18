from aw_reporting.models import Account
from aw_reporting.models import Opportunity


def get_currency_code(account_creation, show_client_cost):
    """
    Get currency code to display
    If show_client_cost is True, use the currency code defined on the Opportunity
    else, use the currency code defined on the Google Ads Account
    :param account_creation:
    :param show_client_cost:
    :return:
    """
    if show_client_cost:
        try:
            opportunities = Opportunity.objects.filter(
                placements__adwords_campaigns__account__account_creation=account_creation).distinct()
            currency_code = opportunities.first().currency_code
        except AttributeError:
            currency_code = None
    else:
        try:
            currency_code = account_creation.account.currency_code
        except (Account.DoesNotExist, AttributeError):
            currency_code = None
    return currency_code
