from aw_reporting.models import Account
from aw_creation.models import AccountCreation


class TestMediaBuyingMixin(object):
    def create_account(self, account_params=None):
        account_params = account_params or {}
        params = dict(
            name="test",
        )
        params.update(account_params)
        account = Account.objects.create(**params)
        return account
