import logging

from django.db import transaction
from django.db.models.signals import post_save
from django.utils import timezone

from aw_reporting.adwords_api import get_all_customers
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Account

logger = logging.getLogger(__name__)


class AccountUpdater(UpdateMixin):
    def __init__(self, mcc_account):
        self.mcc_account = mcc_account
        self.existing_accounts = set()
        # Ignore accounts that do not have valid Google Ads account ids
        for account in Account.objects.all():
            try:
                int(account.id)
                if "demo" in account.name.lower():
                    continue
            except ValueError:
                continue
            except AttributeError:
                # Account name is None
                pass
            self.existing_accounts.add(account.id)

    def update(self, client):
        accounts_to_update = []
        created_accounts = []
        accounts = get_all_customers(client)
        for e in accounts:
            account_id = str(e['customerId'])
            account_obj = Account(
                id=account_id,
                name=e['name'],
                currency_code=e['currencyCode'],
                timezone=e['dateTimeZone'],
                can_manage_clients=e['canManageClients'],
            )
            if account_id in self.existing_accounts:
                accounts_to_update.append(account_obj)
            else:
                account_obj.save()
                created_accounts.append(account_obj)
        Account.objects.bulk_update(accounts_to_update, ["name", "currency_code", "timezone", "can_manage_clients"])
        all_managed_accounts = [cid.id for cid in accounts_to_update] + [cid.id for cid in created_accounts]
        self.mcc_account.managers.add(*Account.objects.filter(id__in=all_managed_accounts))
        self.mcc_account.update_time = timezone.now()
        self.mcc_account.save()
