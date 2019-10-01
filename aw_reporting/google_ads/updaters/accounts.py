import logging

from django.db import transaction
from django.db.models.signals import post_save
from django.utils import timezone
from google.ads.google_ads.errors import GoogleAdsException

from aw_reporting.google_ads.constants import CUSTOMER_CLIENT_ACCOUNT_FIELDS
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Account

logger = logging.getLogger(__name__)


class AccountUpdater(UpdateMixin):
    RESOURCE_NAME = "customer_client"
    MAX_RETRIES = 5

    def __init__(self, mcc_account):
        self.mcc_account = mcc_account
        self.ga_service = None
        self.existing_cid_accounts = set([int(_id) for _id in Account.objects.filter(can_manage_clients=False).values_list("id", flat=True)])

    def update(self, client):
        self.ga_service = client.get_service("GoogleAdsService", version="v2")
        accounts_to_update = []
        accounts_to_create = []
        customer_client_accounts = self.get_client_customer_accounts()
        for row in customer_client_accounts:
            account_id = row.customer_client.id.value
            account_obj = Account(
                id=account_id,
                name=row.customer_client.descriptive_name.value,
                currency_code=row.customer_client.currency_code.value,
                timezone=row.customer_client.time_zone.value,
                can_manage_clients=row.customer_client.manager.value,
                is_test_account=row.customer_client.test_account.value,
            )
            if account_id in self.existing_cid_accounts:
                accounts_to_update.append(account_obj)
            else:
                accounts_to_create.append(account_obj)

        Account.objects.bulk_update(accounts_to_update, ["name", "currency_code", "timezone", "can_manage_clients"])
        try:
            with transaction.atomic():
                Account.objects.bulk_create(accounts_to_create)
            # Manually send post save signals to create related AccountCreation objects as bulk_create does not send signals
            for account in accounts_to_create:
                post_save.send(Account, instance=account, created=True)
        except Exception as e:
            for account in accounts_to_create:
                account.save()
        all_managed_accounts = [cid.id for cid in accounts_to_update] + [cid.id for cid in accounts_to_create]
        self.mcc_account.managers.add(*Account.objects.filter(id__in=all_managed_accounts))
        self.mcc_account.update_time = timezone.now()
        self.mcc_account.save()

    def get_client_customer_accounts(self, client=None):
        """
        Retrieve all client customer accounts
            Optional client to handle separate processing of response outside of update method
        :param client: Google ads client
        :return:
        """
        if client:
            self.ga_service = client.get_service("GoogleAdsService", version="v2")
        query_fields = self.format_query(CUSTOMER_CLIENT_ACCOUNT_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE customer_client.hidden != TRUE"
        customer_client_accounts = self.ga_service.search(self.mcc_account.id, query=query)
        return customer_client_accounts

    @staticmethod
    def get_accessible_customers(client, full_data=True):
        """
        Retrieve resource names of only directly accessible customers
        :param client: GoogleAds client
        :param full_data: bool -> Get full customer data
        :return: list -> [str]
        """
        customer_service = client.get_service("CustomerService", version="v2")
        accessible_customers = customer_service.list_accessible_customers()
        customer_data = accessible_customers.resource_names
        if full_data:
            details = []
            for resource in customer_data:
                try:
                    data = customer_service.get_customer(resource)
                    details.append(data)
                except GoogleAdsException as e:
                    logger.error(f"Error getting accessible customer: {e}")
                    continue
            customer_data = details
        return customer_data

