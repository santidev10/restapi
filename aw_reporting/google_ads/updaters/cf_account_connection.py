import logging

from aw_reporting.google_ads.constants import CUSTOMER_CLIENT_ACCOUNT_FIELDS
from aw_reporting.google_ads.google_ads_api import get_client
from aw_reporting.google_ads.google_ads_api import load_settings
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import Account

logger = logging.getLogger(__name__)


class CFAccountConnector(UpdateMixin):
    RESOURCE_NAME = "customer_client"
    """
    Creates AWConnection between promopush account and all directly manageable accounts
    """

    def __init__(self, email="promopushmaster@gmail.com"):
        self.email = email
        self.google_ads_settings = load_settings()
        self.connection, self.created = AWConnection.objects.update_or_create(
            email="promopushmaster@gmail.com",
            defaults=dict(refresh_token=self.google_ads_settings["refresh_token"]),
        )

    def update(self):
        if self.created:
            try:
                client = get_client()
                customer_service = client.get_service("CustomerService", version="v2")
                customers = customer_service.list_accessible_customers()
                ga_service = client.get_service("GoogleAdsService", version="v2")

            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                logger.critical(f"Unable to get client customers in CFAccountConnector: {e}")
            else:
                customer_ids = ",".join([row.split("/")[-1] for row in customers.resource_names])
                accessible_customers = self.get_customer_data(ga_service, customer_ids)
                for row in accessible_customers:
                    data = {
                        "id": row.customer_client.id.value,
                        "name": row.customer_client.descriptive_name.value,
                        "currency_code": row.customer_client.currency_code.value,
                        "timezone": row.customer_client.time_zone.value,
                        "can_manage_clients": row.customer_client.manager.value,
                        "is_test_account": row.customer_client.test_account.value
                    }
                    account, _ = Account.objects.get_or_create(id=data["id"], defaults=data)
                    AWAccountPermission.objects.get_or_create(aw_connection=self.connection, account=account)

    def get_customer_data(self, ga_service, customer_ids):
        query_fields = self.format_query(CUSTOMER_CLIENT_ACCOUNT_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE customer_client.id IN ({customer_ids})"
        customer_data = ga_service.search(str(self.google_ads_settings["login_customer_id"]), query=query)
        return customer_data
