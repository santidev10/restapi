from aw_reporting.adwords_api import get_web_app_client, get_all_customers

from aw_reporting.models import Account
from suds import WebFault
import logging
logger = logging.getLogger(__name__)


class AWDataLoader:

    advertising_update_tasks = (
        "save_campaigns",
        "save_ad_groups",

        "save_videos",
        "save_ads",

        "save_genders",
        "save_age_ranges",

        "save_placements",
        "save_keywords",
        "save_topics",
        "save_interests",

        "save_cities",
    )

    def __init__(self, account):
        self.account = account

    def full_update(self):
        if self.account.can_manage_clients:
            self.mcc_full_update()
        else:
            self.advertising_full_update()

    def mcc_full_update(self):
        permissions = self.account.mcc_permissions.filter(can_read=True)
        for permission in permissions:
            try:
                client = get_web_app_client(
                    refresh_token=permission.aw_connection.refresh_token,
                    client_customer_id=permission.account_id,
                )
                accounts = get_all_customers(client)
            except WebFault as e:
                if "AuthorizationError.USER_PERMISSION_DENIED" in\
                        e.fault.faultstring:
                    permission.can_read = False
                    permission.save()
                else:
                    raise
            else:
                for e in accounts:
                    a, created = Account.objects.update_or_create(
                        id=e['customerId'],
                        defaults=dict(
                            name=e['name'],
                            currency_code=e['currencyCode'],
                            timezone=e['dateTimeZone'],
                        )
                    )
                    a.managers.add(self.account)
                break

    def advertising_full_update(self):
        managers = self.account.managers.all()
        for manager in managers:
            permissions = manager.mcc_permissions.filter(can_read=True)
            for permission in permissions:
                pass


