from aw_reporting.adwords_api import get_web_app_client, get_all_customers

from aw_reporting.models import Account
from suds import WebFault
from oauth2client.client import HttpAccessTokenRefreshError
from aw_reporting import tasks
import logging
logger = logging.getLogger(__name__)


class AWDataLoader:

    advertising_update_tasks = (
        tasks.get_campaigns,
        tasks.get_ad_groups,

        tasks.get_videos,
        tasks.get_ads,

        tasks.get_genders,
        tasks.get_age_ranges,

        tasks.get_placements,
        tasks.get_keywords,
        tasks.get_topics,
        tasks.get_interests,

        tasks.get_cities,
    )

    def full_update(self, account):
        if account.can_manage_clients:
            self.mcc_full_update(account)
        else:
            self.run_task_with_any_manager(
                self.advertising_account_update,
                account,
            )

    def mcc_full_update(self, manager):
        accounts = self.run_task_with_any_permission(
            lambda c, *_: get_all_customers(c),
            manager, manager
        )
        if accounts:
            for e in accounts:
                a, created = Account.objects.update_or_create(
                    id=e['customerId'],
                    defaults=dict(
                        name=e['name'],
                        currency_code=e['currencyCode'],
                        timezone=e['dateTimeZone'],
                    )
                )
                a.managers.add(manager)

    @staticmethod
    def run_task_with_any_permission(task, account, manager):
        permissions = manager.mcc_permissions.filter(
            can_read=True, aw_connection__revoked_access=False,
        )
        for permission in permissions:
            aw_connection = permission.aw_connection
            try:
                client = get_web_app_client(
                    refresh_token=aw_connection.refresh_token,
                    client_customer_id=account.id,
                )
                result = task(client, account)
            except HttpAccessTokenRefreshError as e:
                logger.warning((permission, e))
                aw_connection.revoked_access = True
                aw_connection.save()

            except WebFault as e:
                if "AuthorizationError.USER_PERMISSION_DENIED" in\
                        e.fault.faultstring:
                    logger.warning((permission, e))
                    permission.can_read = False
                    permission.save()
                else:
                    raise
            else:
                return result

    def run_task_with_any_manager(self, task, account):
        managers = account.managers.all()
        for manager in managers:
            result = self.run_task_with_any_permission(
                task, account, manager
            )
            if result is not None:
                return result

    def advertising_account_update(self, client, account):

        for task in self.advertising_update_tasks:
            task(client, account)



