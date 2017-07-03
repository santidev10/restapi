from aw_reporting.adwords_api import get_web_app_client, get_all_customers

from aw_reporting.models import Account
from suds import WebFault
from oauth2client.client import HttpAccessTokenRefreshError
import aw_reporting.tasks as aw_tasks
from datetime import datetime
import logging
logger = logging.getLogger(__name__)


class AWDataLoader:

    advertising_update_tasks = (
        # get campaigns, ad-groups and ad-group daily stats
        aw_tasks.get_campaigns,
        aw_tasks.get_ad_groups_and_stats,


        aw_tasks.get_videos,
        aw_tasks.get_ads,
        #
        aw_tasks.get_genders,
        aw_tasks.get_age_ranges,
        #
        aw_tasks.get_placements,
        aw_tasks.get_keywords,
        aw_tasks.get_topics,
        aw_tasks.get_interests,
        #
        aw_tasks.get_cities,
    )

    def __init__(self, today):
        self.today = today
        self.aw_cached_clients = {}

    def get_aw_client(self, refresh_token, client_customer_id):
        if refresh_token in self.aw_cached_clients:
            client = self.aw_cached_clients[refresh_token]
        else:
            client = get_web_app_client(
                refresh_token=refresh_token,
                client_customer_id=client_customer_id,
            )
            self.aw_cached_clients[refresh_token] = client

        if client.client_customer_id != client_customer_id:
            client.SetClientCustomerId(client_customer_id)
        return client

    def full_update(self, account):
        if account.can_manage_clients:
            self.mcc_full_update(account)
        else:
            self.run_task_with_any_manager(
                self.advertising_account_update,
                account,
            )

    def mcc_full_update(self, manager):
        self.run_task_with_any_permission(
            self.save_all_customers,
            manager, manager
        )

    def save_all_customers(self, client, manager):
        accounts = get_all_customers(client)
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

            manager.update_time = datetime.now()
            manager.save()

    def run_task_with_any_permission(self, task, account, manager):
        permissions = manager.mcc_permissions.filter(
            can_read=True, aw_connection__revoked_access=False,
        )
        for permission in permissions:
            aw_connection = permission.aw_connection
            try:
                client = self.get_aw_client(aw_connection.refresh_token,
                                            account.id)
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
        today = self.today
        for task in self.advertising_update_tasks:
            logger.debug(task, account)
            task(client, account, today)

        account.update_time = datetime.now()
        account.save()



