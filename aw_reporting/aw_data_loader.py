import logging

from django.utils import timezone
from oauth2client.client import HttpAccessTokenRefreshError
from suds import WebFault

import aw_reporting.update.tasks as aw_tasks
from aw_reporting.adwords_api import get_web_app_client, get_all_customers
from aw_reporting.adwords_reports import AccountInactiveError
from aw_reporting.models import Account
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
from utils.lang import safe_index

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
        aw_tasks.get_parents,
        aw_tasks.get_age_ranges,
        #
        aw_tasks.get_placements,
        aw_tasks.get_keywords,
        aw_tasks.get_topics,
        aw_tasks.get_interests,
        #
        aw_tasks.get_cities,
        aw_tasks.get_geo_targeting
    )

    def __init__(self, today, start=None, end=None):
        self.today = today
        self.aw_cached_clients = {}
        self.update_tasks = self._get_update_tasks(start, end)

    def _get_update_tasks(self, start, end):
        all_names = [m.__name__ for m in self.advertising_update_tasks]
        start_index = safe_index(all_names, start, 0)
        end_index = safe_index(all_names, end, len(all_names))
        return self.advertising_update_tasks[start_index:end_index + 1]

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
                a, _ = Account.objects.update_or_create(
                    id=e['customerId'],
                    defaults=dict(
                        name=e['name'],
                        currency_code=e['currencyCode'],
                        timezone=e['dateTimeZone'],
                    )
                )
                a.managers.add(manager)

            manager.update_time = timezone.now()
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
            except AccountInactiveError:
                account.is_active = False
                account.save()
            except HttpAccessTokenRefreshError as e:
                logger.warning((permission, e))
                aw_connection.revoked_access = True
                aw_connection.save()

            except WebFault as e:
                if "AuthorizationError.USER_PERMISSION_DENIED" in \
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
        for task in self.update_tasks:
            logger.debug("Task: %s, account: %s", task.__name__, account)
            task(client, account, today)

        account.update_time = timezone.now()
        account.save()
        recalculate_de_norm_fields_for_account(account.id)
