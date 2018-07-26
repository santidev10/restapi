import logging
from functools import partial

from django.conf import settings
from django.core.management.base import BaseCommand
from pytz import timezone
from pytz import utc
from suds import WebFault

from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.tasks import detect_success_aw_read_permissions
from aw_reporting.tasks import max_ready_date
from aw_reporting.tasks import recalculate_de_norm_fields
from aw_reporting.utils import command_single_process_lock
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument(
            '--forced',
            dest='forced',
            default=False,
            action='store_true',
            help='Forced update of all accounts'
        )

        parser.add_argument(
            '--start',
            dest='start',
            help='Start from... options: %s' % ", ".join(
                m.__name__ for m in AWDataLoader.advertising_update_tasks)
        )

        parser.add_argument(
            '--end',
            dest='end',
            help='Last method... options: %s' % ", ".join(
                m.__name__ for m in AWDataLoader.advertising_update_tasks)
        )

    def pre_process(self):
        if not settings.IS_TEST:
            self.create_cf_account_connection()
        detect_success_aw_read_permissions()

    @staticmethod
    def post_process():
        add_relation_between_report_and_creation_campaigns()
        add_relation_between_report_and_creation_ad_groups()
        add_relation_between_report_and_creation_ads()
        recalculate_de_norm_fields()

    @command_single_process_lock("aw_main_update")
    def handle(self, *args, **options):
        self.pre_process()

        now = now_in_default_tz(utc)
        today = now.today()
        forced = options.get("forced")
        start = options.get("start")
        end = options.get("end")

        update_account_fn = partial(self._update_accounts, today=today, forced=forced, start=start, end=end)

        update_account_fn(is_mcc=True)
        update_account_fn(is_mcc=False)

        self.post_process()

    def _update_accounts(self, today, forced, start, end, is_mcc: bool):
        from aw_reporting.models import Account
        updater = AWDataLoader(today, start=start, end=end)
        accounts = Account.objects.filter(can_manage_clients=is_mcc)
        accounts_to_update = self._filtered_accounts_generator(accounts, forced)
        for account in accounts_to_update:
            logger.info("%s update: %s", self._get_account_type_str(is_mcc), account)
            updater.full_update(account)

    def _get_account_type_str(self, is_mcc):
        return "MCC" if is_mcc else "Customer"

    def _filtered_accounts_generator(self, queryset, forced):
        if forced:
            return queryset
        now = now_in_default_tz(utc)
        for account in queryset:
            tz = timezone(account.timezone)
            if not account.update_time or max_ready_date(account.update_time, tz) < max_ready_date(now, tz):
                yield account

    @staticmethod
    def create_cf_account_connection():
        from aw_reporting.models import AWConnection, Account, AWAccountPermission
        from aw_reporting.adwords_api import load_web_app_settings, get_customers

        settings = load_web_app_settings()
        connection, created = AWConnection.objects.update_or_create(
            email="promopushmaster@gmail.com",
            defaults=dict(refresh_token=settings['cf_refresh_token']),
        )
        if created:
            try:
                customers = get_customers(
                    connection.refresh_token,
                    **load_web_app_settings()
                )
            except WebFault as e:
                logger.critical(e)
            else:
                mcc_accounts = [c for c in customers
                                if c['canManageClients'] and not c['testAccount']]
                for ac_data in mcc_accounts:
                    data = dict(
                        id=ac_data['customerId'],
                        name=ac_data['descriptiveName'],
                        currency_code=ac_data['currencyCode'],
                        timezone=ac_data['dateTimeZone'],
                        can_manage_clients=ac_data['canManageClients'],
                        is_test_account=ac_data['testAccount'],
                    )
                    obj, _ = Account.objects.get_or_create(
                        id=data['id'], defaults=data,
                    )
                    AWAccountPermission.objects.get_or_create(
                        aw_connection=connection, account=obj,
                    )
