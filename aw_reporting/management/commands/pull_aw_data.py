import logging

from django.core.management.base import BaseCommand
from django.db.models import Q
from pytz import timezone, utc
from suds import WebFault

from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.tasks import detect_success_aw_read_permissions, \
    recalculate_de_norm_fields
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
        detect_success_aw_read_permissions()
        self.create_cf_account_connection()

    @staticmethod
    def post_process():
        add_relation_between_report_and_creation_campaigns()
        add_relation_between_report_and_creation_ad_groups()
        add_relation_between_report_and_creation_ads()
        recalculate_de_norm_fields()

    @command_single_process_lock("aw_main_update")
    def handle(self, *args, **options):
        from aw_reporting.models import Account
        self.pre_process()
        timezones = Account.objects.filter(timezone__isnull=False).values_list(
            "timezone", flat=True).order_by("timezone").distinct()

        now = now_in_default_tz(utc)
        today = now.date()
        timezones = [
            t for t in timezones
            if now.astimezone(timezone(t)).hour > 5
        ]
        logger.info("Timezones: %s", timezones)

        # first we will update accounts based on MCC timezone
        mcc_to_update = Account.objects.filter(
            timezone__in=timezones,
            can_manage_clients=True
        )
        if not options.get('forced'):
            mcc_to_update = mcc_to_update.filter(
                Q(update_time__date__lt=today) | Q(update_time__isnull=True)
            )
        updater = AWDataLoader(today, start=options.get("start"),
                               end=options.get("end"))
        for mcc in mcc_to_update:
            logger.info("MCC update: %s", mcc)
            updater.full_update(mcc)

        # 2) update all the advertising accounts
        accounts_to_update = Account.objects.filter(
            timezone__in=timezones,
            can_manage_clients=False
        )
        if not options.get('forced'):
            accounts_to_update = accounts_to_update.filter(
                Q(update_time__date__lt=today) | Q(update_time__isnull=True)
            )
        for account in accounts_to_update:
            logger.info("Customer account update: %s", account)
            updater.full_update(account)

        self.post_process()

    @staticmethod
    def create_cf_account_connection():
        from aw_reporting.models import AWConnection, Account, \
            AWAccountPermission
        from aw_reporting.adwords_api import load_web_app_settings, \
            get_customers

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
