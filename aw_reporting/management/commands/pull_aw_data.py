from django.core.management.base import BaseCommand
from django.db.models import Q
from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.tasks import detect_success_aw_read_permissions, recalculate_de_norm_fields
from aw_reporting.utils import command_single_process_lock
from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from suds import WebFault
from datetime import datetime
from pytz import timezone, utc
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def pre_process(self):
        detect_success_aw_read_permissions()
        self.create_cf_account_connection()

    @staticmethod
    def post_process():
        add_relation_between_report_and_creation_campaigns()
        add_relation_between_report_and_creation_ad_groups()
        add_relation_between_report_and_creation_ads()

    @command_single_process_lock("aw_main_update")
    def handle(self, *args, **options):
        from aw_reporting.models import Account
        timezones = Account.objects.filter(timezone__isnull=False).values_list(
            "timezone", flat=True).order_by("timezone").distinct()

        now = datetime.now(tz=utc)
        today = now.date()
        timezones = [
            t for t in timezones
            if now.astimezone(timezone(t)).hour > 5
        ]
        logger.info("Timezones: {}".format(timezones))

        # first we will update accounts based on MCC timezone
        mcc_to_update = Account.objects.filter(
            timezone__in=timezones,
            can_manage_clients=True,
        ).filter(
            Q(update_time__date__lt=today) | Q(update_time__isnull=True)
        )
        updater = AWDataLoader(today)
        for mcc in mcc_to_update:
            logger.info("MCC update: {}".format(mcc))
            updater.full_update(mcc)

        # 2) update all the advertising accounts
        accounts_to_update = Account.objects.filter(
            timezone__in=timezones,
            can_manage_clients=False,
        ).filter(
           Q(update_time__date__lt=today) | Q(update_time__isnull=True)
        )
        for account in accounts_to_update:
            logger.info("Customer account update: {}".format(account))
            updater.full_update(account)

        recalculate_de_norm_fields()

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
                mcc_accounts = list(filter(
                    lambda i: i['canManageClients'] and not i['testAccount'],
                    customers,
                ))
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
