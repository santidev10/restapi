import logging

from django.core.management import call_command
from django.core.management.base import BaseCommand

from aw_reporting.models import Account
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AdStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import TopicStatistic

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument(
            "--account_ids",
            dest="account_ids",
            help="Account IDs to update as a comma separated string",
            type=str,
            default=None,
        )

    def handle(self, *args, **options):
        accounts = self._get_account_queryset(options)
        logger.info("Cleaning statistics for {} accounts".format(accounts.count()))
        self._reset_statistic(accounts)
        logger.info("Invoking pull_aw_data")
        call_command("pull_aw_data", **options)
        logger.info("Update complete")

    def _get_account_queryset(self, options):
        account_ids_str = options.get("account_ids")
        account_ids = account_ids_str.split(",") if account_ids_str is not None else None
        queryset = Account.objects.all()
        if account_ids is not None:
            queryset = queryset.filter(id__in=account_ids)
        return queryset

    def _reset_statistic(self, accounts):
        self._remove_statistic_records(accounts)
        self._reset_update_time(accounts)

    def _remove_statistic_records(self, accounts):
        models = (
            (AdGroupStatistic, ""),
            (AdStatistic, "ad__"),
            (AgeRangeStatistic, ""),
            (AudienceStatistic, ""),
            (GenderStatistic, ""),
            (KeywordStatistic, ""),
            (RemarkStatistic, ""),
            (TopicStatistic, ""),
        )

        for model, prefix in models:
            queryset = model.objects.filter(**{
                prefix + "ad_group__campaign__account__in": accounts
            })
            queryset.delete()

    def _reset_update_time(self, accounts):
        return accounts.update(update_time=None)
