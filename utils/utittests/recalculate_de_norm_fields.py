from aw_reporting.models import Account
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account


def recalculate_de_norm_fields():
    for account_id in Account.objects.all().values_list("id", flat=True):
        recalculate_de_norm_fields_for_account(account_id)
