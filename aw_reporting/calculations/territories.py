from aw_reporting.models import Opportunity


def get_salesforce_territories():
    return Opportunity.objects \
        .filter(territory__isnull=False) \
        .order_by("territory") \
        .distinct() \
        .values_list('territory', flat=True)
