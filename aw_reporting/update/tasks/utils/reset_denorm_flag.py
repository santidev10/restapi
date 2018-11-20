from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign


def reset_denorm_flag(ad_group_ids=None, campaign_ids=None):
    if ad_group_ids:
        AdGroup.objects.filter(id__in=ad_group_ids) \
            .update(de_norm_fields_are_recalculated=False)
    if campaign_ids is None:
        campaign_ids = AdGroup.objects.filter(id__in=ad_group_ids) \
            .values_list("campaign_id", flat=True).distinct()
    Campaign.objects.filter(id__in=campaign_ids) \
        .update(de_norm_fields_are_recalculated=False)
