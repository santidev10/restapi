from django.db import models

from .creation import CampaignCreation, AdGroupCreation


class OptimizationTuning(models.Model):

    IMPRESSIONS_KPI = "impressions"
    VIEWS_KPI = "video_views"
    CLICKS_KPI = "clicks"
    COST_KPI = "cost"
    CTR_KPI = "ctr"
    CPV_KPI = "average_cpv"
    CPM_KPI = "average_cpm"
    VIEW_RATE_KPI = "video_view_rate"
    CONVERSIONS_KPI = "conversions"
    VIEW_THROUGH_KPI = "view_through"
    KPI_TYPES = (
        (IMPRESSIONS_KPI, IMPRESSIONS_KPI),
        (VIEWS_KPI, VIEWS_KPI),
        (CLICKS_KPI, CLICKS_KPI),
        (COST_KPI, COST_KPI),
        (CTR_KPI, CTR_KPI),
        (CPV_KPI, CPV_KPI),
        (VIEW_RATE_KPI, VIEW_RATE_KPI),
        (CONVERSIONS_KPI, CONVERSIONS_KPI),
        (VIEW_THROUGH_KPI, VIEW_THROUGH_KPI),
    )
    kpi = models.CharField(max_length=20, choices=KPI_TYPES)

    value = models.DecimalField(  # 9,999,999,999.999
        null=True, blank=True, max_digits=13, decimal_places=3,
    )

    class Meta:
        abstract = True
        unique_together = (('item', 'kpi',),)
        ordering = ['item']


class CampaignOptimizationTuning(OptimizationTuning):
    item = models.ForeignKey(
        CampaignCreation, related_name="optimization_tuning"
    )


class AdGroupOptimizationTuning(OptimizationTuning):
    item = models.ForeignKey(
        AdGroupCreation, related_name="optimization_tuning"
    )
