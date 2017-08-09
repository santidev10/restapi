from django.db import models


class OptimizationSetting(models.Model):

    CTR_KPI = "ctr"
    CTR_V_KPI = "ctr_v"
    CPV_KPI = "average_cpv"
    CPM_KPI = "average_cpm"
    VIEW_RATE_KPI = "video_view_rate"
    KPI_TYPES = (
        CTR_V_KPI, CTR_KPI, CPV_KPI, VIEW_RATE_KPI, CPM_KPI,
    )

    average_cpv = models.DecimalField(null=True, blank=True, max_digits=6, decimal_places=3)
    average_cpm = models.DecimalField(null=True, blank=True, max_digits=6, decimal_places=3)
    video_view_rate = models.DecimalField(null=True, blank=True, max_digits=6, decimal_places=3)
    ctr = models.DecimalField(null=True, blank=True, max_digits=6, decimal_places=3)
    ctr_v = models.DecimalField(null=True, blank=True, max_digits=6, decimal_places=3)

    class Meta:
        abstract = True


class AccountOptimizationSetting(OptimizationSetting):
    item = models.OneToOneField('aw_creation.accountcreation', related_name="optimization_setting")


class CampaignOptimizationSetting(OptimizationSetting):
    item = models.OneToOneField('aw_creation.campaigncreation', related_name="optimization_setting")
