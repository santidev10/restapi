from django.db import models

from aw_reporting.models.ad_words.campaign import Campaign
from aw_reporting.models.ad_words.statistic import ModelPlusDeNormFields, BaseClicksTypesStatisticsModel
from userprofile.managers import UserRelatedManagerMixin


class AdGroupManager(models.Manager, UserRelatedManagerMixin):
    _account_id_ref = "campaign__account_id"


class AdGroup(ModelPlusDeNormFields, BaseClicksTypesStatisticsModel):
    objects = AdGroupManager()
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250)
    status = models.CharField(max_length=7, null=True)
    type = models.CharField(max_length=35, default="")
    campaign = models.ForeignKey(Campaign, related_name='ad_groups', on_delete=models.CASCADE)
    engagements = models.IntegerField(default=0)
    active_view_impressions = models.IntegerField(default=0)
    cpv_bid = models.PositiveIntegerField(null=True)
    cpm_bid = models.PositiveIntegerField(null=True)
    cpc_bid = models.PositiveIntegerField(null=True)

    def __str__(self):
        return "%s %s" % (self.campaign.name, self.name)
