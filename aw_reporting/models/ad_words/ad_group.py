from django.db import models

from aw_reporting.models.ad_words.campaign import Campaign
from aw_reporting.models.ad_words.statistic import ModelPlusDeNormFields
from userprofile.managers import UserRelatedManager


class AdGroupManager(UserRelatedManager):
    _account_id_ref = "campaign__account_id"

    def get_queryset(self, ignore_user=True):
        return super(AdGroupManager, self).get_queryset(ignore_user=ignore_user)


class AdGroup(ModelPlusDeNormFields):
    objects = AdGroupManager()
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250)
    status = models.CharField(max_length=7, null=True)
    type = models.CharField(max_length=25, default="")
    campaign = models.ForeignKey(Campaign, related_name='ad_groups')
    engagements = models.IntegerField(default=0)
    active_view_impressions = models.IntegerField(default=0)
    cpv_bid = models.PositiveIntegerField(null=True)
    cpm_bid = models.PositiveIntegerField(null=True)
    cpc_bid = models.PositiveIntegerField(null=True)

    def __str__(self):
        return "%s %s" % (self.campaign.name, self.name)
