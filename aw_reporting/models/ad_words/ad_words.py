from django.conf import settings
from django.db import models

from aw_reporting.models.ad_words.campaign import Campaign
from aw_reporting.models.ad_words.statistic import BaseStatisticModel
from aw_reporting.models.base import BaseModel

DEFAULT_TIMEZONE = settings.DEFAULT_TIMEZONE


class CampaignTypeId:
    DISPLAY = "display"
    MULTI_CHANNEL = "multi_channel"
    SEARCH = "search"
    SHOPPING = "shopping"
    VIDEO = "video"


_campaign_type_map = dict((
    (CampaignTypeId.DISPLAY, "Display"),
    (CampaignTypeId.MULTI_CHANNEL, "Multi Channel"),
    (CampaignTypeId.SEARCH, "Search"),
    (CampaignTypeId.SHOPPING, "Shopping"),
    (CampaignTypeId.VIDEO, "Video"),
))


def campaign_type_str(campaign_type_id):
    return _campaign_type_map.get(campaign_type_id, "Unknown type")


class VideoCreative(BaseStatisticModel):
    id = models.CharField(max_length=255, primary_key=True)
    duration = models.IntegerField(null=True)


class GeoTarget(models.Model):
    name = models.CharField(max_length=100)
    canonical_name = models.CharField(max_length=100)
    parent = models.ForeignKey('self', null=True)
    country_code = models.CharField(max_length=2)
    target_type = models.CharField(max_length=50)
    status = models.CharField(max_length=20)

    def __str__(self):
        return "%s" % self.canonical_name


class Topic(models.Model):
    parent = models.ForeignKey('self', null=True, related_name='children')
    name = models.CharField(max_length=150, db_index=True)

    def __str__(self):
        return self.name


class Audience(BaseModel):
    parent = models.ForeignKey('self', null=True, related_name='children')
    name = models.CharField(max_length=150)
    type = models.CharField(max_length=25, db_index=True)

    CUSTOM_AFFINITY_TYPE = "custom-affinity"
    AFFINITY_TYPE = "affinity"
    IN_MARKET_TYPE = "in-market"
    TYPES = (CUSTOM_AFFINITY_TYPE, AFFINITY_TYPE, IN_MARKET_TYPE)

    def __str__(self):
        return "%s" % self.name


class RemarkList(BaseModel):
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250)

    def __str__(self):
        return self.name


class CampaignAgeRangeTargeting(models.Model):
    age_range_id = models.SmallIntegerField()
    campaign = models.ForeignKey(Campaign, related_name="age_range_targeting")

    class Meta:
        unique_together = (("age_range_id", "campaign"),)


class CampaignGenderTargeting(models.Model):
    gender_id = models.SmallIntegerField()
    campaign = models.ForeignKey(Campaign, related_name="gender_targeting")

    class Meta:
        unique_together = (("gender_id", "campaign"),)


class CampaignLocationTargeting(models.Model):
    location = models.ForeignKey(GeoTarget)
    campaign = models.ForeignKey(Campaign, related_name="location_targeting")

    class Meta:
        unique_together = (("location", "campaign"),)
