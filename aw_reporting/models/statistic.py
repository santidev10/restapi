from django.db import models

from aw_reporting.models.ad_words import Ad
from aw_reporting.models.ad_words import AdGroup
from aw_reporting.models.ad_words import Audience
from aw_reporting.models.ad_words import BaseStatisticModel
from aw_reporting.models.ad_words import Campaign
from aw_reporting.models.ad_words import Device
from aw_reporting.models.ad_words import GeoTarget
from aw_reporting.models.ad_words import RemarkList
from aw_reporting.models.ad_words import Topic
from aw_reporting.models.ad_words import VideoCreative
from aw_reporting.models.ad_words.statistic import BaseClicksTypesStatisticsModel

ParentStatuses = ("Parent", "Not a parent", "Undetermined")

PLACEMENT_STATISTIC_TYPES = [
    (0, "Managed"),  # Managed placement
    (1, "Group")  # Automatic placement
]


class DailyStatisticModel(BaseStatisticModel):
    date = models.DateField(db_index=True)

    class Meta:
        abstract = True


class DeviceDailyStatisticModel(DailyStatisticModel):
    device_id = models.SmallIntegerField(default=Device.COMPUTER, db_index=True)

    class Meta:
        abstract = True


class AdGroupStatistic(DeviceDailyStatisticModel, BaseClicksTypesStatisticsModel):
    ad_group = models.ForeignKey(AdGroup, related_name="statistics", on_delete=models.CASCADE)
    ad_network = models.CharField(max_length=20, db_index=True)
    average_position = models.DecimalField(max_digits=6, decimal_places=2)
    engagements = models.IntegerField(default=0)
    active_view_impressions = models.IntegerField(default=0)

    class Meta:
        unique_together = (
            ("ad_group", "date", "device_id", "ad_network"),
        )
        ordering = ["ad_group", "date", "device_id", "ad_network"]


class AdGroupGeoViewStatistic(models.Model):
    """
    adgroup stats segmented from geographic view on google ads report api
    """
    # segments
    ad_group = models.ForeignKey(AdGroup, related_name="geo_view_stats", on_delete=models.CASCADE)
    date = models.DateField(db_index=True)
    device_id = models.SmallIntegerField(default=Device.COMPUTER, db_index=True)
    country = models.ForeignKey(GeoTarget, related_name="country_geo_view_stats", on_delete=models.CASCADE)
    region = models.ForeignKey(GeoTarget, related_name="region_geo_view_stats", on_delete=models.CASCADE)
    metro = models.ForeignKey(GeoTarget, related_name="metro_geo_view_stats", on_delete=models.CASCADE)
    # metrics
    impressions = models.IntegerField(default=0, db_index=True)
    video_views = models.IntegerField(default=0, db_index=True)
    clicks = models.IntegerField(default=0, db_index=True)
    cost = models.FloatField(default=0, db_index=True)
    conversions = models.FloatField(default=0, db_index=True)
    all_conversions = models.FloatField(default=0, db_index=True)

    class Meta:
        unique_together = (
            ("ad_group", "date", "device_id", "country", "region", "metro"),
        )
        ordering = ["ad_group", "date", "device_id"]


class AdStatistic(DailyStatisticModel, BaseClicksTypesStatisticsModel):
    ad = models.ForeignKey(Ad, related_name="statistics", on_delete=models.CASCADE)
    average_position = models.DecimalField(max_digits=6, decimal_places=2)

    class Meta:
        unique_together = (("ad", "date"),)
        ordering = ["-date"]


class KeywordStatistic(DailyStatisticModel, BaseClicksTypesStatisticsModel):
    keyword = models.CharField(max_length=150, db_index=True)
    ad_group = models.ForeignKey(AdGroup, related_name="keywords", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("keyword", "ad_group", "date"),)
        ordering = ["-date"]


class TopicStatistic(DailyStatisticModel, BaseClicksTypesStatisticsModel):
    topic = models.ForeignKey(Topic, on_delete=models.CASCADE)
    ad_group = models.ForeignKey(AdGroup, related_name="topics", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("topic", "ad_group", "date"),)
        ordering = ["-date"]

    def __str__(self):
        return "%s %s" % (self.topic_id, self.date)


class AudienceStatistic(DailyStatisticModel, BaseClicksTypesStatisticsModel):
    audience = models.ForeignKey(Audience, on_delete=models.CASCADE)
    ad_group = models.ForeignKey(AdGroup, related_name="audiences", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("audience", "ad_group", "date"),)
        ordering = ["-date"]


class RemarkStatistic(DailyStatisticModel, BaseClicksTypesStatisticsModel):
    remark = models.ForeignKey(RemarkList, on_delete=models.CASCADE)
    ad_group = models.ForeignKey(
        AdGroup, related_name="remark_statistic", on_delete=models.CASCADE
    )

    class Meta:
        unique_together = (("remark", "ad_group", "date"),)
        ordering = ["-date"]


class AgeRangeStatistic(DailyStatisticModel, BaseClicksTypesStatisticsModel):
    age_range_id = models.SmallIntegerField(default=0, db_index=True)
    ad_group = models.ForeignKey(AdGroup, related_name="age_statistics", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("age_range_id", "ad_group", "date"),)
        ordering = ["-date"]


class GenderStatistic(DailyStatisticModel, BaseClicksTypesStatisticsModel):
    gender_id = models.SmallIntegerField(default=0, db_index=True)
    ad_group = models.ForeignKey(AdGroup, related_name="gender_statistics", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("gender_id", "ad_group", "date"),)
        ordering = ["-date"]


class CityStatistic(DailyStatisticModel):
    city = models.ForeignKey(GeoTarget, related_name="aw_stats", on_delete=models.CASCADE)
    ad_group = models.ForeignKey(AdGroup, related_name="cities_stats", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("city", "ad_group", "date"),)
        ordering = ["-date"]


class YTVideoStatistic(DeviceDailyStatisticModel):
    yt_id = models.CharField(max_length=30, db_index=True)
    ad_group = models.ForeignKey(AdGroup,
                                 related_name="managed_video_statistics",
                                 on_delete=models.CASCADE)
    placement_type = models.IntegerField(default=0, choices=PLACEMENT_STATISTIC_TYPES, db_index=True)

    class Meta:
        unique_together = (("ad_group", "yt_id", "device_id", "date", "placement_type"),)
        ordering = ["ad_group", "yt_id", "device_id", "date"]


class YTChannelStatistic(DeviceDailyStatisticModel):
    yt_id = models.CharField(max_length=30, db_index=True)
    ad_group = models.ForeignKey(AdGroup,
                                 related_name="channel_statistics",
                                 on_delete=models.CASCADE)
    placement_type = models.IntegerField(default=0, choices=PLACEMENT_STATISTIC_TYPES, db_index=True)

    class Meta:
        unique_together = (("ad_group", "yt_id", "device_id", "date", "placement_type"),)
        ordering = ["ad_group", "yt_id", "device_id", "date"]


class VideoCreativeStatistic(DailyStatisticModel):
    creative = models.ForeignKey(VideoCreative, related_name="statistics", on_delete=models.CASCADE)
    ad_group = models.ForeignKey(AdGroup, related_name="videos_stats", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("ad_group", "creative", "date"),)
        ordering = ["ad_group", "creative", "date"]


# Hourly stats
class CampaignHourlyStatistic(models.Model):
    id = models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")
    date = models.DateField()
    hour = models.PositiveSmallIntegerField()
    campaign = models.ForeignKey(Campaign,
                                 related_name="hourly_statistics",
                                 on_delete=models.CASCADE)

    impressions = models.IntegerField(default=0)
    video_views = models.IntegerField(default=0)
    clicks = models.IntegerField(default=0)
    cost = models.FloatField(default=0)

    class Meta:
        unique_together = (("campaign", "date", "hour"),)
        ordering = ["campaign", "date", "hour"]


class CampaignStatistic(DeviceDailyStatisticModel, BaseClicksTypesStatisticsModel):
    campaign = models.ForeignKey(Campaign, related_name="statistics", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("campaign", "date", "device_id"),)
        ordering = ["-date"]

    def __str__(self):
        return "%s %s" % (self.campaign.name, self.date)


class ParentStatistic(DailyStatisticModel):
    parent_status_id = models.SmallIntegerField(default=0, db_index=True)
    ad_group = models.ForeignKey(AdGroup, related_name="parent_statistics", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("parent_status_id", "ad_group", "date"),)
        ordering = ["-date"]

    def __str__(self):
        return "%s %s" % (self.parent_status, self.date)

    @property
    def parent_status(self):
        return ParentStatuses[int(self.parent_status_id)]


class GeoTargeting(BaseStatisticModel):
    """
    A model for geo targeting settings at the campaign level
    """
    campaign = models.ForeignKey(Campaign, related_name="geo_performance", on_delete=models.CASCADE)
    geo_target = models.ForeignKey(GeoTarget, related_name="geo_performance", on_delete=models.CASCADE)
    is_negative = models.BooleanField(default=False)

    class Meta:
        unique_together = (("campaign", "geo_target"),)
        ordering = ["campaign", "geo_target"]

    def __str__(self):
        return "%s %s" % (self.campaign, self.geo_target)
