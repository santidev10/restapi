from django.db import models

from aw_reporting.models.ad_words import BaseStatisticModel, Devices, AdGroup, \
    Ad, Topic, RemarkList, Genders, GeoTarget, VideoCreative, Campaign, \
    AgeRanges, Audience


class DailyStatisticModel(BaseStatisticModel):
    date = models.DateField(db_index=True)

    class Meta:
        abstract = True


class DeviceDailyStatisticModel(DailyStatisticModel):
    device_id = models.SmallIntegerField(default=0, db_index=True)

    class Meta:
        abstract = True

    @property
    def device(self):
        return Devices[int(self.device_id)]


class AdGroupStatistic(DeviceDailyStatisticModel):
    ad_group = models.ForeignKey(AdGroup, related_name='statistics')
    ad_network = models.CharField(max_length=20, db_index=True)
    average_position = models.DecimalField(max_digits=6, decimal_places=2)
    engagements = models.IntegerField(default=0)
    active_view_impressions = models.IntegerField(default=0)

    class Meta:
        unique_together = (
            ("ad_group", "date", "device_id", "ad_network"),
        )
        ordering = ["ad_group", "date", "device_id", "ad_network"]


class AdStatistic(DailyStatisticModel):
    ad = models.ForeignKey(Ad, related_name='statistics')
    average_position = models.DecimalField(max_digits=6, decimal_places=2)

    class Meta:
        unique_together = (("ad", "date"),)
        ordering = ['-date']


class KeywordStatistic(DailyStatisticModel):
    keyword = models.CharField(max_length=150, db_index=True)
    ad_group = models.ForeignKey(AdGroup, related_name='keywords')

    class Meta:
        unique_together = (("keyword", "ad_group", "date"),)
        ordering = ['-date']


class TopicStatistic(DailyStatisticModel):
    topic = models.ForeignKey(Topic)
    ad_group = models.ForeignKey(AdGroup, related_name='topics')

    class Meta:
        unique_together = (("topic", "ad_group", "date"),)
        ordering = ['-date']

    def __str__(self):
        return "%s %s" % (self.topic_id, self.date)


class AudienceStatistic(DailyStatisticModel):
    audience = models.ForeignKey(Audience)
    ad_group = models.ForeignKey(AdGroup, related_name='audiences')

    class Meta:
        unique_together = (("audience", "ad_group", "date"),)
        ordering = ['-date']


class RemarkStatistic(DailyStatisticModel):
    remark = models.ForeignKey(RemarkList)
    ad_group = models.ForeignKey(
        AdGroup, related_name='remark_statistic'
    )

    class Meta:
        unique_together = (("remark", "ad_group", "date"),)
        ordering = ['-date']


class AgeRangeStatistic(DailyStatisticModel):
    age_range_id = models.SmallIntegerField(default=0, db_index=True)
    ad_group = models.ForeignKey(AdGroup, related_name='age_statistics')

    class Meta:
        unique_together = (("age_range_id", "ad_group", "date"),)
        ordering = ['-date']

    @property
    def age_range(self):
        return AgeRanges[int(self.age_range_id)]


class GenderStatistic(DailyStatisticModel):
    gender_id = models.SmallIntegerField(default=0, db_index=True)
    ad_group = models.ForeignKey(AdGroup, related_name='gender_statistics')

    class Meta:
        unique_together = (("gender_id", "ad_group", "date"),)
        ordering = ['-date']

    @property
    def gender(self):
        return Genders[int(self.gender_id)]


class CityStatistic(DailyStatisticModel):
    city = models.ForeignKey(GeoTarget, related_name='aw_stats')
    ad_group = models.ForeignKey(AdGroup, related_name='cities_stats')

    class Meta:
        unique_together = (("city", "ad_group", "date"),)
        ordering = ['-date']


class YTVideoStatistic(DeviceDailyStatisticModel):
    yt_id = models.CharField(max_length=30, db_index=True)
    ad_group = models.ForeignKey(AdGroup,
                                 related_name='managed_video_statistics')

    class Meta:
        unique_together = (('ad_group', 'yt_id', 'device_id', 'date'),)
        ordering = ['ad_group', 'yt_id', 'device_id', 'date']


class YTChannelStatistic(DeviceDailyStatisticModel):
    yt_id = models.CharField(max_length=30, db_index=True)
    ad_group = models.ForeignKey(AdGroup,
                                 related_name='channel_statistics')

    class Meta:
        unique_together = (('ad_group', 'yt_id', 'device_id', 'date'),)
        ordering = ['ad_group', 'yt_id', 'device_id', 'date']


class VideoCreativeStatistic(DailyStatisticModel):
    creative = models.ForeignKey(VideoCreative, related_name="statistics")
    ad_group = models.ForeignKey(AdGroup, related_name='videos_stats')

    class Meta:
        unique_together = (("ad_group", "creative", "date"),)
        ordering = ["ad_group", "creative", "date"]


# Hourly stats
class CampaignHourlyStatistic(models.Model):
    date = models.DateField()
    hour = models.PositiveSmallIntegerField()
    campaign = models.ForeignKey(Campaign,
                                 related_name="hourly_statistics")

    impressions = models.IntegerField(default=0)
    video_views = models.IntegerField(default=0)
    clicks = models.IntegerField(default=0)
    cost = models.FloatField(default=0)

    class Meta:
        unique_together = (("campaign", "date", "hour"),)
        ordering = ["campaign", "date", "hour"]
