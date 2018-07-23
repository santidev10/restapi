import re

from django.db import models

from aw_reporting.models.ad_words.calculations import CALCULATED_STATS
from aw_reporting.models.ad_words.constants import VIEW_RATE_STATS, AgeRange, Gender, Parent, Device
from aw_reporting.models.base import BaseModel


class BaseStatisticModel(BaseModel):
    impressions = models.IntegerField(default=0)
    video_views = models.IntegerField(default=0)
    clicks = models.IntegerField(default=0)
    cost = models.FloatField(default=0)
    conversions = models.FloatField(default=0)
    all_conversions = models.FloatField(default=0)
    view_through = models.IntegerField(default=0)
    video_views_25_quartile = models.FloatField(default=0)
    video_views_50_quartile = models.FloatField(default=0)
    video_views_75_quartile = models.FloatField(default=0)
    video_views_100_quartile = models.FloatField(default=0)

    class Meta:
        abstract = True

    def __getattr__(self, name):
        if name in CALCULATED_STATS:
            data = CALCULATED_STATS[name]
            dependencies = data['args']
            receipt = data['receipt']
            return receipt(
                *[getattr(self, stat_name)
                  for stat_name in dependencies]
            )
        elif name in VIEW_RATE_STATS:
            quart = re.findall(r'\d+', name)[0]
            quart_views = getattr(self, 'video_views_%s_quartile' % quart)
            impressions = self.impressions
            return quart_views / impressions * 100 \
                if impressions else None
        else:
            raise AttributeError(self, name)


class ModelPlusDeNormFields(BaseStatisticModel):
    # for now we will use them in Pricing Tool
    de_norm_fields_are_recalculated = models.BooleanField(default=False)
    min_stat_date = models.DateField(null=True)
    max_stat_date = models.DateField(null=True)

    gender_undetermined = models.BooleanField(default=False)
    gender_male = models.BooleanField(default=False)
    gender_female = models.BooleanField(default=False)

    parent_parent = models.BooleanField(default=False)
    parent_not_parent = models.BooleanField(default=False)
    parent_undetermined = models.BooleanField(default=False)

    age_undetermined = models.BooleanField(default=False)
    age_18_24 = models.BooleanField(default=False)
    age_25_34 = models.BooleanField(default=False)
    age_35_44 = models.BooleanField(default=False)
    age_45_54 = models.BooleanField(default=False)
    age_55_64 = models.BooleanField(default=False)
    age_65 = models.BooleanField(default=False)

    device_computers = models.BooleanField(default=False)
    device_mobile = models.BooleanField(default=False)
    device_tablets = models.BooleanField(default=False)
    device_other = models.BooleanField(default=False)

    has_interests = models.BooleanField(default=False)
    has_keywords = models.BooleanField(default=False)
    has_channels = models.BooleanField(default=False)
    has_videos = models.BooleanField(default=False)
    has_remarketing = models.BooleanField(default=False)
    has_topics = models.BooleanField(default=False)

    class Meta:
        abstract = True


def _field_name_dict(pairs):
    return {key: field.field_name for key, field in pairs}


class ModelDenormalizedFields:
    AGES = _field_name_dict((
        (AgeRange.UNDETERMINED, ModelPlusDeNormFields.age_undetermined),
        (AgeRange.AGE_18_24, ModelPlusDeNormFields.age_18_24),
        (AgeRange.AGE_25_34, ModelPlusDeNormFields.age_25_34),
        (AgeRange.AGE_35_44, ModelPlusDeNormFields.age_35_44),
        (AgeRange.AGE_45_54, ModelPlusDeNormFields.age_45_54),
        (AgeRange.AGE_55_64, ModelPlusDeNormFields.age_55_64),
        (AgeRange.AGE_65_UP, ModelPlusDeNormFields.age_65),
    ))
    GENDERS = _field_name_dict((
        (Gender.UNDETERMINED, ModelPlusDeNormFields.gender_undetermined),
        (Gender.MALE, ModelPlusDeNormFields.gender_male),
        (Gender.FEMALE, ModelPlusDeNormFields.gender_female),
    ))
    PARENTS = _field_name_dict((
        (Parent.UNDETERMINED, ModelPlusDeNormFields.parent_undetermined),
        (Parent.PARENT, ModelPlusDeNormFields.parent_parent),
        (Parent.NOT_A_PARENT, ModelPlusDeNormFields.parent_not_parent),
    ))
    DEVICES = _field_name_dict((
        (Device.COMPUTER, ModelPlusDeNormFields.device_computers),
        (Device.MOBILE, ModelPlusDeNormFields.device_mobile),
        (Device.TABLET, ModelPlusDeNormFields.device_tablets),
        (Device.OTHER, ModelPlusDeNormFields.device_other),
    ))
