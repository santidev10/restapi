from django.contrib.auth import get_user_model
from django.db import models

from utils.models import Timestampable
from .constants import OAUTH_CHOICES
from .constants import ENTITY_STATUS_CHOICES


class OAuthBase(Timestampable):
    oauth_type = models.IntegerField(db_index=True, choices=OAUTH_CHOICES)

    class Meta:
        abstract = True


class OAuthAccount(OAuthBase):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    name = models.CharField(max_length=255, null=True, db_index=True)
    email = models.EmailField(max_length=255, null=True, db_index=True)
    token = models.CharField(null=True, max_length=255)
    refresh_token = models.CharField(null=True, max_length=150)
    revoked_access = models.BooleanField(default=False, db_index=True)


class Campaign(OAuthBase):
    id = models.BigAutoField(primary_key=True)
    account = models.ForeignKey(OAuthAccount, on_delete=models.CASCADE)
    name = models.CharField(max_length=255, null=True, db_index=True)
    impressions = models.IntegerField(default=0, db_index=True)
    video_views = models.IntegerField(default=0, db_index=True)
    cost = models.FloatField(default=0, db_index=True)
    cpm = models.FloatField(default=0, db_index=True)
    cpv = models.FloatField(default=0, db_index=True)
    ctr = models.FloatField(default=0, db_index=True)
    active_view_viewability = models.FloatField(default=0, db_index=True)
    video_quartile_100_rate = models.FloatField(default=0, db_index=True)


class DV360Base(Timestampable):
    id = models.BigIntegerField(db_index=True, primary_key=True)
    name = models.CharField(max_length=250)
    display_name = models.CharField(max_length=250)
    update_time = models.DateTimeField()
    entity_status = models.SmallIntegerField(db_index=True, choices=ENTITY_STATUS_CHOICES)

    class Meta:
        abstract = True


class DV360Partner(DV360Base):
    oauth_accounts = models.ManyToManyField(OAuthAccount, related_name="dv360_partners")


class DV360Advertiser(DV360Base):
    partner = models.ForeignKey(DV360Partner, on_delete=models.CASCADE, related_name="advertisers")


class DV360Campaign(DV360Base):
    advertiser = models.ForeignKey(DV360Advertiser, on_delete=models.CASCADE, related_name="campaigns")
