from django.contrib.auth import get_user_model
from django.contrib.postgres.fields import JSONField
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

    def to_dict(self):
        d = {
            'campaign_type': OAUTH_CHOICES[self.oauth_type],
            'campaign_id': self.id,
            'campaign_name': self.name,
            'metrics': {
                'impressions': self.impressions,
                'video_views': self.video_views,
                'cost': self.cost,
                'cpm': self.cpm,
                'cpv': self.cpv,
                'ctr': self.ctr,
                'active_view_viewability': self.active_view_viewability,
                'video_quartile_100_rate': self.video_quartile_100_rate,
            }
        }
        return d

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
    oauth_accounts = models.ManyToManyField(OAuthAccount, related_name="dv360_advertisers")


class DV360Campaign(DV360Base):
    advertiser = models.ForeignKey(DV360Advertiser, on_delete=models.CASCADE, related_name="campaigns")

class IQCampaign(models.Model):
    created = models.DateTimeField(db_index=True, auto_now_add=True)
    params = JSONField(default=dict)
    results = JSONField(default=dict)
    started = models.DateTimeField(default=None, null=True, db_index=True)
    completed = models.DateTimeField(default=None, null=True, db_index=True)
    # campaign is the campaign object above, google ads or dv360, null if its a csv
    campaign = models.ForeignKey(Campaign, db_index=True, null=True, default=None)

    def to_dict(self):
        d = {
            'created': self.created,
            'started': self.started,
            'completed': self.completed,
            'campaign': self.campaign.to_dict() if self.campaign else None,
            'params': self.params,
            'results': self.results,
        }
        return d