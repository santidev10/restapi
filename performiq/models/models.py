from django.contrib.auth import get_user_model
from django.db import models

from oauth.models import Campaign


class IQCampaign(models.Model):
    completed = models.DateTimeField(default=None, null=True, db_index=True)
    created = models.DateTimeField(db_index=True, auto_now_add=True)
    params = models.JSONField(default=dict)
    name = models.CharField(max_length=255, db_index=True, null=True)
    results = models.JSONField(default=dict)
    started = models.DateTimeField(default=None, null=True, db_index=True)

    # campaign is the campaign object above, google ads or dv360, null if its a csv
    campaign = models.ForeignKey(Campaign, db_index=True, null=True, default=None, on_delete=models.CASCADE)
    # Must store user for CSV type analysis
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE, null=True, default=None)

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


class IQCampaignChannel(models.Model):
    iq_campaign = models.ForeignKey(IQCampaign, on_delete=models.CASCADE, related_name="channels")
    clean = models.BooleanField(default=None, db_index=True, null=True)
    meta_data = models.JSONField(default=dict)  # the performance data from csv or API
    results = models.JSONField(default=dict)
    channel_id = models.CharField(max_length=128, db_index=True)
