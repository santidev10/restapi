from django.contrib.auth import get_user_model
from django.contrib.postgres.fields import JSONField
from django.db import models

class FTUX(models.Model):
    feature = models.CharField(unique=True, max_length=10)
    refresh_time = models.DateTimeField(default=None, db_index=True, null=True)
    meta_data = JSONField(default=dict) # contains all data about the actual ftux

    def to_dict(self):
        d = {
            'feature': self.feature,
            'meta_data': self.meta_data,
        }
        return d

class FTUXUser(models.Model):
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    ftux = models.ForeignKey(FTUX, on_delete=models.CASCADE)
    last_seen = models.DateTimeField(default=None, null=True, db_index=True)

    class Meta:
        unique_together = ("user", "ftux")