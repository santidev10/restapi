from django.db import models
from django.db.models import CASCADE

from aw_reporting.models.ad_words.ad_group import AdGroup
from aw_reporting.models.ad_words.statistic import BaseStatisticModel


class Ad(BaseStatisticModel):
    id = models.CharField(max_length=15, primary_key=True)
    ad_group = models.ForeignKey(AdGroup, related_name='ads', on_delete=CASCADE)

    headline = models.TextField(null=True)
    creative_name = models.TextField(null=True)
    display_url = models.TextField(null=True)
    status = models.CharField(max_length=10, null=True)
    is_disapproved = models.BooleanField(default=False, null=False)

    def __str__(self):
        return "%s #%s" % (self.creative_name, self.id)
