from django.db import models

from aw_reporting.models import BaseQueryset


class BadWord(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=80)
    category = models.CharField(max_length=80)

    objects = BaseQueryset.as_manager()

    class Meta:
        unique_together = ("name", "category")
