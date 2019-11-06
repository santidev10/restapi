"""
Utils models module
"""
from django.db.models import Model, DateTimeField
from django.db import models
from django.contrib.postgres.fields import JSONField


class Timestampable(Model):
    """
    Create and update instance time
    """
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    class Meta:
        abstract = True

#
# class CacheItem(Timestampable):
#     key = models.CharField(max_length=255, unique=True)
#     value = JSONField(default=dict)
#
