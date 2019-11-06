from utils.models import Timestampable
from django.db import models
from django.contrib.postgres.fields import JSONField


class CacheItem(Timestampable):
    key = models.CharField(max_length=255, unique=True)
    value = JSONField(default=dict)
