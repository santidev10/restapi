from django.contrib.postgres.fields import JSONField
from django.db import models

from utils.models import Timestampable


class CacheItem(Timestampable):
    key = models.CharField(max_length=255, unique=True)
    value = JSONField(default=dict)
