import logging

from django.contrib.postgres.fields import JSONField
from django.db import models

from utils.models import Timestampable


logger = logging.getLogger(__name__)


class ContactMessage(Timestampable):
    """
    Feedback main model
    """
    subject = models.CharField(max_length=255, default="", null=False)
    email = models.CharField(max_length=255, default="", null=False)
    data = JSONField(default={})

