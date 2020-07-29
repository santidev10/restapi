from django.db import models
from django.conf import settings

from aw_reporting.models import Opportunity
from userprofile.models import UserProfile


class OpportunityWatch(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    opportunity = models.ForeignKey(Opportunity, on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, related_name="watch", on_delete=models.CASCADE)
