from uuid import uuid4

from django.db import models


def get_uid():
    return uuid4().hex


class SavedEmail(models.Model):
    id = models.CharField(primary_key=True, max_length=32, default=get_uid,
                          editable=False)
    html = models.TextField()
    date = models.DateField(auto_now_add=True)
