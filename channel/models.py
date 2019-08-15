from django.db import models

from utils.models import Timestampable


class AuthChannel(Timestampable):
    channel_id = models.CharField(max_length=255, primary_key=True)
    user_info = models.TextField(null=True)
    refresh_token = models.CharField(max_length=255, null=True)
    access_token = models.CharField(max_length=255, null=True)
    client_id = models.CharField(max_length=255)
    client_secret = models.CharField(max_length=255)
    access_token_expire_at = models.DateTimeField(null=True)

    class Meta:
        db_table = "auth_channel"
