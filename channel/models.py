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
    token_revocation = models.DateTimeField(null=True)

    class Meta:
        db_table = "auth_channel"

    @staticmethod
    def get_auth_channel_ids() -> list:
        """
        returns list of channel ids with active tokens in AuthChannel
        """
        try:
            auth_channels = AuthChannel.objects.filter(token_revocation=None)
            return [item.channel_id for item in auth_channels]
        except AuthChannel.DoesNotExist:
            return None
