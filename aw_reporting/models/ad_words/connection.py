from django.conf import settings
from django.db import models

from .account import Account


class AWConnection(models.Model):
    email = models.EmailField(primary_key=True)
    refresh_token = models.CharField(max_length=150)

    # Token has been expired or revoked
    revoked_access = models.BooleanField(default=False)

    def __str__(self):
        return "AWConnection: {}".format(self.email)


class AWConnectionToUserRelation(models.Model):
    connection = models.ForeignKey(AWConnection, related_name="user_relations")
    user = models.ForeignKey(settings.AUTH_USER_MODEL,
                             related_name="aw_connections")
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = (("user", "connection"),)


class AWAccountPermission(models.Model):
    aw_connection = models.ForeignKey(
        AWConnection, related_name="mcc_permissions")
    account = models.ForeignKey(
        Account, related_name="mcc_permissions")
    can_read = models.BooleanField(default=False)
    # we will check read permission every day and show data to those users
    # who has access to it on AdWords
    can_write = models.BooleanField(default=False)

    # we will be set True only after successful account creations
    # and set False on errors

    class Meta:
        unique_together = (("aw_connection", "account"),)

    def __str__(self):
        return "AWPermission({}, {})".format(self.aw_connection, self.account)
