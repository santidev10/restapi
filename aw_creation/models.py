from django.core.validators import MaxValueValidator, MinValueValidator, \
    RegexValidator, ValidationError
from django.dispatch import receiver
from django.db import models
from django.db.models.signals import post_save
from django.conf import settings
from decimal import Decimal
import calendar
import json
import uuid
import logging
logger = logging.getLogger(__name__)

NameValidator = RegexValidator(r"^[^#']*$", 'Not allowed characters')


def get_uid(length=12):
    return str(uuid.uuid4()).replace('-', '')[:length]


def get_version():
    return get_uid(8)


class UniqueItem(models.Model):

    name = models.CharField(max_length=250, validators=[NameValidator])

    class Meta:
        abstract = True

    def __str__(self):
        return self.unique_name

    @property
    def unique_name(self):
        return "{} #{}".format(self.name, self.id)


class AccountCreation(UniqueItem):
    id = models.CharField(primary_key=True, max_length=12,
                          default=get_uid, editable=False)
    owner = models.ForeignKey('userprofile.userprofile',
                              related_name="aw_account_creations")
    aw_manager_id = models.CharField(
        max_length=15, null=True,  blank=True,
    )
    account = models.OneToOneField(
        "aw_campaign.Account", related_name='account_creation',
        on_delete=models.SET_NULL, null=True, blank=True,
    )
    is_paused = models.BooleanField(default=False)
    is_ended = models.BooleanField(default=False)
    is_approved = models.BooleanField(default=False)
    # means: "pause placements and move account to the bottom of the page"

    version = models.CharField(max_length=8, default=get_version)
    is_changed = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def get_aws_code(self):
        if self.account_id:
            lines = []
            for c in self.campaign_managements.filter(is_approved=True):
                lines.append(c.get_aws_code())
            lines.append(
                "sendChangesStatus('{}', '{}');".format(
                    self.account_id, self.version)
            )
            return " ".join(lines)
