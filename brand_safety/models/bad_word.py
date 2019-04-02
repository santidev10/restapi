from django.db import models

from aw_reporting.models import BaseQueryset


class BadWordCategory(models.Model):
    name = models.CharField(max_length=80, unique=True)

    @staticmethod
    def from_string(in_var):
        db_result, _ = BadWordCategory.objects.get_or_create(name=in_var.lower())
        return db_result


class BadWord(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=80)
    category = models.CharField(max_length=80) # tech debt: remove in 3.15
    category_ref = models.ForeignKey(BadWordCategory, db_index=True, default=None, null=True)

    objects = BaseQueryset.as_manager()

    class Meta:
        unique_together = ("name", "category")