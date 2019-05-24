from django.db import models
from django.utils import timezone

from aw_reporting.models import BaseQueryset


class BadWordCategory(models.Model):
    name = models.CharField(max_length=80, unique=True)
    # Categories to exclude from brand safety
    EXCLUDED = ["1", "2"]

    @staticmethod
    def from_string(in_var):
        db_result, _ = BadWordCategory.objects.get_or_create(name=in_var.lower())
        return db_result

    @staticmethod
    def get_category_mapping():
        mapping = {
            str(category.id): category.name
            for category in BadWordCategory.objects.all()
        }
        return mapping

    def __str__(self):
        return self.name


class BadWordLanguage(models.Model):
    DEFAULT = "en"
    name = models.CharField(max_length=20, unique=True)

    @staticmethod
    def from_string(in_var):
        db_result, _ = BadWordLanguage.objects.get_or_create(name=in_var.lower())
        return db_result

    def __str__(self):
        return self.name


class BadWordQuerySet(BaseQueryset):
    # Soft delete for queryset bulk delete operations
    def delete(self):
        return super(BadWordQuerySet, self).update(deleted_at=timezone.now())


class BadWordManager(models.Manager):
    def __init__(self, *args, **kwargs):
        self.active_only = kwargs.pop("active_only", True)
        super(BadWordManager, self).__init__(*args, **kwargs)

    def get_queryset(self):
        # If manager was accessed with BadWord.objects, then return objects with deleted_at=None
        if self.active_only:
            return BadWordQuerySet(self.model).filter(deleted_at=None)
        return BadWordQuerySet(self.model)


def get_default_language():
    language = BadWordLanguage.from_string(BadWordLanguage.DEFAULT).id
    return language


class BadWord(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=80, db_index=True)
    category = models.ForeignKey(BadWordCategory, db_index=True)
    language = models.ForeignKey(BadWordLanguage, db_index=True, default=get_default_language, related_name="bad_words")
    negative_score = models.IntegerField(default=1, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    deleted_at = models.DateTimeField(blank=True, null=True, db_index=True)

    objects = BadWordManager(active_only=True)
    all_objects = BadWordManager(active_only=False)

    # Soft delete for single objects
    def delete(self):
        self.deleted_at = timezone.now()
        self.save()

    class Meta:
        unique_together = ("name", "category", "language")

