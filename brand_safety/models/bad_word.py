from django.db import models
from django.utils import timezone
from django.db.models.signals import pre_save
from django.db.models.signals import post_save
from django.dispatch import receiver

from aw_reporting.models import BaseQueryset
from audit_tool.models import AuditLanguage


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


class BadWordQuerySet(BaseQueryset):
    # Soft delete for queryset bulk delete operations
    def delete(self):
        return super(BadWordQuerySet, self).update(deleted_at=timezone.now())

    def hard_delete(self):
        return super(BadWordQuerySet, self).delete()


class BadWordManager(models.Manager):
    def __init__(self, *args, **kwargs):
        self.active_only = kwargs.pop("active_only", True)
        super(BadWordManager, self).__init__(*args, **kwargs)

    def get_queryset(self):
        # If manager was accessed with BadWord.objects, then return objects with deleted_at=None
        if self.active_only:
            return BadWordQuerySet(self.model).filter(deleted_at=None)
        return BadWordQuerySet(self.model)
    

class BadWord(models.Model):
    DEFAULT_LANGUAGE = "en"
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=80, db_index=True)
    category = models.ForeignKey(BadWordCategory, db_index=True)
    language = models.ForeignKey(AuditLanguage, db_index=True, null=True, default=None, related_name="bad_words")
    negative_score = models.IntegerField(default=1, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    deleted_at = models.DateTimeField(null=True, default=None, db_index=True)

    objects = BadWordManager(active_only=True)
    all_objects = BadWordManager(active_only=False)

    def delete(self):
        self.deleted_at = timezone.now()
        self.save(update_fields=['deleted_at'])
        return self

    def hard_delete(self):
        return super(BadWord, self).delete()

    def save(self, *args, **kwargs):
        if self.id is not None:
            try:
                prev_instance = BadWord.all_objects.get(id=self.id)
            except Exception as e:
                prev_instance = None
        if prev_instance:
            if 'update_fields' in kwargs and 'deleted_at' in kwargs['update_fields']:
                if self.deleted_at is not None:
                    BadWordHistory.objects.create(tag=self, action="Deleted")
                else:
                    BadWordHistory.objects.create(tag=self, action="Recovered")
            else:
                prev_instance = BadWord.all_objects.get(id=self.id)
                fields = ['name', 'category', 'language', 'negative_score']
                for field in fields:
                    old_field_value = getattr(prev_instance, field)
                    new_field_value = getattr(self, field)
                    if old_field_value != new_field_value:
                        if field == 'negative_score':
                            field = 'rating'
                        changes = "{}: {} -> {}".format(
                            field.capitalize(), old_field_value, new_field_value
                        )
                        BadWordHistory.objects.create(tag=self, action="Edited", changes=changes)
        else:
            super().save(*args, **kwargs)
            BadWordHistory.objects.create(tag=self, action="Added")
            return
        return super().save(*args, **kwargs)

    class Meta:
        unique_together = ("name", "language")


class BadWordHistory(models.Model):
    id = models.AutoField(primary_key=True, db_column='id')
    tag = models.ForeignKey(BadWord, on_delete=models.CASCADE)
    action = models.CharField(max_length=30, db_column='action')
    created_at = models.DateTimeField(auto_now_add=True, db_column='created_at')
    changes = models.CharField(max_length=250, db_index=True, default="")
