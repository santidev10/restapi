from django.db import models

from aw_reporting.models import BaseQueryset


class BadWordCategory(models.Model):
    name = models.CharField(max_length=80, unique=True)
    # Categories to exclude from brand safety
    EXCLUDED = [9]

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


class BadWord(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=80, unique=True)
    category = models.ForeignKey(BadWordCategory, db_index=True, default=None, null=True)
    negative_score = models.IntegerField(default=1, db_index=True)

    objects = BaseQueryset.as_manager()

    def save(self, *args, **kwargs):
        if not self.pk:
            BadWordHistory.objects.create(tag_name=self.name, action='ADDED')
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        BadWordHistory.objects.create(tag_name=self.name, action='DELETED')
        super().delete(*args, **kwargs)

    class Meta:
        unique_together = ("name", "category")


class BadWordHistory(models.Model):
    id = models.AutoField(primary_key=True)
    tag_name = models.CharField(max_length=150)
    action = models.CharField(max_length=30)
    created_at = models.DateTimeField(auto_now_add=True)

