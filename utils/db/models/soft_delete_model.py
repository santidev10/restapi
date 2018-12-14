from django.db import models

from utils.lang import pick_dict, merge_dicts


class SoftDeleteManager(models.Manager):
    def get_base_queryset(self):
        return super(SoftDeleteManager, self).get_queryset()

    def get_queryset(self):
        queryset = self.get_base_queryset()
        return queryset.filter(is_deleted=False)

    def _get_unique_selector(self, **kwargs):
        return merge_dicts(*[
            pick_dict(kwargs, unique_constraint)
            for unique_constraint in self.model._meta.unique_together
        ])

    def create(self, **kwargs):
        kwargs["is_deleted"] = False
        unique_selector = self._get_unique_selector(**kwargs)
        return self.get_base_queryset() \
            .update_or_create(**unique_selector, defaults=kwargs)[0]


class SoftDeleteModel(models.Model):
    is_deleted = models.BooleanField(null=False, default=False)

    objects = SoftDeleteManager()

    def delete(self, using=None, keep_parents=False):
        self.is_deleted = True
        self.save()

    class Meta:
        abstract = True
