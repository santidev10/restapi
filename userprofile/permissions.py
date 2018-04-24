from django.db import models
from django.contrib.auth.models import Permission, Group
from django.contrib.contenttypes.models import ContentType

from userprofile.models import PermissionSet


class GlobalPermissionManager(models.Manager):
    def get_queryset(self):
        return super(GlobalPermissionManager, self). \
            get_queryset().filter(content_type__model='access_permission')


class GlobalPermission(Permission):
    objects = GlobalPermissionManager()

    class Meta:
        proxy = True
        verbose_name = "access_permission"

    def save(self, *args, **kwargs):
        ct, created = ContentType.objects.get_or_create(
            model=self._meta.verbose_name, app_label=self._meta.app_label,
        )
        self.content_type = ct
        super(GlobalPermission, self).save(*args)


class PermissionHandler:

    def __init__(self, user):
        self.user = user

    def add_custom_user_permission(self, perm):
        """
        :param perm: str, permission name
        :return:
        """
        permission = self.get_custom_permission(perm)
        self.user.user_permissions.add(permission)

    def remove_custom_user_permission(self, perm):
        """
        :param perm: str, permission name
        :return:
        """
        permission = self.get_custom_permission(perm)
        self.user.user_permissions.remove(permission)

    def get_custom_permission(self, perm):
        """
        :param perm: str, permission name
        :return: GlobalPermission object
        """
        permission, _ = GlobalPermission.objects.get_or_create(codename=perm)
        return permission

    def get_user_groups(self):
        return self.user.groups.values_list('name', flat=True)

    def add_custom_user_group(self, raw_group):
        try:
            group = Group.objects.get(name=raw_group)
            self.user.groups.add(group)
        except Group.DoesNotExist:
            pass

    def remove_custom_user_group(self, raw_group):
        try:
            group = Group.objects.get(name=raw_group)
            self.user.groups.remove(group)
        except Group.DoesNotExist:
            pass

    def sync_groups(self):
        """
        sync permission groups from PermissionSet model
        """
        perm_set_data = PermissionSet.objects.all()
        for perm_data in perm_set_data:
            group_name = perm_data.permission_set
            raw_group_permissions = perm_data.permissions_values

            group, _ = Group.objects.get_or_create(name=group_name)
            group_permissions = tuple([self.get_custom_permission(perm) for perm in raw_group_permissions])
            group.permissions.set(group_permissions)
            group.save()

