from django.db import models
from django.contrib.auth.models import Permission
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
        todo fill
        :param perm: str, permission name
        :return:
        """
        permission = self.get_custom_permission(perm)
        self.user.user_permissions.remove(permission)

    def get_custom_permission(self, perm):
        permission, _ = GlobalPermission.objects.get_or_create(codename=perm)
        return permission

    def get_user_perm_sets(self):
        compliance = {}
        all_perm_sets = PermissionSet.objects.all()
        for perm_set in all_perm_sets:
            user_perm = self.user.user_permissions.filter(codename__in=perm_set.permissions_values,
                                                          content_type__model='access_permission').values_list(
                'codename', flat=True)
            compliance[perm_set.permission_set] = set(user_perm) == set(perm_set.permissions_values)
        return compliance

    def manage_permission_by_set(self, perm_set, action=None):
        assert action is not None, 'specify action: add or remove'
        try:
            p_set_obj = PermissionSet.objects.get(permission_set=perm_set)
            if action == 'add':
                for perm in p_set_obj.permissions_values:
                    self.add_custom_user_permission(perm)
            elif action == 'remove':
                for perm in p_set_obj.permissions_values:
                    self.remove_custom_user_permission(perm)
        except PermissionSet.DoesNotExist:
            return False
        return True

