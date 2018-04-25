from django.db import models
from django.contrib.auth.models import Permission, Group
from django.contrib.contenttypes.models import ContentType


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
    def add_custom_user_permission(self, perm):
        """
        :param perm: str, permission name
        :return:
        """
        permission = self.get_custom_permission(perm)
        self.user_permissions.add(permission)

    def remove_custom_user_permission(self, perm):
        """
        :param perm: str, permission name
        :return:
        """
        permission = self.get_custom_permission(perm)
        self.user_permissions.remove(permission)

    def get_custom_permission(self, perm):
        """
        :param perm: str, permission name
        :return: GlobalPermission object
        """
        permission, _ = GlobalPermission.objects.get_or_create(codename=perm)
        return permission

    def get_user_groups(self):
        return self.groups.values_list('name', flat=True)

    def add_custom_user_group(self, group_name):
        try:
            group = Group.objects.get(name=group_name)
            self.groups.add(group)
        except Group.DoesNotExist:
            pass

    def remove_custom_user_group(self, group_name):
        try:
            group = Group.objects.get(name=group_name)
            self.groups.remove(group)
        except Group.DoesNotExist:
            pass

    def sync_groups(self):
        """
        sync permission groups from Permissions class
        """
        perm_set_data = dict(Permissions.PERMISSION_SETS)
        for k, v in perm_set_data.items():
            group_name = k
            raw_group_permissions = v

            group, _ = Group.objects.get_or_create(name=group_name)
            group_permissions = tuple([self.get_custom_permission(perm) for perm in raw_group_permissions])
            group.permissions.set(group_permissions)
            group.save()


# tmp class
class Permissions:
    """
    purpose: fill db with permissions sets

    for k,v in dict(PERMISSION_SETS).items():
        PermissionSet.objects.create(permission_set=k, permissions_values=list(v))


    """
    PERMISSION_SETS = (
        ('Highlights', ("view_highlights", "my_yt_channels")),
        ('Discovery', ("channel_list",
                       "channel_filter",
                       "channel_details",
                       "video_list",
                       "video_filter",
                       "video_details",
                       "keyword_list",
                       "keyword_details",
                       "keyword_filter",)),
        ('Segments', ("segment_video_private",
                      "segment_channel_private",
                      "segment_keyword_private",)),
        ('Segments - pre-baked segments', ("segment_video_all",
                                           "segment_channel_all",
                                           "segment_keyword_all",
                                           "view_pre_baked_segments",)),
        ('Media buying', ("view_media_buying",
                          "settings_my_aw_accounts",)),
        ('Auth channels and audience data', ("channel_audience",
                                             "channel_aw_performance",
                                             "video_audience",
                                             "video_aw_performance",
                                             )),
    )

    PERM_LIST = (
        # view section
        "view_trends",
        "view_benchmarks",
        "view_highlights",
        "view_performance",
        "view_media_buying",
        "view_pre_baked_segments",
        "view_create_and_manage_campaigns",
        # video section
        "video_list",
        "video_filter",
        "video_details",
        "video_audience",
        "video_aw_performance",
        # channel section
        "channel_list",
        "channel_filter",
        "channel_details",
        "channel_audience",
        "channel_aw_performance",
        # keyword section
        "keyword_list",
        "keyword_details",
        "keyword_filter",
        # segment section
        "segment_video_all",
        "segment_video_private",
        "segment_channel_all",
        "segment_channel_private",
        "segment_keyword_all",
        "segment_keyword_private",
        # settings section
        "settings_my_aw_accounts",
        "settings_my_yt_channels",
    )
