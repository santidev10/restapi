from django.db import models
from django.contrib.auth.models import Group
from django.contrib.auth.models import Permission
from django.contrib.contenttypes.models import ContentType


class GlobalPermissionManager(models.Manager):
    def get_queryset(self):
        queryset = super(GlobalPermissionManager, self)\
                   .get_queryset()\
                   .filter(content_type__model="userprofile")
        return queryset


class GlobalPermission(Permission):
    objects = GlobalPermissionManager()

    class Meta:
        proxy = True
        # use verbose_name of existing model to get content type
        verbose_name = "userprofile"

    def save(self, *args, **kwargs):
        ct = ContentType.objects.get(
            model=self._meta.verbose_name,
            app_label=self._meta.app_label)
        self.content_type = ct
        super(GlobalPermission, self).save(*args)


class PermissionHandler:
    def add_custom_user_permission(self, perm):
        """
        :param perm: str, permission name
        :return:
        """
        permission = get_custom_permission(perm)
        self.user_permissions.add(permission)

    def remove_custom_user_permission(self, perm):
        """
        :param perm: str, permission name
        :return:
        """
        permission = get_custom_permission(perm)
        self.user_permissions.remove(permission)

    def get_user_groups(self):
        groups = self.groups.values_list('name', flat=True)
        return groups

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

    def update_access(self, access):
        """
        :param access: [{access: [{name: "group_name", value: true/false}]}, ...]
        :return:
        """
        # get data from access
        for access_item in access:
            group_name = access_item.get('name', None)
            is_group_for_add = access_item.get('value', None)

            # set data from access
            if group_name is not None and is_group_for_add is not None:

                if is_group_for_add:
                    self.add_custom_user_group(group_name)
                else:
                    self.remove_custom_user_group(group_name)


class PermissionGroupNames:
    HIGHLIGHTS = "Highlights"
    RESEARCH = "Research"
    SEGMENTS = "Segments"
    SEGMENTS_PRE_BAKES = "Segments - pre-baked segments"
    MEDIA_BUYING = "Media buying"
    AUTH_CHANNELS = "Auth channels and audience data"
    TOOLS = "Tools"
    ANALYTICS = "Analytics"


class Permissions:
    PERMISSION_SETS = (
        (PermissionGroupNames.HIGHLIGHTS, (
            "view_highlights",
            "settings_my_yt_channels",
        )),
        (PermissionGroupNames.RESEARCH, (
            "channel_list",
            "channel_filter",
            "channel_details",
            "video_list",
            "video_filter",
            "video_details",
            "keyword_list",
            "keyword_details",
            "keyword_filter",
        )),
        (PermissionGroupNames.SEGMENTS, (
            "segment_video_private",
            "segment_channel_private",
            "segment_keyword_private",
        )),
        (PermissionGroupNames.SEGMENTS_PRE_BAKES, (
            "segment_video_all",
            "segment_channel_all",
            "segment_keyword_all",
            "view_pre_baked_segments",
        )),
        (PermissionGroupNames.MEDIA_BUYING, (
            "view_media_buying",
            "settings_my_aw_accounts",
        )),
        (PermissionGroupNames.AUTH_CHANNELS, (
            "channel_audience",
            "channel_aw_performance",
            "video_audience",
            "video_aw_performance",
        )),
        (PermissionGroupNames.TOOLS, (
            "view_pacing_report",
            "view_pricing_tool",
            "view_health_check",
            "view_trends",
            "view_health_check",
            "view_dashboard",
        )),
        (PermissionGroupNames.ANALYTICS, (
            "account_creation_list",
            "account_creation_post",
        ))
    )

    PERM_LIST = (
        # view section
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
        # tools section
        "view_pacing_report",
        "view_pricing_tool",
        "view_trends",
        "view_health_check",
        "view_dashboard",
        # analytics sectipn
        "account_creation_list",
        "account_creation_post",
        # settings section
        "settings_my_aw_accounts",
        "settings_my_yt_channels",
    )

    @staticmethod
    def sync_groups():
        """
        sync permission groups
        """
        perm_set_data = dict(Permissions.PERMISSION_SETS)
        for k, v in perm_set_data.items():
            group_name = k
            raw_group_permissions = v

            group, _ = Group.objects.get_or_create(name=group_name)
            group_permissions = tuple([get_custom_permission(perm) for perm in raw_group_permissions])
            group.permissions.set(group_permissions)
            group.save()


def get_custom_permission(perm):
    """
    :param perm: str, permission name
    :return: GlobalPermission object
    """
    permission, _ = GlobalPermission.objects.get_or_create(codename=perm)
    return permission
