from django.apps import apps
from django.contrib.auth.models import Group
from django.contrib.auth.models import Permission
from django.contrib.contenttypes.models import ContentType
from django.db import models


class GlobalPermissionManager(models.Manager):
    def get_queryset(self):
        queryset = super(GlobalPermissionManager, self) \
            .get_queryset() \
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

    def has_custom_user_group(self, group_name):
        return self.groups.filter(name=group_name).exists()

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
    MEDIA_PLANNING = "Media Planning"
    MEDIA_PLANNING_PRE_BAKES = "Media Planning - pre-baked segments"
    MEDIA_PLANNING_AUDIT = "Media Planning - audit"
    MEDIA_PLANNING_BRAND_SAFETY = "Media Planning - Brand Safety"
    MEDIA_PLANNING_WHITE_LISTS = "Media Planning - whitelists"
    MEDIA_BUYING = "Media Buying"
    AUTH_CHANNELS = "Auth channels and audience data"
    TOOLS = "Tools"
    MANAGED_SERVICE = "Managed Service"
    MANAGED_SERVICE_PERFORMANCE_DETAILS = "Hide Managed Service Performance Details"
    SELF_SERVICE = "Self Service"
    SELF_SERVICE_TRENDS = "Self Service Trends"
    FORECASTING = "Forecasting"
    AUDIT_DOWNLOAD = "Audit Download"
    AUDIT_VIEW = "Audit View"
    BRAND_SAFETY_VIEW = "Brand Safety View"
    BRAND_SAFETY_DOWNLOAD = "Brand Safety Download"
    BRAND_SAFETY_SCORING = "Brand Safety Scoring"
    AUDIT_FLAGGING = "Audit Flagging"
    ADS_ANALYZER = "Ads Analyzer"
    ADS_ANALYZER_RECIPIENTS = "Ads Analyzer Recipients"
    TRANSCRIPTS = "Transcripts"
    MONETIZATION = "Monetization"
    VIEW_CHANNEL_VIDEO_TABS = "View Channel/Video Tabs"
    AUDIT_VET_ADMIN = "Vetting Tool Admin"
    AUDIT_VET = "Vetting Tool"
    CUSTOM_SEGMENTS = "custom_segments"
    DOMAIN_MANAGEMENT = "Domain Management"


class Permissions:
    PERMISSION_SETS = (
        (PermissionGroupNames.HIGHLIGHTS, (
            "view_highlights",
            "settings_my_yt_channels",
        )),
        (PermissionGroupNames.RESEARCH, (
            "channel_list",
            "channel_details",
            "video_list",
            "video_details",
            "keyword_list",
            "keyword_details",
        )),
        (PermissionGroupNames.MEDIA_PLANNING, (
            "channel_list",
            "video_list",
            "keyword_list",
        )),
        (PermissionGroupNames.MEDIA_PLANNING_BRAND_SAFETY, (
            "view_audit_segments",
        )),
        (PermissionGroupNames.MEDIA_PLANNING_PRE_BAKES, (
            "view_pre_baked_segments",
        )),
        (PermissionGroupNames.MEDIA_PLANNING_AUDIT, (
            "view_audit_segments",
        )),
        (PermissionGroupNames.MEDIA_PLANNING_WHITE_LISTS, (
            "view_white_lists",
        )),
        (PermissionGroupNames.MEDIA_BUYING, (
            "view_buying",
        )),
        (PermissionGroupNames.AUTH_CHANNELS, (
            "channel_audience",
            "video_audience",
        )),
        (PermissionGroupNames.TOOLS, (
            "view_pricing_tool",
            "view_chf_trends",
        )),
        (PermissionGroupNames.MANAGED_SERVICE, (
            "view_dashboard",
        )),
        (PermissionGroupNames.VIEW_CHANNEL_VIDEO_TABS, (
            "view_channel_video_tabs",
        )),
        (PermissionGroupNames.MANAGED_SERVICE_PERFORMANCE_DETAILS, (
            "view_performance_details",
        )),
        (PermissionGroupNames.SELF_SERVICE, (
            "view_media_buying",
            "settings_my_aw_accounts",
        )),
        (PermissionGroupNames.SELF_SERVICE_TRENDS, (
        )),
        (PermissionGroupNames.FORECASTING, (
            "forecasting",
        )),
        (PermissionGroupNames.BRAND_SAFETY_VIEW, (
            "view_brand_safety",
        )),
        (PermissionGroupNames.BRAND_SAFETY_DOWNLOAD, (
            "download_brand_safety",
        )),
        (PermissionGroupNames.BRAND_SAFETY_SCORING, (
            "scoring_brand_safety",
        )),
        (PermissionGroupNames.AUDIT_DOWNLOAD, (
            "download_audit",
        )),
        (PermissionGroupNames.AUDIT_VIEW, (
            "view_audit",
        )),
        (PermissionGroupNames.AUDIT_FLAGGING, (
            "flag_audit",
        )),
        (PermissionGroupNames.ADS_ANALYZER, (
            "view_opportunity_list",
            "create_opportunity_report",
        )),
        (PermissionGroupNames.ADS_ANALYZER_RECIPIENTS, (
            "view_opportunity_report_recipients_list",
        )),
        (PermissionGroupNames.TRANSCRIPTS, (
            "transcripts_filter",
        )),
        (PermissionGroupNames.MONETIZATION, (
            "monetization_filter",
        )),

        (PermissionGroupNames.AUDIT_VET_ADMIN, (
            "vet_audit_admin",
        )),
        (PermissionGroupNames.AUDIT_VET, (
            "vet_audit",
        )),
        (PermissionGroupNames.DOMAIN_MANAGEMENT, (
            "domain_management",
        ))
    )

    PERM_LIST = (
        # view section
        "view_highlights",
        "view_media_buying",
        "view_pre_baked_segments",
        "view_audit_segments",
        # video section
        "video_list",
        "video_details",
        "video_audience",
        # channel section
        "channel_list",
        "channel_details",
        "channel_audience",
        # keyword section
        "keyword_list",
        "keyword_details",
        # tools section
        "view_pricing_tool",
        # dashboard section
        "view_dashboard",
        "view_performance_details",
        "view_channel_video_tabs",
        # settings section
        "settings_my_aw_accounts",
        "settings_my_yt_channels",
        # brand safety section
        "view_brand_safety",
        "download_brand_safety",
        "scoring_brand_safety",
        # audits
        "download_audit",
        "view_audit",
        "flag_audit",
        "vet_audit",
        "vet_audit_admin",
        # Ads Analyzer
        "view_opportunity_list",
        "create_opportunity_report",
        "view_opportunity_report_recipients_list",
        # Transcripts
        "transcripts_filter",
        # Monetization
        "monetization_filter",
        "domain_management"
    )

    @staticmethod
    def sync_groups(apps_config=None):
        """
        Sync groups and groups permissions
        """
        apps_config = apps_config or apps
        permissions_set_data = dict(Permissions.PERMISSION_SETS)
        groups_names = set()
        permissions_codenames = set()
        group_model = apps_config.get_model("auth", "group")
        for group_name, raw_group_permissions in permissions_set_data.items():
            group, _ = group_model.objects.get_or_create(name=group_name)
            group_permissions = tuple([
                get_custom_permission(perm, apps_config)
                for perm in raw_group_permissions
            ])
            group.permissions.set(group_permissions)
            group.save()

            groups_names.add(group_name)
            permissions_codenames |= set(raw_group_permissions)

        cleanup_groups_permissions(apps_config, groups_names, permissions_codenames)


def cleanup_groups_permissions(apps_config, groups_names, permissions_codenames):
    group_model = apps_config.get_model("auth", "group")
    global_permissions_model = apps_config.get_model("userprofile", "globalpermission")

    group_model.objects.all().exclude(name__in=groups_names) \
        .delete()
    global_permissions_model.objects.all().exclude(codename__in=permissions_codenames) \
        .delete()


def get_custom_permission(perm, apps_config=None):
    """
    :param perm: str, permission name
    :return: GlobalPermission object
    """
    apps_config = apps_config or apps
    global_permission_model = apps_config.get_model("userprofile", "globalpermission")
    content_type_model = apps_config.get_model("contenttypes", "contenttype")
    userprofile_contenttype = content_type_model.objects.get(app_label="userprofile", model="userprofile")
    permission, _ = global_permission_model.objects.filter(content_type__model="userprofile").get_or_create(
        codename=perm,
        defaults=dict(content_type=userprofile_contenttype)
    )
    return permission
