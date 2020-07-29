from django.conf.urls import include
from django.conf.urls import url

import aw_creation.api.views.ad_creation_available_ad_formats
import aw_creation.api.views.ad_creation_duplicate
import aw_creation.api.views.ad_creation_list_setup
import aw_creation.api.views.ad_creation_setup
import aw_creation.api.views.ad_group_creation_duplicate
import aw_creation.api.views.ad_group_creation_list_setup
import aw_creation.api.views.ad_group_creation_setup
import aw_creation.api.views.ad_group_creation_targeting_export
import aw_creation.api.views.audience_flat_list
import aw_creation.api.views.audience_tool_list
import aw_creation.api.views.audience_tool_list_export
import aw_creation.api.views.aw_creation_change_status
import aw_creation.api.views.aw_creation_changed_accounts_list
import aw_creation.api.views.aw_creation_code_retrieve
import aw_creation.api.views.campaign_creation_duplicate
import aw_creation.api.views.campaign_creation_list_setup
import aw_creation.api.views.campaing_creation_setup
import aw_creation.api.views.creation_options
import aw_creation.api.views.document_to_changes
import aw_creation.api.views.geo_target_list
import aw_creation.api.views.items_from_segment_ids
import aw_creation.api.views.performance_targeting_filters
import aw_creation.api.views.performance_targeting_item
import aw_creation.api.views.performance_targeting_report
import aw_creation.api.views.targeting_items_import
import aw_creation.api.views.targeting_items_search
import aw_creation.api.views.topic_tool_flat_list
import aw_creation.api.views.topic_tool_list
import aw_creation.api.views.topic_tool_list_export
import aw_creation.api.views.youtube_video_from_url
import aw_creation.api.views.youtube_video_search
from aw_creation.api import views
from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from utils.api.urls import APP_NAME
from .urls_analytics import urlpatterns as analytics_urls
from .urls_dashboard import urlpatterns as dashboard_urls
from .urls_media_buying import urlpatterns as media_buying_uls

urlpatterns = [
    url(r"^geo_target_list/$",
        aw_creation.api.views.geo_target_list.GeoTargetListApiView.as_view(),
        name="geo_target_list"),
    url(r"^document_to_changes/(?P<content_type>\w+)/$",
        aw_creation.api.views.document_to_changes.DocumentToChangesApiView.as_view(),
        name="document_to_changes"),
    url(r"^youtube_video_search/(?P<query>.+)/$",
        aw_creation.api.views.youtube_video_search.YoutubeVideoSearchApiView.as_view(),
        name="youtube_video_search"),
    url(r"^youtube_video_from_url/(?P<url>.+)/$",
        aw_creation.api.views.youtube_video_from_url.YoutubeVideoFromUrlApiView.as_view(),
        name="youtube_video_from_url"),

    # <<< Setup
    url(r"^creation_options/$",
        aw_creation.api.views.creation_options.CreationOptionsApiView.as_view(),
        name="creation_options"),

    url(r"analytics/", include((analytics_urls, APP_NAME), namespace=Namespace.ANALYTICS)),
    url(r"dashboard/", include((dashboard_urls, APP_NAME), namespace=Namespace.DASHBOARD)),
    url(r"media_buying/", include((media_buying_uls, APP_NAME), namespace=Namespace.MEDIA_BUYING)),

    # these endpoints are closed for users who don"t have Media Buying add-on
    url(r"^account_creation_setup/(?P<pk>\w+)/$",
        views.AccountCreationSetupApiView.as_view(),
        name=Name.CreationSetup.ACCOUNT),
    url(r"^campaign_creation_list_setup/(?P<pk>\w+)/$",
        aw_creation.api.views.campaign_creation_list_setup.CampaignCreationListSetupApiView.as_view(),
        name="campaign_creation_list_setup"),
    url(r"^campaign_creation_setup/(?P<pk>\w+)/$",
        aw_creation.api.views.campaing_creation_setup.CampaignCreationSetupApiView.as_view(),
        name=Name.CreationSetup.CAMPAIGN),
    url(r"^ad_group_creation_list_setup/(?P<pk>\w+)/$",
        aw_creation.api.views.ad_group_creation_list_setup.AdGroupCreationListSetupApiView.as_view(),
        name="ad_group_creation_list_setup"),
    url(r"^ad_group_creation_setup/(?P<pk>\w+)/$",
        aw_creation.api.views.ad_group_creation_setup.AdGroupCreationSetupApiView.as_view(),
        name="ad_group_creation_setup"),
    url(r"^ad_creation_list_setup/(?P<pk>\w+)/$",
        aw_creation.api.views.ad_creation_list_setup.AdCreationListSetupApiView.as_view(),
        name="ad_creation_list_setup"),
    url(r"^ad_creation_setup/(?P<pk>\w+)/$",
        aw_creation.api.views.ad_creation_setup.AdCreationSetupApiView.as_view(),
        name="ad_creation_setup"),
    url(r"^ad_creation_available_ad_formats/(?P<pk>\w+)/$",
        aw_creation.api.views.ad_creation_available_ad_formats.AdCreationAvailableAdFormatsApiView.as_view(),
        name="ad_creation_available_ad_formats"),

    # for targeting management
    url(r"^items_from_segment_ids/(?P<segment_type>\w+)/$",
        aw_creation.api.views.items_from_segment_ids.ItemsFromSegmentIdsApiView.as_view(),
        name="items_from_segment_ids"),
    # targeting items search
    url(r"^targeting_items_search/(?P<list_type>\w+)/(?P<query>.+)/$",
        aw_creation.api.views.targeting_items_search.TargetingItemsSearchApiView.as_view(),
        name="targeting_items_search"),
    # import & export targeting
    url(r"^ad_group_creation_targeting_export/"
        r"(?P<pk>\w+)/(?P<list_type>\w+)/(?P<sub_list_type>positive|negative)/$",
        aw_creation.api.views.ad_group_creation_targeting_export.AdGroupCreationTargetingExportApiView.as_view(),
        name="ad_group_creation_targeting_export"),
    url(r"^targeting_items_import/(?P<list_type>\w+)/$",
        aw_creation.api.views.targeting_items_import.TargetingItemsImportApiView.as_view(),
        name="targeting_items_import"),

    url(r"^campaign_creation_duplicate/(?P<pk>\w+)/$",
        aw_creation.api.views.campaign_creation_duplicate.CampaignCreationDuplicateApiView.as_view(),
        name=Name.CreationSetup.CAMPAIGN_DUPLICATE),
    url(r"^ad_group_creation_duplicate/(?P<pk>\w+)/$",
        aw_creation.api.views.ad_group_creation_duplicate.AdGroupCreationDuplicateApiView.as_view(),
        name=Name.CreationSetup.AD_GROUP_DUPLICATE),
    url(r"^ad_creation_duplicate/(?P<pk>\w+)/$",
        aw_creation.api.views.ad_creation_duplicate.AdCreationDuplicateApiView.as_view(),
        name=Name.CreationSetup.AD_DUPLICATE),
    # >>> these endpoints are closed for users who don"t have Media Buying add-on
    # >>> Setup

    # <<< Performance
    # Regarding SAAS-793 we return DEMO data if user has no connected MCC's for all the performance endpoints below
    url(r"^performance_targeting_list/$",
        views.PerformanceTargetingListAPIView.as_view(),
        name="performance_targeting_list"),
    url(r"^performance_targeting_filters/(?P<pk>\w+)/$",
        aw_creation.api.views.performance_targeting_filters.PerformanceTargetingFiltersAPIView.as_view(),
        name="performance_targeting_filters"),
    url(r"^performance_targeting_report/(?P<pk>\w+)/$",
        aw_creation.api.views.performance_targeting_report.PerformanceTargetingReportAPIView.as_view(),
        name="performance_targeting_report"),
    url(
        r"^performance_targeting_item/(?P<targeting>\w+)/(?P<ad_group_id>\w+)/(?P<criteria>[^/]+)/$",
        aw_creation.api.views.performance_targeting_item.PerformanceTargetingItemAPIView.as_view(),
        name="performance_targeting_item"),
    # >>> Performance

    # tools
    url(r"^setup_topic_tool/$",
        aw_creation.api.views.topic_tool_list.TopicToolListApiView.as_view(),
        name="setup_topic_tool"),
    url(r"^setup_topic_tool_export/$",
        aw_creation.api.views.topic_tool_list_export.TopicToolListExportApiView.as_view(),
        name="setup_topic_tool_export"),
    url(r"^topic_list/$",
        aw_creation.api.views.topic_tool_flat_list.TopicToolFlatListApiView.as_view(),
        name=Name.TOPIC_LIST),

    url(r"^setup_audience_tool/$",
        aw_creation.api.views.audience_tool_list.AudienceToolListApiView.as_view(),
        name="setup_audience_tool"),
    url(r"^setup_audience_tool_export/$",
        aw_creation.api.views.audience_tool_list_export.AudienceToolListExportApiView.as_view(),
        name="setup_audience_tool_export"),
    url(r"^audience_list/$",
        aw_creation.api.views.audience_flat_list.AudienceFlatListApiView.as_view(),
        name=Name.AUDIENCE_LIST_FLAT),

    # aws script endpoints
    url(r"^aw_creation_changed_accounts_list/(?P<manager_id>\d+)/$",
        aw_creation.api.views.aw_creation_changed_accounts_list.AwCreationChangedAccountsListAPIView.as_view(),
        name="aw_creation_changed_accounts_list"),
    url(r"^aw_creation_code/(?P<account_id>\d+)/$",
        aw_creation.api.views.aw_creation_code_retrieve.AwCreationCodeRetrieveAPIView.as_view(),
        name="aw_creation_code"),
    url(r"^aw_creation_changes_status/(?P<account_id>\d+)/$",
        aw_creation.api.views.aw_creation_change_status.AwCreationChangeStatusAPIView.as_view(),
        name="aw_creation_change_status"),

    url(r"^account/(?P<account_id>\w+)/account_creation/$",
        views.AccountCreationByAccountAPIView.as_view(),
        name=Name.Dashboard.ACCOUNT_CREATION_BY_ACCOUNT)
]
