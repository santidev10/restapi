from django.conf.urls import url, include

from aw_creation.api import views
from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from .urls_analytics import urlpatterns as analytics_urls
from .urls_dashboard import urlpatterns as dashboard_urls

urlpatterns = [
    url(r'^geo_target_list/$',
        views.GeoTargetListApiView.as_view(),
        name="geo_target_list"),
    url(r'^document_to_changes/(?P<content_type>\w+)/$',
        views.DocumentToChangesApiView.as_view(),
        name="document_to_changes"),
    url(r'^youtube_video_search/(?P<query>.+)/$',
        views.YoutubeVideoSearchApiView.as_view(),
        name="youtube_video_search"),
    url(r'^youtube_video_from_url/(?P<url>.+)/$',
        views.YoutubeVideoFromUrlApiView.as_view(),
        name="youtube_video_from_url"),

    # <<< Setup
    url(r'^creation_options/$',
        views.CreationOptionsApiView.as_view(),
        name="creation_options"),

    url(r'analytics/', include(analytics_urls, namespace=Namespace.ANALYTICS)),
    url(r'dashboard/', include(dashboard_urls, namespace=Namespace.DASHBOARD)),

    # these endpoints are closed for users who don't have Media Buying add-on
    url(r'^account_creation_setup/(?P<pk>\w+)/$',
        views.AccountCreationSetupApiView.as_view(),
        name=Name.CreationSetup.ACCOUNT),
    url(r'^campaign_creation_list_setup/(?P<pk>\w+)/$',
        views.CampaignCreationListSetupApiView.as_view(),
        name="campaign_creation_list_setup"),
    url(r'^campaign_creation_setup/(?P<pk>\w+)/$',
        views.CampaignCreationSetupApiView.as_view(),
        name=Name.CreationSetup.CAMPAIGN),
    url(r'^ad_group_creation_list_setup/(?P<pk>\w+)/$',
        views.AdGroupCreationListSetupApiView.as_view(),
        name="ad_group_creation_list_setup"),
    url(r'^ad_group_creation_setup/(?P<pk>\w+)/$',
        views.AdGroupCreationSetupApiView.as_view(),
        name="ad_group_creation_setup"),
    url(r'^ad_creation_list_setup/(?P<pk>\w+)/$',
        views.AdCreationListSetupApiView.as_view(),
        name="ad_creation_list_setup"),
    url(r'^ad_creation_setup/(?P<pk>\w+)/$',
        views.AdCreationSetupApiView.as_view(),
        name="ad_creation_setup"),
    url(r'^ad_creation_available_ad_formats/(?P<pk>\w+)/$',
        views.AdCreationAvailableAdFormatsApiView.as_view(),
        name="ad_creation_available_ad_formats"),

    # for targeting management
    url(r'^items_from_segment_ids/(?P<segment_type>\w+)/$',
        views.ItemsFromSegmentIdsApiView.as_view(),
        name="items_from_segment_ids"),
    # targeting items search
    url(r'^targeting_items_search/(?P<list_type>\w+)/(?P<query>.+)/$',
        views.TargetingItemsSearchApiView.as_view(),
        name="targeting_items_search"),
    # import & export targeting
    url(r'^ad_group_creation_targeting_export/'
        r'(?P<pk>\w+)/(?P<list_type>\w+)/(?P<sub_list_type>positive|negative)/$',
        views.AdGroupCreationTargetingExportApiView.as_view(),
        name="ad_group_creation_targeting_export"),
    url(r'^targeting_items_import/(?P<list_type>\w+)/$',
        views.TargetingItemsImportApiView.as_view(),
        name="targeting_items_import"),

    url(r'^campaign_creation_duplicate/(?P<pk>\w+)/$',
        views.CampaignCreationDuplicateApiView.as_view(),
        name=Name.CreationSetup.CAMPAIGN_DUPLICATE),
    url(r'^ad_group_creation_duplicate/(?P<pk>\w+)/$',
        views.AdGroupCreationDuplicateApiView.as_view(),
        name=Name.CreationSetup.AD_GROUP_DUPLICATE),
    url(r'^ad_creation_duplicate/(?P<pk>\w+)/$',
        views.AdCreationDuplicateApiView.as_view(),
        name=Name.CreationSetup.AD_DUPLICATE),
    # >>> these endpoints are closed for users who don't have Media Buying add-on
    # >>> Setup

    # <<< Performance
    # Regarding SAAS-793 we return DEMO data if user has no connected MCC's for all the performance endpoints below
    url(r'^performance_targeting_list/$',
        views.PerformanceTargetingListAPIView.as_view(),
        name="performance_targeting_list"),
    url(r'^performance_targeting_filters/(?P<pk>\w+)/$',
        views.PerformanceTargetingFiltersAPIView.as_view(),
        name="performance_targeting_filters"),
    url(r'^performance_targeting_report/(?P<pk>\w+)/$',
        views.PerformanceTargetingReportAPIView.as_view(),
        name="performance_targeting_report"),
    url(
        r'^performance_targeting_item/(?P<targeting>\w+)/(?P<ad_group_id>\w+)/(?P<criteria>[^/]+)/$',
        views.PerformanceTargetingItemAPIView.as_view(),
        name="performance_targeting_item"),
    # >>> Performance

    # tools
    url(r'^setup_topic_tool/$',
        views.TopicToolListApiView.as_view(),
        name="setup_topic_tool"),
    url(r'^setup_topic_tool_export/$',
        views.TopicToolListExportApiView.as_view(),
        name="setup_topic_tool_export"),
    url(r'^topic_list/$',
        views.TopicToolFlatListApiView.as_view(),
        name=Name.TOPIC_LIST),

    url(r'^setup_audience_tool/$',
        views.AudienceToolListApiView.as_view(),
        name="setup_audience_tool"),
    url(r'^setup_audience_tool_export/$',
        views.AudienceToolListExportApiView.as_view(),
        name="setup_audience_tool_export"),
    url(r'^audience_list/$',
        views.AudienceFlatListApiView.as_view(),
        name=Name.AUDIENCE_LIST_FLAT),

    # aws script endpoints
    url(r'^aw_creation_changed_accounts_list/(?P<manager_id>\d+)/$',
        views.AwCreationChangedAccountsListAPIView.as_view(),
        name="aw_creation_changed_accounts_list"),
    url(r'^aw_creation_code/(?P<account_id>\d+)/$',
        views.AwCreationCodeRetrieveAPIView.as_view(),
        name="aw_creation_code"),
    url(r'^aw_creation_changes_status/(?P<account_id>\d+)/$',
        views.AwCreationChangeStatusAPIView.as_view(),
        name="aw_creation_change_status"),

    url(r"^account/(?P<account_id>\w+)/account_creation/$",
        views.AccountCreationByAccountAPIView.as_view(),
        name=Name.Dashboard.ACCOUNT_CREATION_BY_ACCOUNT)
]