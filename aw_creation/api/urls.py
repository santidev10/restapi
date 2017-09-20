from django.conf.urls import url

from aw_creation.api import views

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
    url(r'^account_creation_list/$',
        views.AccountCreationListApiView.as_view(),
        name="account_creation_list"),
    url(r'^account_creation_details/(?P<pk>\w+)/$',
        views.AccountCreationDetailsApiView.as_view(),
        name="account_creation_details"),

    url(r'^account_creation_setup/(?P<pk>\w+)/$',
        views.AccountCreationSetupApiView.as_view(),
        name="account_creation_setup"),
    url(r'^campaign_creation_list_setup/(?P<pk>\w+)/$',
        views.CampaignCreationListSetupApiView.as_view(),
        name="campaign_creation_list_setup"),
    url(r'^campaign_creation_setup/(?P<pk>\w+)/$',
        views.CampaignCreationSetupApiView.as_view(),
        name="campaign_creation_setup"),
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


    url(r'^account_creation_duplicate/(?P<pk>\w+)/$',
        views.AccountCreationDuplicateApiView.as_view(),
        name="account_creation_duplicate"),
    url(r'^campaign_creation_duplicate/(?P<pk>\w+)/$',
        views.CampaignCreationDuplicateApiView.as_view(),
        name="campaign_creation_duplicate"),
    url(r'^ad_group_creation_duplicate/(?P<pk>\w+)/$',
        views.AdGroupCreationDuplicateApiView.as_view(),
        name="ad_group_creation_duplicate"),
    url(r'^ad_creation_duplicate/(?P<pk>\w+)/$',
        views.AdCreationDuplicateApiView.as_view(),
        name="ad_creation_duplicate"),
    # >>> Setup

    # <<< Performance
    # Regarding SAAS-793 we return DEMO data if user has no connected MCC's for all the performance endpoints below
    url(r'^performance_account_campaigns/(?P<pk>\w+)/$',
        views.PerformanceAccountCampaignsListApiView.as_view(),
        name="performance_account_campaigns"),
    url(r'^performance_account_details/(?P<pk>\w+)/$',
        views.PerformanceAccountDetailsApiView.as_view(),
        name="performance_account_details"),
    url(r'^performance_chart/(?P<pk>\w+)/',
        views.PerformanceChartApiView.as_view(),
        name="performance_chart"),
    url(r'^performance_chart_items/(?P<pk>\w+)/(?P<dimension>\w+)/',
        views.PerformanceChartItemsApiView.as_view(),
        name="performance_chart_items"),
    url(r'^performance_export/(?P<pk>\w+)/',
        views.PerformanceExportApiView.as_view(),
        name="performance_export"),
    url(r'^performance_export_weekly_report/(?P<pk>\w+)/$',
        views.PerformanceExportWeeklyReport.as_view(),
        name="performance_export_weekly_report"),

    url(r'^performance_targeting_filters/(?P<pk>\w+)/$',
        views.PerformanceTargetingFiltersAPIView.as_view(),
        name="performance_targeting_filters"),
    url(r'^performance_targeting_report/(?P<pk>\w+)/(?P<list_type>\w+)/$',
        views.PerformanceTargetingReportAPIView.as_view(),
        name="performance_targeting_report"),
    url(r'^performance_targeting_report_details/(?P<pk>\w+)/(?P<list_type>\w+)/$',
        views.PerformanceTargetingReportDetailsAPIView.as_view(),
        name="performance_targeting_report_details"),
    url(r'^performance_targeting_settings/(?P<pk>\w+)/$',
        views.PerformanceTargetingSettingsAPIView.as_view(),
        name="performance_targeting_settings"),
    # >>> Performance

    # tools
    url(r'^setup_topic_tool/$',
        views.TopicToolListApiView.as_view(),
        name="setup_topic_tool"),
    url(r'^setup_topic_tool_export/$',
        views.TopicToolListExportApiView.as_view(),
        name="setup_topic_tool_export"),

    url(r'^setup_audience_tool/$',
        views.AudienceToolListApiView.as_view(),
        name="setup_audience_tool"),
    url(r'^setup_audience_tool_export/$',
        views.AudienceToolListExportApiView.as_view(),
        name="setup_audience_tool_export"),

    # ad group targeting lists
    url(r'^optimization_ad_group_targeting/'
        r'(?P<pk>\w+)/(?P<list_type>\w+)/$',
        views.AdGroupTargetingListApiView.as_view(),
        name="optimization_ad_group_targeting"),

    url(r'^optimization_ad_group_targeting_import_lists/'
        r'(?P<pk>\w+)/(?P<list_type>\w+)/$',
        views.AdGroupTargetingListImportListsApiView.as_view(),
        name="optimization_ad_group_targeting_import_lists"),

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
]
