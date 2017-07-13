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

    # creation functionality
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

    # optimization
    url(r'^optimization_filters/(?P<pk>\w+)/(?P<kpi>\w+)/$',
        views.OptimizationFiltersApiView.as_view(),
        name="optimization_filters"),
    url(r'^optimization_settings/(?P<pk>\w+)/(?P<kpi>\w+)/$',
        views.OptimizationSettingsApiView.as_view(),
        name="optimization_settings"),
    url(r'^optimization_targeting/'
        r'(?P<pk>\w+)/(?P<kpi>\w+)/(?P<list_type>\w+)/$',
        views.OptimizationTargetingApiView.as_view(),
        name="optimization_targeting"),

    # tools
    url(r'^optimization_topic_tool/$',
        views.TopicToolListApiView.as_view(),
        name="optimization_topic_tool"),
    url(r'^optimization_topic_tool_export/$',
        views.TopicToolListExportApiView.as_view(),
        name="optimization_topic_tool_export"),

    url(r'^optimization_audience_tool/$',
        views.AudienceToolListApiView.as_view(),
        name="optimization_audience_tool"),
    url(r'^optimization_audience_tool_export/$',
        views.AudienceToolListExportApiView.as_view(),
        name="optimization_audience_tool_export"),

    # ad group targeting lists
    url(r'^optimization_ad_group_targeting/'
        r'(?P<pk>\w+)/(?P<list_type>\w+)/$',
        views.AdGroupTargetingListApiView.as_view(),
        name="optimization_ad_group_targeting"),
    url(r'^optimization_ad_group_targeting_export/'
        r'(?P<pk>\w+)/(?P<list_type>\w+)/$',
        views.AdGroupTargetingListExportApiView.as_view(),
        name="optimization_ad_group_targeting_export"),
    url(r'^optimization_ad_group_targeting_import/'
        r'(?P<pk>\w+)/(?P<list_type>\w+)/$',
        views.AdGroupTargetingListImportApiView.as_view(),
        name="optimization_ad_group_targeting_import"),
    url(r'^optimization_ad_group_targeting_import_lists/'
        r'(?P<pk>\w+)/(?P<list_type>\w+)/$',
        views.AdGroupTargetingListImportListsApiView.as_view(),
        name="optimization_ad_group_targeting_import_lists"),
]
