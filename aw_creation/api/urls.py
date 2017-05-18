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

    # creation
    url(r'^creation_options/$',
        views.CreationOptionsApiView.as_view(),
        name="creation_options"),
    url(r'^creation_account/$',
        views.CreationAccountApiView.as_view(),
        name="creation_account"),

    # optimize
    url(r'^optimization_options/$',
        views.OptimizationOptionsApiView.as_view(),
        name="optimization_options"),
    url(r'^optimization_account_list/$',
        views.OptimizationAccountListApiView.as_view(),
        name="optimization_account_list"),
    url(r'^optimization_account/(?P<pk>\w+)/$',
        views.OptimizationAccountApiView.as_view(),
        name="optimization_account"),
    url(r'^optimization_campaign/(?P<pk>\w+)/$',
        views.OptimizationCampaignApiView.as_view(),
        name="optimization_campaign"),
    url(r'^optimization_ad_group/(?P<pk>\w+)/$',
        views.OptimizationAdGroupApiView.as_view(),
        name="optimization_ad_group"),

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
        r'(?P<pk>\d+)/(?P<list_type>\w+)/$',
        views.AdGroupTargetingListApiView.as_view(),
        name="optimization_ad_group_targeting"),
    url(r'^optimization_ad_group_targeting_export/'
        r'(?P<pk>\d+)/(?P<list_type>\w+)/$',
        views.AdGroupTargetingListExportApiView.as_view(),
        name="optimization_ad_group_targeting_export"),
    url(r'^optimization_ad_group_targeting_import/'
        r'(?P<pk>\d+)/(?P<list_type>\w+)/$',
        views.AdGroupTargetingListImportApiView.as_view(),
        name="optimization_ad_group_targeting_import"),
]
