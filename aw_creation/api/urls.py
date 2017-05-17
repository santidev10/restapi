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

    # optimization
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

    # creation
    url(r'^creation_options/$',
        views.CreationOptionsApiView.as_view(),
        name="creation_options"),
    url(r'^creation_account/$',
        views.CreationAccountApiView.as_view(),
        name="creation_account"),
]
