from django.conf import settings
from django.conf.urls import url

from keyword_tool.api import views

urlpatterns = [
    url(r'^kw_tool_viral/$',
        views.ViralKeywordsApiView.as_view(),
        name="kw_tool_viral"),
    url(r'^kw_tool_predefined_queries/$',
        views.PredefinedQueriesApiView.as_view(),
        name="kw_tool_predefined_queries"),
    url(r'^kw_tool_saved_lists/$',
        views.SavedListsGetOrCreateApiView.as_view(),
        name="kw_tool_saved_lists"),
    url(r'^kw_tool_saved_list/(?P<pk>.+)/$',
        views.SavedListApiView.as_view(),
        name="kw_tool_saved_list"),
    url(r'^kw_tool_saved_list/(?P<pk>.+)/duplicate$',
        views.ListsDuplicateApiView.as_view(),
        name="kw_tool_saved_list_duplicate"),
    url(r'^kw_tool_saved_list_keywords/(?P<pk>.+)/$',
        views.SavedListKeywordsApiView.as_view(),
        name="kw_tool_saved_list_keywords"),
    url(r'^service/{}/build_viral_list$'.format(settings.KW_TOOL_KEY),
        views.ViralListBuildView.as_view(),
        name="kw_tool_build_viral_list"),
]
