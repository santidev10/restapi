from django.conf.urls import url

from keyword_tool.api import views

urlpatterns = [
    url(r'^kw_tool_viral/$',
        views.ViralKeywordsApiView.as_view(),
        name="kw_tool_viral"),
    url(r'^kw_tool_interests/$',
        views.InterestsApiView.as_view(),
        name="kw_tool_interests"),
    url(r'^kw_tool_predefined_queries/$',
        views.PredefinedQueriesApiView.as_view(),
        name="kw_tool_predefined_queries"),
    url(r'^kw_tool_optimize_query/(?P<query>.+)/$',
        views.OptimizeQueryApiView.as_view(),
        name="kw_tool_optimize_query"),
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
]
