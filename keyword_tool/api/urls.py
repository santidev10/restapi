from django.conf import settings
from django.conf.urls import url

from keyword_tool.api import views

urlpatterns = [
    url(r'^kw_tool_viral/$',
        views.ViralKeywordsApiView.as_view(),
        name="kw_tool_viral"),
    url(r'^kw_tool_saved_lists/$',
        views.SavedListsGetOrCreateApiView.as_view(),
        name="kw_tool_saved_lists"),
    url(r'^service/{}/build_viral_list$'.format(settings.KW_TOOL_KEY),
        views.ViralListBuildView.as_view(),
        name="kw_tool_build_viral_list"),
]
