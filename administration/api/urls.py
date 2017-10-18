"""
Administration api urls module
"""
from django.conf.urls import url

from administration.api.views import UserListAdminApiView, \
    UserDeleteAdminApiView, AuthAsAUserAdminApiView, \
    UserActionListCreateApiView, UserActionDeleteAdminApiView, \
    PlanListCreateApiView, PlanChangeDeleteApiView

urlpatterns = [
    url(r'^users/$', UserListAdminApiView.as_view(), name="user_list"),
    url(r'^users/(?P<pk>\d+)/$', UserDeleteAdminApiView.as_view(),
        name="user_details"),
    url(r'^users/(?P<pk>\d+)/auth/$', AuthAsAUserAdminApiView.as_view(),
        name="user_auth_admin"),
    url(r'^user_actions/$', UserActionListCreateApiView.as_view(),
        name="user_action_list"),
    url(r'^user_actions/(?P<pk>\d+)/$', UserActionDeleteAdminApiView.as_view(),
        name="user_action_details"),
    url(r'^plan/(?P<pk>)/$', PlanChangeDeleteApiView.as_view(), name="plan"),
    url(r'^plan/$', PlanListCreateApiView.as_view(), name="plan"),
]
