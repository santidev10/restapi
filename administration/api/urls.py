"""
Administration api urls module
"""
from django.conf.urls import url

from administration.api.views import UserListAdminApiView, \
    UserRetrieveUpdateDeleteAdminApiView, AuthAsAUserAdminApiView, \
    UserActionListCreateApiView, UserActionDeleteAdminApiView, AccessGroupsListApiView

urlpatterns = [
    url(r'^users/$', UserListAdminApiView.as_view(), name="user_list"),
    url(r'^users/(?P<pk>\d+)/$', UserRetrieveUpdateDeleteAdminApiView.as_view(),
        name="user_details"),
    url(r'^users/(?P<pk>\d+)/auth/$', AuthAsAUserAdminApiView.as_view(),
        name="user_auth_admin"),
    url(r'^user_actions/$', UserActionListCreateApiView.as_view(),
        name="user_action_list"),
    url(r'^user_actions/(?P<pk>\d+)/$', UserActionDeleteAdminApiView.as_view(),
        name="user_action_details"),
    url(r'^access/groups/$', AccessGroupsListApiView.as_view(),
        name="access_groups"),
]
