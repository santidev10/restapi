"""
Administration api urls module
"""
from django.conf.urls import url

from administration.api.urls.names import AdministrationPathName as Names
from administration.api.views import AccessGroupsListApiView
from administration.api.views import AuthAsAUserAdminApiView
from administration.api.views import UserActionDeleteAdminApiView
from administration.api.views import UserActionListCreateApiView
from administration.api.views import UserListAdminApiView
from administration.api.views import UserRetrieveUpdateDeleteAdminApiView
from administration.api.views import UserListExportApiView

urlpatterns = [
    url(r"^users/$", UserListAdminApiView.as_view(), name=Names.USER_LIST),
    url(r"^users/export/$", UserListExportApiView.as_view(), name=Names.USER_LIST_EXPORT),
    url(r"^users/(?P<pk>\d+)/$", UserRetrieveUpdateDeleteAdminApiView.as_view(), name=Names.USER_DETAILS),
    url(r"^users/(?P<pk>\d+)/auth/$", AuthAsAUserAdminApiView.as_view(), name=Names.USER_AUTH_ADMIN),
    url(r"^user_actions/$", UserActionListCreateApiView.as_view(), name=Names.USER_ACTION_LIST),
    url(r"^user_actions/(?P<pk>\d+)/$", UserActionDeleteAdminApiView.as_view(), name=Names.USER_ACTION_DETAILS),
    url(r"^access/groups/$", AccessGroupsListApiView.as_view(), name=Names.ACCESS_GROUPS),
]
