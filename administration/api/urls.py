"""
Administration api urls module
"""
from django.conf.urls import url

from administration.api.views import UserListAdminApiView, \
    UserDeleteAdminApiView, AuthAsAUserAdminApiView

urlpatterns = [
    url(r'^users/$', UserListAdminApiView.as_view(), name="user_list"),
    url(r'^users/(?P<pk>\d+)/$', UserDeleteAdminApiView.as_view(),
        name="user_details"),
    url(r'^users/(?P<pk>\d+)/auth/$', AuthAsAUserAdminApiView.as_view(),
        name="user_auth_admin"),
]
