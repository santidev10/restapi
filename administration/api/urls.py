"""
Administration api urls module
"""
from django.conf.urls import url

from administration.api.views import UserListAdminApiView

urlpatterns = [
    url(r'^users/$', UserListAdminApiView.as_view(), name="user_list"),
]
