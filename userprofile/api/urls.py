"""
Userprofile api urls module
"""
from django.conf.urls import url

from userprofile.api.views import UserCreateApiView

urlpatterns = [
    url(r'^users/$', UserCreateApiView.as_view(), name="user_create"),
]
