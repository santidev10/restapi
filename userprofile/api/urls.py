"""
Userprofile api urls module
"""
from django.conf.urls import url

from userprofile.api.views import UserCreateApiView, UserAuthApiView, \
    UserProfileApiView, UserPasswordResetApiView, UserPasswordSetApiView

urlpatterns = [
    url(r'^users/$', UserCreateApiView.as_view(), name="user_create"),
    url(r'^auth/$', UserAuthApiView.as_view(), name="user_auth"),
    url(r'^users/me/$', UserProfileApiView.as_view(), name="user_profile"),
    url(r'^password_reset/$', UserPasswordResetApiView.as_view(), name="password_reset"),
    url(r'^set_password/$', UserPasswordSetApiView.as_view(), name="set_password"),
]
