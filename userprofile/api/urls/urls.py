"""
Userprofile api urls module
"""
from django.conf.urls import url

from userprofile.api.urls.names import Name
from userprofile.api.views import UserCreateApiView, UserAuthApiView, \
    UserProfileApiView, UserPasswordResetApiView, UserPasswordSetApiView, \
    ContactFormApiView, VendorDetailsApiView, ErrorReportApiView, \
    UserProfileSharedListApiView
from userprofile.api.views import UserPasswordChangeApiView

urlpatterns = [
    url(r'^users/$', UserCreateApiView.as_view(), name="user_create"),
    url(r'^auth/$', UserAuthApiView.as_view(), name=Name.AUTH, ),
    url(r'^users/me/$', UserProfileApiView.as_view(), name=Name.USER_PROFILE),
    url(r'^users/me/collaborators/$', UserProfileSharedListApiView.as_view(),
        name="user_profile_collaborators"),
    url(r'^password_reset/$', UserPasswordResetApiView.as_view(),
        name="password_reset"),
    url(r'^set_password/$', UserPasswordSetApiView.as_view(),
        name="set_password"),
    url(r'^change_password/$', UserPasswordChangeApiView.as_view(),
        name="change_password"),
    url(r'^contact_forms/$',
        ContactFormApiView.as_view(), name="contact_from"),
    url(r'^vendor/$',
        VendorDetailsApiView.as_view(), name="vendor_details"),
    url(r'^error_report/$', ErrorReportApiView.as_view(), name="error_report"),
]
