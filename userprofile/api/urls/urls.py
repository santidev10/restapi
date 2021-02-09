from django.conf.urls import url

from userprofile.api.urls.names import UserprofilePathName
from userprofile.api.views import ContactFormApiView
from userprofile.api.views import ErrorReportApiView
from userprofile.api.views import UserAuthApiView
from userprofile.api.views import UserAvatarApiView
from userprofile.api.views import UserCreateApiView
from userprofile.api.views import UserPasswordChangeApiView
from userprofile.api.views import UserPasswordResetApiView
from userprofile.api.views import UserPasswordSetApiView
from userprofile.api.views import UserPermissionsManagement
from userprofile.api.views import UserProfileApiView
from userprofile.api.views import WhiteLabelApiView
from userprofile.api.views import UserRoleListCreateAPIView
from userprofile.api.views import UserRoleRetrieveUpdateAPIView


urlpatterns = [
    url(r"^users/$", UserCreateApiView.as_view(), name=UserprofilePathName.CREATE_USER),
    url(r"^auth/$", UserAuthApiView.as_view(), name=UserprofilePathName.AUTH, ),
    url(r"^users/me/$", UserProfileApiView.as_view(), name=UserprofilePathName.USER_PROFILE),
    url(r"^users/me/avatar/$", UserAvatarApiView.as_view(), name=UserprofilePathName.AVATAR),
    url(r"^password_reset/$", UserPasswordResetApiView.as_view(), name="password_reset"),
    url(r"^set_password/$", UserPasswordSetApiView.as_view(), name="set_password"),
    url(r"^change_password/$", UserPasswordChangeApiView.as_view(), name=UserprofilePathName.CHANGE_PASSWORD),
    url(r"^contact_forms/$", ContactFormApiView.as_view(), name="contact_from"),
    url(r"^error_report/$", ErrorReportApiView.as_view(), name="error_report"),
    url(r"^config/$", WhiteLabelApiView.as_view(), name="white_label"),
    url(r"^users/manage_permissions/$", UserPermissionsManagement.as_view(), name="manage_permissions"),
    url(r"^user_roles/$", UserRoleListCreateAPIView.as_view(), name=UserprofilePathName.ROLE_LIST_CREATE),
    url(r"^user_roles/(?P<pk>\d+)/$", UserRoleRetrieveUpdateAPIView.as_view(), name=UserprofilePathName.ROLE_RETRIEVE_UPDATE),
]
