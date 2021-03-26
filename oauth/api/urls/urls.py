from django.conf.urls import url

from oauth.api.views import OAuthAccountUpdateAPIView
from .names import OAuthPathName

urlpatterns = [
    url(r"^performiq/oauth_accounts/(?P<pk>\d+)/$",
        OAuthAccountUpdateAPIView.as_view(),
        name=OAuthPathName.OAUTH_ACCOUNT_UPDATE),
]
