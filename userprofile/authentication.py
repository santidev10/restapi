from datetime import timedelta

from django.conf import settings
from django.utils import timezone
from rest_framework.authentication import TokenAuthentication
from rest_framework.exceptions import AuthenticationFailed


class ExpiringTokenAuthentication(TokenAuthentication):
    """
    Validate tokens that are within creation threshold
    """
    def authenticate_credentials(self, key):
        user, token = super().authenticate_credentials(key)
        threshold = timezone.now() - timedelta(days=settings.AUTH_TOKEN_EXPIRES)
        if token.created < threshold:
            raise AuthenticationFailed("Token expired. Please log in again.")
        if token.key.startswith("temp_"):
            raise AuthenticationFailed(
                "You have provided a temporary token which does not grant you access to this page. Please log in again."
            )
        return user, token
