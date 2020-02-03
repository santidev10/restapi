from datetime import timedelta

from django.conf import settings
from django.utils import timezone

from userprofile.models import UserDeviceToken


def clean_device_auth_tokens():
    """
    Delete old auth tokens not in use anymore
    """
    threshold = timezone.now() - timedelta(days=settings.AUTH_TOKEN_EXPIRES)
    UserDeviceToken.objects.filter(created_at__lt=threshold).delete()
