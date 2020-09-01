from enum import IntEnum


class OAuthType(IntEnum):
    GOOGLE_ADS = 0
    DV360 = 1


OAUTH_CHOICES = [
    (OAuthType.GOOGLE_ADS.value, "Google Ads Oauth"),
    (OAuthType.DV360.value, "Google DV360 Oauth"),
]
