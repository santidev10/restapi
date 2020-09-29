from enum import IntEnum


class OAuthType(IntEnum):
    GOOGLE_ADS = 0
    DV360 = 1


OAUTH_CHOICES = [
    (OAuthType.GOOGLE_ADS.value, "Google Ads Oauth"),
    (OAuthType.DV360.value, "Google DV360 Oauth"),
]

class EntityStatusType(IntEnum):
    ENTITY_STATUS_UNSPECIFIED = 0
    ENTITY_STATUS_ACTIVE = 1
    ENTITY_STATUS_ARCHIVED = 2
    ENTITY_STATUS_DRAFT = 3
    ENTITY_STATUS_PAUSED = 4
    ENTITY_STATUS_SCHEDULED_FOR_DELETION = 5


ENTITY_STATUS_CHOICES = [
    (EntityStatusType.ENTITY_STATUS_UNSPECIFIED.value, "Unspecified"),
    (EntityStatusType.ENTITY_STATUS_ACTIVE.value, "Active"),
    (EntityStatusType.ENTITY_STATUS_ARCHIVED.value, "Archived"),
    (EntityStatusType.ENTITY_STATUS_DRAFT.value, "Draft"),
    (EntityStatusType.ENTITY_STATUS_PAUSED.value, "Paused"),
    (EntityStatusType.ENTITY_STATUS_SCHEDULED_FOR_DELETION.value, "Scheduled for Deletion"),
]

ENTITY_STATUS_MAP_TO_ID = {status.name: status.value for status in EntityStatusType}

ENTITY_STATUS_MAP_TO_STR = {status.value: status.name for status in EntityStatusType}