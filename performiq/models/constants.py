from collections import namedtuple
from enum import IntEnum
from enum import Enum


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


class CampaignDataFields:
    AD_GROUP_ID = "ad_group_id"
    CHANNEL_ID = "channel_id"
    IMPRESSIONS = "impressions"
    VIDEO_VIEWS = "video_views"
    COST = "cost"
    ACTIVE_VIEW_VIEWABILITY = "active_view_viewability"
    VIDEO_VIEW_RATE = "video_view_rate"
    VIDEO_QUARTILE_100_RATE = "video_quartile_100_rate"
    CTR = "ctr"
    CPM = "cpm"
    CPV = "cpv"


class CampaignData:
    def __init__(self, raw_data, fields_mapping, data_type=OAuthType.GOOGLE_ADS):
        config = {
            OAuthType.GOOGLE_ADS: self.gads
        }
        self._raw_data = raw_data
        self._fields_mapping = fields_mapping
        self._data_type = data_type
        self._data = config[data_type]()

    @property
    def data(self):
        return self._data

    def gads(self):
        gads_data = namedtuple("CampaignData", self._fields_mapping.values())
        mapped_data = {
            mapped_key: getattr(self._raw_data, field, None) for field, mapped_key in self._fields_mapping.items()
        }
        data = gads_data(**mapped_data)
        return data


    def to_dict(self):
        pass