from audit_tool.api.serializers.audit_channel_vet_serializer import AuditChannelVetSerializer
from audit_tool.api.serializers.audit_video_vet_serializer import AuditVideoVetSerializer
from audit_tool.models import AuditChannel
from audit_tool.models import AuditVideo
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditVideoVet


class SegmentAuditUtils(object):
    def __init__(self, segment_type):
        """
        Segment audit utility class
        :param segment_type: int
        """
        if segment_type not in range(2):
            raise ValueError(f"segment_type must be 0-1")
        self._segment_type = segment_type
        self._video_config = {
            "model": AuditVideo,
            "meta_model": AuditVideoMeta,
            "vetting_model": AuditVideoVet,
            "serializer": AuditVideoVetSerializer,
        }
        self._channel_config = {
            "model": AuditChannel,
            "meta_model": AuditChannelMeta,
            "vetting_model": AuditChannelVet,
            "serializer": AuditChannelVetSerializer,
        }

        # Access config types with segment_type in properties
        self._config = {
            0: self._video_config,
            1: self._channel_config,
        }

    @property
    def model(self):
        return self._get_key("model")

    @property
    def meta_model(self):
        return self._get_key("meta_model")

    @property
    def vetting_model(self):
        return self._get_key("vetting_model")

    @property
    def serializer(self):
        return self._get_key("serializer")

    def _get_key(self, key):
        try:
            value = self._config[self._segment_type][key]
        except KeyError:
            raise AttributeError(f"Attribute not found: {key}")
        return value
