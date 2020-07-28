from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditContentType
from audit_tool.models import AuditGender
from audit_tool.models import AuditContentQuality
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager


class ChannelGroupParamAdapter:
    parameter_full_name = "stats.channel_group"

    def adapt(self, query_params):
        parameter = query_params.get(self.parameter_full_name)
        if parameter:
            parameter = parameter.lower().split(" ")[0]
            query_params[self.parameter_full_name] = parameter

        return query_params


class VettedParamsAdapter:
    parameters = ["task_us_data.age_group", "task_us_data.content_type", "task_us_data.gender",
                  "task_us_data.content_quality"]
    mappings = {
        "task_us_data.age_group": AuditAgeGroup.to_id,
        "task_us_data.content_type": AuditContentType.to_id,
        "task_us_data.gender": AuditGender.to_id,
        "task_us_data.content_quality": AuditContentQuality.to_id,
    }

    def adapt(self, query_params):
        for param in self.parameters:
            parameter = query_params.get(param)
            if parameter:
                values = parameter.lower().split(",")
                mapped_values = [str(self.mappings[param][value]) for value in values]
                query_params[param] = mapped_values
        return query_params


class IsTrackedParamsAdapter:
    parameter_name = "custom_properties.is_tracked"

    def adapt(self, query_params):
        parameter = query_params.get(self.parameter_name)
        if parameter == "Tracked Channels":
            query_params[self.parameter_name] = [True]
        return query_params


def track_channels(channel_ids):
    max_upsert_channels = 10000
    manager = ChannelManager(sections=(Sections.MAIN, Sections.CUSTOM_PROPERTIES),
                             upsert_sections=(Sections.MAIN, Sections.CUSTOM_PROPERTIES))
    offset = 0
    channels_to_update = channel_ids[:max_upsert_channels]
    new_channels_counter = 0
    while channels_to_update:
        channels = manager.get_or_create(channels_to_update, only_new=True)
        new_channels_counter += len(channels)
        for channel in channels:
            channel.populate_custom_properties(is_tracked=True)
        manager.upsert(channels)
        offset += max_upsert_channels
        channels_to_update = channel_ids[offset:offset + max_upsert_channels]
    return new_channels_counter
