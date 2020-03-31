from es_components.managers.channel import ChannelManager
from es_components.constants import Sections


class ChannelGroupParamAdapter:
    parameter_full_name = "stats.channel_group"

    def adapt(self, query_params):
        parameter = query_params.get(self.parameter_full_name)
        if parameter:
            parameter = parameter.lower().split(" ")[0]
            query_params[self.parameter_full_name] = parameter

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
        channels_to_update = channel_ids[offset:offset+max_upsert_channels]
    return new_channels_counter
