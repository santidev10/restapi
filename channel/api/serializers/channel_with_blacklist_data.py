from audit_tool.models import BlacklistItem
from channel.api.serializers.channel import ChannelSerializer
from utils.serializers.fields import ParentDictValueField


# todo: duplicates VideoWithBlackListSerializer
class ChannelWithBlackListSerializer(ChannelSerializer):
    blacklist_data = ParentDictValueField("blacklist_data", source="main.id")

    def __init__(self, instance, *args, **kwargs):
        super(ChannelWithBlackListSerializer, self).__init__(instance, *args, **kwargs)
        self.blacklist_data = {}
        if instance:
            channels = instance if isinstance(instance, list) else [instance]
            self.blacklist_data = self.fetch_blacklist_items(channels)

    def fetch_blacklist_items(cls, channels):
        doc_ids = [doc.meta.id for doc in channels]
        blacklist_items = BlacklistItem.get(doc_ids, BlacklistItem.CHANNEL_ITEM)
        blacklist_items_by_id = {
            item.item_id: item.to_dict() for item in blacklist_items
        }
        return blacklist_items_by_id
