from audit_tool.models import BlacklistItem
from utils.serializers.fields import ParentDictValueField
from video.api.serializers.video import VideoSerializer


# todo: duplicates ChannelWithBlackListSerializer
class VideoWithBlackListSerializer(VideoSerializer):
    blacklist_data = ParentDictValueField("blacklist_data", source="main.id")

    def __init__(self, instance, *args, **kwargs):
        super(VideoWithBlackListSerializer, self).__init__(instance, *args, **kwargs)
        self.blacklist_data = {}
        if instance:
            videos = instance if isinstance(instance, list) else [instance]
            self.blacklist_data = self.fetch_blacklist_items(videos)

    def fetch_blacklist_items(cls, videos):
        doc_ids = [doc.meta.id for doc in videos]
        blacklist_items = BlacklistItem.get(doc_ids, BlacklistItem.VIDEO_ITEM)
        blacklist_items_by_id = {
            item.item_id: {
                "blacklist_data": item.to_dict()
            } for item in blacklist_items
        }
        return blacklist_items_by_id
