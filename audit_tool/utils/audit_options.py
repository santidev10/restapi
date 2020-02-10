from audit_tool.models import AuditCategory
from audit_tool.models import ChannelType
from brand_safety.models import BadWordCategory
from brand_safety.api.serializers.bad_word_category_serializer import BadWordCategorySerializer


class AuditOptions(object):
    def __init__(self):
        # get iab categories set
        pass

    @staticmethod
    def brand_safety_categories():
        all_categories = BadWordCategory.objects.all()
        data = BadWordCategorySerializer(all_categories, many=True).data
        return data

    @staticmethod
    def channel_types():
        all_types = ChannelType.objects.all()
        data = {
            item.id: item.channel_type for item in all_types
        }
        return data

    @staticmethod
    def content_categories():
        # Get all iab categories
        # Use static mapping to separate items into tiers
        all_categories = AuditCategory.get_all(iab=True, unique=True)
        pass

    @staticmethod
    def gender():
        pass