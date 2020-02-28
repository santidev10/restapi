from audit_tool.utils.audit_utils import AuditUtils
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentChannelWithMonetizationExportSerializer
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentVideoExportSerializer


class CustomSegmentChannelVettedExportSerializer(CustomSegmentChannelWithMonetizationExportSerializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Overall_Score", "Vetted", "Monetizable")

    def get_vetted(self, obj):
        """
        extra_data provided by inheritance
        :param obj:
        :return:
        """
        item_id = obj.main.id
        vetting_data = self.extra_data.get(item_id, {})
        skipped = vetting_data.get("skipped", None)
        suitability = vetting_data.get("clean", None)
        vetted_value = AuditUtils.get_vetting_value(skipped, suitability)
        return vetted_value


class CustomSegmentVideoVettedExportSerializer(CustomSegmentVideoExportSerializer):
    columns = ("URL", "Title", "Language", "Category", "Views", "Overall_Score")

    def get_vetted(self, obj):
        """
        extra_data provided by inheritance
        :param obj:
        :return:
        """
        item_id = obj.main.id
        vetting_data = self.extra_data.get(item_id, {})
        skipped = vetting_data.get("skipped", None)
        suitability = vetting_data.get("clean", None)
        vetted_value = AuditUtils.get_vetting_value(skipped, suitability)
        return vetted_value

