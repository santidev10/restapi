from rest_framework.generics import ListAPIView

from segment.api.mixins import DynamicPersistentModelViewMixin
from segment.api.paginator import SegmentPaginator
from segment.api.serializers import PersistentSegmentSerializer
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles
from segment.models.persistent.constants import PersistentSegmentType
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from utils.permissions import user_has_permission


class PersistentSegmentListApiView(DynamicPersistentModelViewMixin, ListAPIView):
    serializer_class = PersistentSegmentSerializer
    pagination_class = SegmentPaginator
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )

    def get_queryset(self):
        queryset = super().get_queryset().filter(
                                            Q(title__in=PersistentSegmentTitles.MASTER_BLACKLIST_SEGMENT_TITLES)
                                            | Q(title__in=PersistentSegmentTitles.MASTER_WHITELIST_SEGMENT_TITLES)
                                            | Q(category=PersistentSegmentCategory.WHITELIST)
                                            | Q(category=PersistentSegmentCategory.TOPIC)
                                         )
        return queryset

    def finalize_response(self, request, response, *args, **kwargs):
        formatted_response = {
            "items": []
        }
        for item in response.data.get("items", []):
            items_count = item.get("statistics", {}).get("items_count", 0)
            if items_count is None or items_count <= 0:
                continue
            if item.get("title") in PersistentSegmentTitles.MASTER_BLACKLIST_SEGMENT_TITLES:
                formatted_response["master_blacklist"] = item

            if self.model.segment_type == PersistentSegmentType.CHANNEL and item.get("title") in PersistentSegmentTitles.CURATED_CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE:
                formatted_response["master_whitelist"] = item
            elif item.get("title") in PersistentSegmentTitles.MASTER_WHITELIST_SEGMENT_TITLES:
                formatted_response["master_whitelist"] = item

            if item.get("title") not in PersistentSegmentTitles.ALL_MASTER_SEGMENT_TITLES and item.get("title") not in PersistentSegmentTitles.CURATED_CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE:
                formatted_response["items"].append(item)

            if not item.get("thumbnail_image_url"):
                item["thumbnail_image_url"] = S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL

        for item in formatted_response["items"]:
            # remove "Channels " or "Videos " prefix
            prefix = "{}s ".format(item.get("segment_type").capitalize())
            if item.get("title", prefix).startswith(prefix):
                item["title"] = item.get("title", "")[len(prefix):]

        response.data = formatted_response
        return super().finalize_response(request, response, *args, **kwargs)
