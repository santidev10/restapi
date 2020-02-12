from django.db.models import Q
from rest_framework.generics import ListAPIView

from segment.api.mixins import DynamicPersistentModelViewMixin
from segment.api.paginator import SegmentPaginator
from segment.api.serializers.persistent_segment_serializer import PersistentSegmentSerializer
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from userprofile.utils import is_correct_apex_domain
from utils.permissions import user_has_permission


class PersistentSegmentListApiView(DynamicPersistentModelViewMixin, ListAPIView):
    serializer_class = PersistentSegmentSerializer
    pagination_class = SegmentPaginator
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )
    MINIMUM_ITEMS_COUNT = 100

    def get_queryset(self):
        """
        Filter queryset depending on APEX request HTTP_ORIGIN or HTTP_REFERER

        :return: Queryset
        """
        request_origin = self.request.META.get("HTTP_ORIGIN", "") or self.request.META.get("HTTP_REFERER", "")
        if is_correct_apex_domain(request_origin):
            queryset = super().get_queryset().filter(Q(category=PersistentSegmentCategory.APEX) | Q(is_master=True))
        else:
            queryset = super().get_queryset().filter(Q(category=PersistentSegmentCategory.WHITELIST) | Q(is_master=True))
        return queryset

    def finalize_response(self, request, response, *args, **kwargs):
        """
        Format data for response and excludes items with items_count values less than threshold
        :return: Response
        """
        data = {
            "master_blacklist": None,
            "master_whitelist": None,
            "items": []
        }
        for item in response.data.get("items", []):
            if (not item.get("statistics") or item["statistics"].get("items_count", 0) < self.MINIMUM_ITEMS_COUNT) and item["is_master"] is False:
                continue
            if item["category"] == PersistentSegmentCategory.WHITELIST and item["is_master"] is True:
                data["master_whitelist"] = item
            elif item["category"] == PersistentSegmentCategory.BLACKLIST and item["is_master"] is True:
                data["master_blacklist"] = item
            else:
                # remove "Channels " or "Videos " prefix
                prefix = "{}s ".format(item.get("segment_type").capitalize())
                if item.get("title", prefix).startswith(prefix):
                    item["title"] = item.get("title", "")[len(prefix):]
                if not item.get("thumbnail_image_url"):
                    item["thumbnail_image_url"] = S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
                data["items"].append(item)
        response.data = data
        return super().finalize_response(request, response, *args, **kwargs)
