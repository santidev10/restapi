from django.contrib.postgres.fields.jsonb import KeyTextTransform
from django.db.models import Q
from rest_framework.generics import ListAPIView

from segment.api.mixins import DynamicPersistentModelViewMixin
from segment.api.paginator import SegmentPaginator
from segment.api.serializers.serializers import PersistentSegmentSerializer
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

    def get_queryset(self):
        request_origin = self.request.META.get("HTTP_ORIGIN") or self.request.META.get("HTTP_REFERER")
        if is_correct_apex_domain(request_origin):
            queryset = super().get_queryset().filter(Q(category=PersistentSegmentCategory.APEX) | Q(is_master=True))
        else:
            queryset = super().get_queryset()\
                .filter(Q(category=PersistentSegmentCategory.WHITELIST) | Q(is_master=True))\
                .annotate(items_count=KeyTextTransform("items_count", "details"))\
                .exclude(Q(items_count__lte=0) & Q(is_master=False))
        return queryset

    def finalize_response(self, request, response, *args, **kwargs):
        data = {
            "master_blacklist": None,
            "master_whitelist": None,
            "items": []
        }
        for item in response.data.get("items", []):
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

