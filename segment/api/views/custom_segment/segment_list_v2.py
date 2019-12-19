from django.contrib.postgres.fields.jsonb import KeyTextTransform
from django.db.models import IntegerField
from django.db.models.functions import Cast
from rest_framework.generics import ListAPIView
from rest_framework.serializers import ValidationError

from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.api.paginator import SegmentPaginator
from segment.models.custom_segment import CustomSegment


class SegmentListApiViewV2(ListAPIView):
    ALLOWED_SORTS = {
        "items",
        "created_at",
        "updated_at",
        "title"
    }
    serializer_class = CustomSegmentSerializer
    pagination_class = SegmentPaginator
    queryset = CustomSegment.objects.all().order_by("created_at")

    def _do_filters(self, queryset):
        """
        Filter queryset
        """
        filters = {}
        # search
        search = self.request.query_params.get("search")
        if search:
            filters["title__icontains"] = search
        # list type
        list_type = self.request.query_params.get("list_type")
        if list_type:
            value = CustomSegmentSerializer.map_to_id(list_type, item_type="list")
            filters["list_type"] = value
        if filters:
            queryset = queryset.filter(**filters)
        return queryset

    def _do_sorts(self, queryset):
        try:
            sort_by = self.request.query_params["sort_by"]
            if sort_by not in self.ALLOWED_SORTS:
                raise ValidationError("Allowed sorts: {}".format(", ".join(self.ALLOWED_SORTS)))
            if sort_by == "items":
                queryset = queryset.annotate(items=Cast(KeyTextTransform("items_count", "statistics"), IntegerField()))
            if self.request.query_params.get("ascending"):
                sort_by = "{}".format(sort_by)
            else:
                sort_by = "-{}".format(sort_by)
            queryset = queryset.order_by(sort_by)
        except KeyError:
            pass
        return queryset

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        segment_type = CustomSegmentSerializer.map_to_id(self.kwargs["segment_type"], item_type="segment")
        queryset = super().get_queryset().filter(owner=self.request.user, segment_type=segment_type)
        queryset = self._do_filters(queryset)
        queryset = self._do_sorts(queryset)
        return queryset

    def paginate_queryset(self, queryset):
        """
        Processing flat query param
        """
        flat = self.request.query_params.get("flat")
        if flat == "1":
            return None
        return super().paginate_queryset(queryset)
