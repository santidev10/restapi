from django.contrib.postgres.fields.jsonb import KeyTextTransform
from django.db.models import IntegerField
from django.db.models import Q
from django.db.models import F
from django.db.models.functions import Cast
from rest_framework.generics import ListAPIView
from rest_framework.serializers import ValidationError

from segment.api.paginator import SegmentPaginator
from segment.api.serializers.ctl_serializer import CTLSerializer
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import SegmentListType
from segment.models.custom_segment import CustomSegment
from userprofile.constants import StaticPermissions


class SegmentListApiView(ListAPIView):
    ALLOWED_SORTS = ["items", "created_at", "updated_at", "title"]
    serializer_class = CTLSerializer
    pagination_class = SegmentPaginator
    queryset = CustomSegment.objects.all().select_related("export").order_by("created_at")
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.BUILD__CTL, StaticPermissions.BUILD__CTL_VET,
                                    StaticPermissions.BUILD__CTL_SEE_ALL),
    )

    def _do_filters(self, queryset):
        """
        Filter queryset

        :return: Queryset
        """
        filters = {}
        q_filter = Q()

        search = self.request.query_params.get("search")
        if search:
            filters["title__icontains"] = search

        list_type = self.request.query_params.get("list_type")
        if list_type:
            value = SegmentListType[list_type.upper()].value
            filters["list_type"] = value

        content_categories = self.request.query_params.get("general_data.iab_categories")
        if content_categories:
            for category in content_categories.split(","):
                q_filter |= Q(export__query__params__content_categories__icontains=category)

        # Channel and video language query param differ. Filter out empty str items
        languages = self.request.query_params.get("general_data.top_lang_code", "").split(",") \
                    + self.request.query_params.get("general_data.lang_code", "").split(",")
        languages = list(filter(bool, languages))
        if languages:
            for lang in languages:
                q_filter |= Q(export__query__params__languages__icontains=lang)
        queryset = queryset.filter(q_filter, **filters)
        return queryset

    def _do_sorts(self, queryset):
        """
        Sort queryset

        :param queryset: Queryset
        :return: Queryset
        """
        try:
            sort_by = self.request.query_params["sort_by"]
            if sort_by not in self.ALLOWED_SORTS:
                raise ValidationError("Allowed sorts: {}".format(", ".join(self.ALLOWED_SORTS)))
            if sort_by == "items":
                queryset = queryset.annotate(items=Cast(KeyTextTransform("items_count", "statistics"), IntegerField()))
                if self.request.query_params.get("ascending"):
                    queryset = queryset.order_by(F("items").asc(nulls_first=True))
                else:
                    queryset = queryset.order_by(F("items").desc(nulls_last=True))
            else:
                invert = True if sort_by in ["updated_at", "created_at"] else False
                if self.request.query_params.get("ascending"):
                    sort_by = "{}".format(sort_by) if invert else "-{}".format(sort_by)
                else:
                    sort_by = "-{}".format(sort_by) if invert else "{}".format(sort_by)
                queryset = queryset.order_by(sort_by)
        except KeyError:
            pass
        return queryset

    def get_queryset(self):
        """
        Prepare queryset to display

        :return: Queryset
        """
        # Filter queryset depending on permission level
        user = self.request.user
        if user.has_permission(StaticPermissions.BUILD__CTL_SEE_ALL):
            base_filters = {}
        elif user.has_permission(StaticPermissions.BUILD__CTL_VET):
            base_filters = {"audit_id__isnull": False}
        else:
            base_filters = {"owner": self.request.user}
        queryset = super().get_queryset().filter(**base_filters)
        segment_type = self.request.query_params.get("segment_type", "")
        # Only filter for segment type if not sending both
        if ("channel" in segment_type and "video" in segment_type) is False and segment_type:
            segment_type = SegmentTypeEnum[segment_type.upper()].value
            queryset = queryset.filter(segment_type=segment_type)
        queryset = self._do_filters(queryset)
        queryset = self._do_sorts(queryset)
        return queryset

    def paginate_queryset(self, queryset):
        """
        Processing flat query param

        :return: Queryset
        """
        flat = self.request.query_params.get("flat")
        if flat == "1":
            return None
        return super().paginate_queryset(queryset)
