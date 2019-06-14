from django.db.models import Q
from segment.utils import get_persistent_segment_model_by_type
from segment.utils import get_segment_model_by_type


class DynamicModelViewMixin(object):
    def dispatch(self, request, segment_type, **kwargs):
        self.model = get_segment_model_by_type(segment_type)
        self.serializer_class.Meta.model = self.model
        return super().dispatch(request, **kwargs)

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        if self.request.user.is_staff:
            queryset = self.model.objects.all()
        elif self.request.user.has_perm('userprofile.view_pre_baked_segments'):
            queryset = self.model.objects.filter(
                Q(owner=self.request.user)
                | ~Q(category='private')
                | Q(shared_with__contains=[self.request.user.email])
            )
        else:
            queryset = self.model.objects.filter(
                Q(owner=self.request.user)
                | Q(shared_with__contains=[self.request.user.email])
            )
        return queryset


class DynamicPersistentModelViewMixin(object):
    def dispatch(self, request, segment_type, **kwargs):
        self.model = get_persistent_segment_model_by_type(segment_type)
        if hasattr(self, "serializer_class"):
            self.serializer_class.Meta.model = self.model
        return super().dispatch(request, **kwargs)

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        queryset = self.model.objects.all().order_by("title")
        return queryset
