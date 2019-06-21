from rest_framework.permissions import IsAdminUser
from rest_framework_csv.renderers import CSVStreamingRenderer
from django_filters.rest_framework import DjangoFilterBackend

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.models import BadWord
from utils.api.file_list_api_view import FileListApiView


class BadWordCSVRendered(CSVStreamingRenderer):
    header = ["name", "category", "language"]
    labels = {
        "name": "Name",
        "category": "Category",
        "language": "Language"
    }


class BadWordExportApiView(FileListApiView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    renderer_classes = (BadWordCSVRendered,)
    filename = "Bad Words.csv"

    def do_filters(self, queryset):
        filters = {}

        language = self.request.query_params.get("language")
        if language:
            filters["language__language"] = language

        if filters:
            queryset = queryset.filter(**filters)
        return queryset

    def get_queryset(self):
        queryset = BadWord.objects.all().order_by("name")
        queryset = self.do_filters(queryset)
        return queryset
