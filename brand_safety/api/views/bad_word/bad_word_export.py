from rest_framework.permissions import IsAdminUser
from rest_framework_csv.renderers import CSVStreamingRenderer
from rest_framework.serializers import ValidationError
from django_filters.rest_framework import DjangoFilterBackend

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.models import BadWord
from utils.api.file_list_api_view import FileListApiView


class BadWordCSVRendered(CSVStreamingRenderer):
    header = ["name", "category", "language", "negative_score"]
    labels = {
        "name": "Name",
        "category": "Category",
        "language": "Language",
        "negative_score": "Score"
    }


class BadWordExportApiView(FileListApiView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    renderer_classes = (BadWordCSVRendered,)
    queryset = BadWord.objects.all().order_by("name")
    filename = "Bad Words.csv"

    def do_filters(self, queryset):
        filters = {}

        category = self.request.query_params.get("category")
        if category:
            try:
                category_id = int(category)
                filters["category_id"] = category_id
            except ValueError:
                raise ValidationError("Category filter param must be Category ID value. Received: {}.".format(category))

        language = self.request.query_params.get("language")

        if language:
            filters["language__language"] = language

        negative_scores = self.request.query_params.get("negative_score")
        if negative_scores:
            negative_scores = negative_scores.split(',')
            filters["negative_score__in"] = negative_scores

        queryset = queryset.filter(**filters)
        return queryset

    def get_queryset(self):
        self.queryset = self.do_filters(self.queryset)
        return self.queryset
