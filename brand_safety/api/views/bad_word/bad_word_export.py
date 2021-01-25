from rest_framework.permissions import IsAdminUser
from rest_framework.serializers import ValidationError
from rest_framework_csv.renderers import CSVStreamingRenderer

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.models import BadWord
from userprofile.constants import StaticPermissions
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
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.BSTE),)
    serializer_class = BadWordSerializer
    renderer_classes = (BadWordCSVRendered,)
    queryset = BadWord.objects.all().order_by("name")
    filename = "Bad Words.csv"
    MIN_SEARCH_LENGTH = 2

    def do_filters(self, queryset):
        filters = {}

        search = self.request.query_params.get("search")
        if search:
            if len(search) < self.MIN_SEARCH_LENGTH:
                raise ValidationError("Search term must be at least {} characters.".format(self.MIN_SEARCH_LENGTH))
            filters["name__icontains"] = search
            queryset = queryset.filter(**filters)
            return queryset

        category = self.request.query_params.get("category")
        if category:
            try:
                category_id = int(category)
                filters["category_id"] = category_id
            except ValueError:
                raise ValidationError(
                    "Category filter param must be Category ID value. Received: {}.".format(category))

        language = self.request.query_params.get("language")

        if language:
            filters["language__language"] = language

        negative_scores = self.request.query_params.get("negative_score")
        if negative_scores:
            negative_scores = negative_scores.split(",")
            filters["negative_score__in"] = negative_scores

        queryset = queryset.filter(**filters)
        return queryset

    def get_queryset(self):
        self.queryset = self.do_filters(self.queryset)
        return self.queryset
