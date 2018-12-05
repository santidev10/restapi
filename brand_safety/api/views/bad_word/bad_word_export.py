from rest_framework.permissions import IsAdminUser
from rest_framework_csv.renderers import CSVStreamingRenderer

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.models import BadWord
from utils.api.file_list_api_view import FileListApiView


class BadWordCSVRendered(CSVStreamingRenderer):
    header = ["id", "name", "category"]
    labels = {
        "id": "Id",
        "name": "Name",
        "category": "Category",
    }


class BadWordExportApiView(FileListApiView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    renderer_classes = (BadWordCSVRendered,)
    queryset = BadWord.objects.all().order_by("name")
    filename = "Bad Words.csv"
