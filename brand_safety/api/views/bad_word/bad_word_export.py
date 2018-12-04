from django.http import StreamingHttpResponse
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework_csv.renderers import CSVStreamingRenderer

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.models import BadWord


class BadWordCSVRendered(CSVStreamingRenderer):
    header = ["id", "name", "category"]
    labels = {
        "id": "Id",
        "name": "Name",
        "category": "Category",
    }


class BadWordExportApiView(ListAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    renderer_classes = (BadWordCSVRendered,)
    queryset = BadWord.objects.all().order_by("name")

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())

        def data_generator():
            for bad_word in queryset:
                yield self.get_serializer(bad_word).data

        renderer = BadWordCSVRendered()
        response = StreamingHttpResponse(renderer.render(data_generator()), content_type="text/csv")
        response["Content-Disposition"] = "attachment; filename=\"Bad Words.csv\""
        return response
