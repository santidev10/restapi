from rest_framework.parsers import MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from performiq.api.serializers.map_csv_fields_serializer import MapCSVFieldsSerializer
from performiq.utils.map_csv_fields import CSVColumnMapper


class PerformIQMapCSVFieldsAPIView(APIView):

    permission_classes = (
        IsAuthenticated,
    )

    parser_classes = [MultiPartParser]

    serializer_class = MapCSVFieldsSerializer

    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data
        csv_file = validated_data.get("csv_file")

        mapper = CSVColumnMapper(csv_file)
        data = {
            "mapping": mapper.get_mapping(),
            "column_options": mapper.get_column_options(),
        }
        return Response(status=HTTP_200_OK, data=data)

