import uuid
from typing import Type
from django.conf import settings
from django.core.files.uploadedfile import UploadedFile
from rest_framework.parsers import MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from performiq.api.serializers.map_csv_fields_serializer import MapCSVFieldsSerializer
from performiq.utils.map_csv_fields import CSVColumnMapper
from utils.aws.s3_exporter import S3Exporter


class PerformIQMapCSVFieldsAPIView(APIView):

    permission_classes = (
        IsAuthenticated,
    )

    parser_classes = [MultiPartParser]

    serializer_class = MapCSVFieldsSerializer

    def post(self, request, *args, **kwargs):
        """
        Guess csv field mapping, and return the guess map. Upload the csv to s3 and return the s3 key
        :param request:
        :param args:
        :param kwargs:
        :return: Response
        """
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data
        csv_file = validated_data.get("csv_file")

        mapper = CSVColumnMapper(csv_file)
        data = {
            "mapping": mapper.get_mapping(),
            "column_options": mapper.get_column_options(),
            "s3_key": self._export_to_s3(csv_file),
        }
        return Response(status=HTTP_200_OK, data=data)

    def _export_to_s3(self, csv_file: Type[UploadedFile]) -> str:
        """
        export a given uploaded CSV file to s3
        :param csv_file:
        :return:
        """
        csv_file.seek(0)
        s3_key = PerformiqCustomCampaignUploadS3Exporter.get_s3_key()
        PerformiqCustomCampaignUploadS3Exporter.export_to_s3(csv_file, s3_key)
        return s3_key


class PerformiqCustomCampaignUploadS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME

    @classmethod
    def get_s3_key(cls):
        id = uuid.uuid4()
        return f"{id}.csv"

    @classmethod
    def export_to_s3(cls, exported_file, key):
        cls._s3().put_object(
            Bucket=cls.bucket_name,
            Key=key,
            Body=exported_file
        )
