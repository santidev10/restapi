from django.conf import settings
from rest_framework.exceptions import ValidationError
from rest_framework.parsers import JSONParser
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.status import HTTP_403_FORBIDDEN

from userprofile.constants import DEFAULT_DOMAIN
from userprofile.models import WhiteLabel
from userprofile.api.serializers.white_label import WhiteLabelSerializer
from userprofile.api.views.user_avatar import ImageUploadParser
from utils.file_storage.s3_connector import upload_file
from utils.views import validate_fields
from utils.views import get_object
from utils.views import CustomAPIException
from utils.permissions import ReadOnly
from utils.permissions import or_permission_classes


class WhiteLabelApiView(APIView):
    READ_ONLY = ("GET",)
    permission_classes = (
        or_permission_classes(IsAdminUser, ReadOnly),
    )
    parser_classes = (JSONParser, ImageUploadParser)
    IMAGE_FIELDS = ("favicon", "logo")
    ALLOWED_CONFIG_FIELDS = IMAGE_FIELDS + ("domain", "disable",  "name")

    def get(self, request):
        all_domains = request.query_params.get("all")
        if all_domains:
            if not request.user or not request.user.is_staff:
                raise CustomAPIException(HTTP_403_FORBIDDEN, None)
            data = WhiteLabelSerializer(WhiteLabel.objects.all(), many=True).data
        else:
            try:
                domain = (request.get_host() or "").lower().split('viewiq')[0]
                sub_domain = domain.strip(".") or DEFAULT_DOMAIN
            except IndexError:
                sub_domain = DEFAULT_DOMAIN
            data = WhiteLabelSerializer(WhiteLabel.get(domain=sub_domain)).data
        return Response(data)

    def patch(self, request):
        data = request.data
        pk = data.get("id")
        config = data.get("config", {})
        white_label = get_object(WhiteLabel, id=pk)
        validate_fields(config.keys(), self.ALLOWED_CONFIG_FIELDS, should_raise=True)
        serializer = WhiteLabelSerializer(white_label, data=data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(data=data)

    def post(self, request):
        if "application/json" in request.content_type:
            response = self._save_domain(request)
        elif request.content_type == "image/png":
            response = self._save_image(request)
        else:
            raise ValidationError(f"Unsupported Content-Type header: {request.content_type}")
        return response

    def _save_domain(self, request):
        data = request.data
        config = data.get("conifg", {})
        validate_fields(config.keys(), self.ALLOWED_CONFIG_FIELDS, should_raise=True)
        serializer = WhiteLabelSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(data=serializer.data)

    def _save_image(self, request):
        pk = request.query_params.get("id")
        white_label = get_object(WhiteLabel, id=pk)
        image_type = request.query_params.get("image_type")
        if image_type not in self.IMAGE_FIELDS:
            raise ValidationError(f"Invalid image_type: {image_type}")
        filename = f"branding/{white_label.domain}_{image_type}.png"
        image = request.FILES["file"]
        params = {"ACL": "public-read"}
        image_url = upload_file(filename, image, image.content_type,
                                bucket=settings.AMAZON_S3_UI_ASSETS_BUCKET_NAME, extra=params)
        white_label.config[image_type] = image_url
        white_label.save()
        return Response(data={"image_url": image_url})
