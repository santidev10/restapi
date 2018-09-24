from django.conf import settings
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from utils.api.parsers.file_size_limited_parser import FileSizeLimitedParser
from utils.datetime import TIMESTAMP_FORMAT
from utils.datetime import now_in_default_tz
from utils.file_storage import upload_file


class ImageUploadParser(FileSizeLimitedParser):
    media_type = "image/png"

    @property
    def size_limit_mb(self):
        return settings.MAX_AVATAR_SIZE_MB

    def get_filename(self, stream, media_type, parser_context):
        return "temp.png"


class UserAvatarApiView(APIView):
    permission_classes = (IsAuthenticated,)
    parser_classes = (ImageUploadParser,)

    def post(self, request):
        user = request.user
        timestamp = now_in_default_tz().strftime(TIMESTAMP_FORMAT)
        filename = "user/{user_id}/avatar.png".format(
            user_id=user.id,
        )
        image = request.FILES["file"]
        avatar_url = upload_file(filename, image.file, image.content_type)
        avatar_url_with_timestamp = "{url}?t={timestamp}".format(
            url=avatar_url,
            timestamp=timestamp,
        )
        user.profile_image_url = avatar_url_with_timestamp
        user.save()
        return Response(data=avatar_url_with_timestamp)

    def delete(self, request):
        user = request.user
        user.profile_image_url = None
        user.save()
        return Response()
