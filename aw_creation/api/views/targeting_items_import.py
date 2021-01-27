import re

from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_creation.api.serializers import add_targeting_list_items_info
from aw_reporting.models import Audience
from aw_reporting.models import Topic
from userprofile.constants import StaticPermissions
from utils.views import XLSX_CONTENT_TYPE
from .document_import_base import DOCUMENT_LOAD_ERROR_TEXT
from .document_import_base import DocumentImportBaseAPIView


class TargetingItemsImportApiView(DocumentImportBaseAPIView):
    parser_classes = (MultiPartParser,)
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MEDIA_BUYING),)

    def post(self, request, list_type, **_):

        method = "import_{}_criteria".format(list_type)
        if not hasattr(self, method):
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Unsupported list type: {}".format(list_type))

        criteria_list = []
        for _, file_obj in request.data.items():
            if not hasattr(file_obj, "content_type"):
                # skip empty items
                continue

            fct = file_obj.content_type
            try:
                if fct == XLSX_CONTENT_TYPE:
                    data = self.get_xlsx_contents(file_obj, return_lines=True)
                elif fct in ("text/csv", "application/vnd.ms-excel"):
                    data = self.get_csv_contents(file_obj, return_lines=True)
                else:
                    return Response(status=HTTP_400_BAD_REQUEST,
                                    data={
                                        "errors": [DOCUMENT_LOAD_ERROR_TEXT]
                                    })
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                return Response(status=HTTP_400_BAD_REQUEST,
                                data={
                                    "errors": [DOCUMENT_LOAD_ERROR_TEXT,
                                               "Stage: Load File Data. Cause: {}".format(
                                                   e)]
                                })

            try:
                criteria_list.extend(getattr(self, method)(data))
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                return Response(
                    status=HTTP_400_BAD_REQUEST,
                    data={
                        "errors": [DOCUMENT_LOAD_ERROR_TEXT,
                                   "Stage: Data Extraction. Cause: {}".format(
                                       e)]
                    },
                )

        add_targeting_list_items_info(criteria_list, list_type)

        return Response(criteria_list)

    @staticmethod
    def import_keyword_criteria(data):
        kws = []
        data = list(data)
        for line in data[1:]:
            if line:
                criteria = line[0]
            else:
                continue

            if re.search(r"\w+", criteria):
                kws.append(
                    dict(criteria=criteria)
                )
        return kws

    @staticmethod
    def import_channel_criteria(data):
        channels = []
        channel_pattern = re.compile(r"[\w-]{24}")

        for line in data:
            criteria = None
            if len(line) > 1:
                first, second, *_ = line
            elif line:
                first, second = line[0], ""
            else:
                continue

            match = channel_pattern.search(second)
            if match:
                criteria = match.group(0)
            else:
                match = channel_pattern.search(first)
                if match:
                    criteria = match.group(0)

            if criteria:
                channels.append(
                    dict(criteria=criteria)
                )
        return channels

    @staticmethod
    def import_video_criteria(data):
        videos = []
        pattern = re.compile(r"[\w-]{11}")
        for line in data:
            criteria = None
            if len(line) > 1:
                first, second, *_ = line
            elif line:
                first, second = line[0], ""
            else:
                continue

            match = pattern.search(second)
            if match:
                criteria = match.group(0)
            else:
                match = pattern.search(first)
                if match:
                    criteria = match.group(0)

            if criteria:
                videos.append(
                    dict(criteria=criteria)
                )
        return videos

    @staticmethod
    def import_topic_criteria(data):
        objects = []
        topic_ids = set(Topic.objects.values_list("id", flat=True))
        for line in data:
            if line:
                criteria = line[0]
            else:
                continue

            try:
                criteria = int(criteria)
            except ValueError:
                continue
            else:
                if criteria in topic_ids:
                    objects.append(
                        dict(criteria=criteria)
                    )
        return objects

    @staticmethod
    def import_interest_criteria(data):
        objects = []
        interest_ids = set(
            Audience.objects.filter(
                type__in=(Audience.IN_MARKET_TYPE, Audience.AFFINITY_TYPE)
            ).values_list("id", flat=True)
        )
        for line in data:
            if line:
                criteria = line[0]
            else:
                continue

            try:
                criteria = int(criteria)
            except ValueError:
                continue
            else:
                if criteria in interest_ids:
                    objects.append(
                        dict(criteria=criteria)
                    )
        return objects
