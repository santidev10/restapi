import re

from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.models import GeoTarget
from utils.views import XLSX_CONTENT_TYPE
from .document_import_base import DOCUMENT_LOAD_ERROR_TEXT
from .document_import_base import DocumentImportBaseAPIView


class DocumentToChangesApiView(DocumentImportBaseAPIView):
    """
    Send a post request with multipart-ford-data encoded file data
    key: "file"
    will return
    {"result":[{"name":"94002,California,United States","id":9031903}, ..],
                "undefined":[]}
    """
    parser_classes = (MultiPartParser,)

    def post(self, request, content_type, **_):
        file_obj = request.data["file"]
        fct = file_obj.content_type
        if fct == XLSX_CONTENT_TYPE:
            data = self.get_xlsx_contents(file_obj)
        elif fct in ("text/csv", "application/vnd.ms-excel"):
            data = self.get_csv_contents(file_obj)
        else:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data={"errors": [DOCUMENT_LOAD_ERROR_TEXT]})
        if content_type == "postal_codes":
            try:
                response_data = self.get_location_rules(data)
            except UnicodeDecodeError:
                return Response(
                    status=HTTP_400_BAD_REQUEST,
                    data={"errors": [DOCUMENT_LOAD_ERROR_TEXT]},
                )
        else:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data={
                    "errors": ["The content type isn't supported: "
                               "{}".format(content_type)]
                })
        return Response(status=HTTP_200_OK, data=response_data)

    # pylint: disable=too-many-nested-blocks
    def get_location_rules(self, items):
        items = set(str(i) for i in items if i)
        geo_targets = self.get_geo_targets(items)
        result = [dict(id=i["id"], name=i["canonical_name"])
                  for i in geo_targets]
        undefined = list(items - set(i["name"] for i in geo_targets))
        if undefined:
            # let's search for zip+4 postal codes
            re_sub = re.sub
            numeric_values = [re_sub(r"\D", "", i) for i in undefined]
            plus_4_zips = filter(lambda i: len(i) == 9, numeric_values)
            common_codes = [c[:5] for c in plus_4_zips]
            if common_codes:
                geo_targets = self.get_geo_targets(common_codes)
                if geo_targets:
                    valid_zips = set(i["name"] for i in geo_targets)
                    result.extend(
                        [dict(id=i["id"], name=i["canonical_name"])
                         for i in geo_targets]
                    )
                    # remove items from undefined set
                    drop_indexes = []
                    for code in valid_zips:
                        for n, i in enumerate(numeric_values):
                            if i.startswith(code):
                                drop_indexes.append(n)
                    undefined = [i for n, i in enumerate(undefined)
                                 if n not in drop_indexes]

        return {"result": result, "undefined": undefined}
    # pylint: enable=too-many-nested-blocks

    @staticmethod
    def get_geo_targets(names):
        geo_targets = GeoTarget.objects.filter(
            name__in=names
        ).values("id", "name", "canonical_name")
        return geo_targets
