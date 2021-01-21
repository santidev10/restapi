import csv
from datetime import datetime
from io import StringIO

from django.http import StreamingHttpResponse

from userprofile.models import UserDeviceToken
from .targeting_list_base import TargetingListBaseAPIClass
from userprofile.constants import StaticPermissions


class AdGroupCreationTargetingExportApiView(TargetingListBaseAPIClass):
    permission_classes = (StaticPermissions()(StaticPermissions.MANAGED_SERVICE__EXPORT),)

    def get_user(self):
        auth_token = self.request.query_params.get("auth_token")
        token = UserDeviceToken.objects.get(key=auth_token)
        return token.user

    def get_data(self):
        queryset = self.get_queryset()
        sub_list_type = self.kwargs["sub_list_type"]
        queryset = queryset.filter(is_negative=sub_list_type == "negative")
        data = self.get_serializer(queryset, many=True).data
        self.add_items_info(data)
        return data

    def get(self, request, pk, list_type, sub_list_type, **_):
        data = self.get_data()

        def generator():
            def to_line(line):
                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(line)
                return output.getvalue()

            fields = ["criteria", "name"]
            yield to_line(fields)
            for item in data:
                yield to_line(tuple(item.get(f) for f in fields))

        response = StreamingHttpResponse(generator(), content_type="text/csv")
        filename = "targeting_list_{}_{}_{}_{}.csv".format(
            datetime.now().strftime("%Y%m%d"), pk, list_type, sub_list_type
        )
        response["Content-Disposition"] = "attachment; filename=" \
                                          "\"{}\"".format(filename)
        return response
