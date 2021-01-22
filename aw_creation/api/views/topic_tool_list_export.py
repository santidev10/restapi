import csv
from datetime import datetime
from io import StringIO

from django.http import StreamingHttpResponse

from userprofile.constants import StaticPermissions
from .topic_tool_list import TopicToolListApiView


class TopicToolListExportApiView(TopicToolListApiView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MANAGED_SERVICE__EXPORT),)
    export_fields = ("id", "name", "parent_id")
    file_name = "topic_list"

    def get(self, request, *args, **kwargs):
        queryset = self.get_queryset()

        export_ids = request.GET.get("export_ids")
        if export_ids:
            export_ids = set(int(i) for i in export_ids.split(","))
        fields = self.export_fields

        def generator():
            def to_line(line):
                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(line)
                return output.getvalue()

            def item_with_children(item):
                if not export_ids or item.id in export_ids:
                    yield to_line(tuple(getattr(item, f) for f in fields))

                for i in item.children.all():
                    if not export_ids or i.id in export_ids:
                        yield to_line(tuple(getattr(i, f) for f in fields))

            yield to_line(fields)
            for topic in queryset:
                yield from item_with_children(topic)

        response = StreamingHttpResponse(
            generator(), content_type="text/csv")
        filename = "{}_{}.csv".format(
            self.file_name,
            datetime.now().strftime("%Y%m%d"),
        )
        response["Content-Disposition"] = "attachment; filename=" \
                                          "\"{}\"".format(filename)
        return response
