from utils.permissions import IsAuthQueryTokenPermission
from .audience_tool_list import AudienceToolListApiView
from .topic_tool_list_export import TopicToolListExportApiView


class AudienceToolListExportApiView(TopicToolListExportApiView):
    permission_classes = (IsAuthQueryTokenPermission,)
    export_fields = ("id", "name", "parent_id", "type")
    file_name = "audience_list"

    def get_queryset(self):
        return AudienceToolListApiView.queryset.all()
