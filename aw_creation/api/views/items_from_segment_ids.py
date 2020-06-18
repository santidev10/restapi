from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from segment.models import CustomSegment
from utils.permissions import MediaBuyingAddOnPermission


class ItemsFromSegmentIdsApiView(APIView):
    permission_classes = (MediaBuyingAddOnPermission,)

    def post(self, request, segment_type, **_):
        all_related_items = []
        ids = request.data
        for segment_id in ids:
            segment = CustomSegment.objects.get(id=segment_id)
            scan = segment.generate_search_with_params().scan()
            related_ids = [
                {
                    "criteria": item.main.id,
                    "id": item.main.id,
                    "name": item.general_data.title,
                    "thumnail": item.general_data.thumbnail_image_url
                } for item in scan
            ]
            all_related_items.extend(related_ids)
        return Response(status=HTTP_200_OK, data=all_related_items)
