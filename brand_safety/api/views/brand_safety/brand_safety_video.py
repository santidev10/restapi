from collections import defaultdict

from django.conf import settings
from django.http import Http404
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_502_BAD_GATEWAY
from rest_framework.views import APIView

from brand_safety.utils import get_es_data
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
import brand_safety.constants as constants
from utils.brand_safety_view_decorator import get_brand_safety_label
from utils.elasticsearch import ElasticSearchConnectorException
from audit_tool.models import BlacklistItem

class BrandSafetyVideoAPIView(APIView):
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )
    MAX_SIZE = 10000

    def get(self, request, **kwargs):
        """
        View to retrieve individual video brand safety data
        """
        video_id = kwargs["pk"]
        category_mapping = BadWordCategory.get_category_mapping()
        video_es_data = get_es_data(video_id, settings.BRAND_SAFETY_VIDEO_INDEX)
        if isinstance(video_es_data, ElasticSearchConnectorException):
            return Response(status=HTTP_502_BAD_GATEWAY, data=constants.UNAVAILABLE_MESSAGE)
        if not video_es_data:
            raise Http404
        video_score = video_es_data["overall_score"]
        video_brand_safety_data = {
            "score": video_score,
            "label": get_brand_safety_label(video_score),
            "total_unique_flagged_words": 0,
            "category_flagged_words": defaultdict(set),
        }
        # Map category ids to category names and aggregate all keywords for each category
        all_keywords = set()
        for category_id, data in video_es_data["categories"].items():
            if str(category_id) in BadWordCategory.EXCLUDED:
                continue
            # Handle category ids that may have been removed
            try:
                category_name = category_mapping[category_id]
            except KeyError:
                continue
            keywords = [word["keyword"] for word in data["keywords"]]
            all_keywords.update(keywords)
            video_brand_safety_data["total_unique_flagged_words"] += len(keywords)
            video_brand_safety_data["category_flagged_words"][category_name].update(keywords)
        worst_words = BadWord.objects.filter(name__in=all_keywords).order_by("-negative_score")[:3]
        video_brand_safety_data["worst_words"] = [word.name for word in worst_words]
        #video_brand_safety_data["blacklist_data"] = BlacklistItem.get(video_id, BlacklistItem.VIDEO_ITEM, to_dict=True)
        return Response(status=HTTP_200_OK, data=video_brand_safety_data)
