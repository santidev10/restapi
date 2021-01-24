from collections import defaultdict

from django.http import Http404
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from brand_safety.auditors.utils import AuditUtils
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from userprofile.constants import StaticPermissions
from utils.brand_safety import get_brand_safety_data
from utils.permissions import has_static_permission


class BrandSafetyVideoAPIView(APIView):
    permission_required = (has_static_permission(StaticPermissions.RESEARCH),)
    video_manager = VideoManager(sections=Sections.BRAND_SAFETY)
    MAX_SIZE = 10000

    def get(self, request, **kwargs):
        """
        Retrieve individual video brand safety data
        """
        video_id = kwargs["pk"]
        category_mapping = BadWordCategory.get_category_mapping()
        try:
            video_data = AuditUtils.get_items([video_id], self.video_manager)[0]
            brand_safety_section = video_data.brand_safety
        except (IndexError, AttributeError):
            raise Http404
        brand_safety_data = get_brand_safety_data(brand_safety_section.overall_score)
        video_brand_safety_data = {
            "score": brand_safety_data["score"],
            "label": brand_safety_data["label"],
            "total_unique_flagged_words": 0,
            "category_flagged_words": defaultdict(set),
        }
        # Map category ids to category names and aggregate all keywords for each category
        all_keywords = set()
        categories = brand_safety_section.categories.to_dict()
        for category_id, data in categories.items():
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
        return Response(status=HTTP_200_OK, data=video_brand_safety_data)
