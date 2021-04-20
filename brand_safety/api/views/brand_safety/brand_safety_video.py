from collections import defaultdict
from functools import reduce
import operator

from django.db.models import Q
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


class BrandSafetyVideoAPIView(APIView):
    permission_required = (StaticPermissions.has_perms(StaticPermissions.RESEARCH),)
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

        # Prepare response data to update
        brand_safety_data = get_brand_safety_data(brand_safety_section.overall_score)
        video_brand_safety_data = {
            "score": brand_safety_data["score"],
            "label": brand_safety_data["label"],
            "total_unique_flagged_words": 0,
            "category_flagged_words": defaultdict(set),
        }

        categories = brand_safety_section.categories.to_dict()
        keywords = set()
        # Get all keywords to check they have not been deleted
        for category_id, data in categories.items():
            category_id = str(category_id)
            if category_id in BadWordCategory.EXCLUDED:
                continue
            # Handle category ids that may have been removed
            try:
                category_mapping[category_id]
            except KeyError:
                continue
            # Prepare tuples of word and category id to query as there may be duplicate words
            keywords.update((word["keyword"], category_id) for word in data["keywords"])
        exists = set()
        worst_words = set()
        try:
            # Check if words have been deleted
            query = reduce(
                operator.or_,
                (Q(name=name, category_id=category_id) for name, category_id in keywords)
            )
            # Add to worst words and exists to filter out deleted words
            for word in BadWord.objects.filter(query).order_by("-negative_score"):
                if len(worst_words) < 3 and word.name not in worst_words:
                    worst_words.add(word.name)
                exists.add((word.name, str(word.category_id)))
        except TypeError:
            # Raises if reduce is given empty iterable
            pass
        video_brand_safety_data["worst_words"] = list(worst_words)
        # Map category ids to category names and aggregate all keywords for each category for existing words
        for word, category_id in keywords:
            if (word, category_id) in exists:
                video_brand_safety_data["total_unique_flagged_words"] += 1
                video_brand_safety_data["category_flagged_words"][category_name].add(word)
        return Response(status=HTTP_200_OK, data=video_brand_safety_data)
