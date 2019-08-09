from django.http import Http404
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView

from keywords.api.utils import get_keywords_aw_stats
from keywords.api.utils import get_keywords_aw_top_bottom_stats
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class KeywordAWStatsApiView(APIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.keyword_details"),
            user_has_permission("userprofile.settings_my_yt_channels"),
            user_has_permission("userprofile.view_highlights"),
            IsAdminUser
        ),
    )

    def get(self, request, pk):
        keyword = pk
        aw_stats = get_aw_stats([keyword])[0]
        if aw_stats is None:
            raise Http404
        return Response(aw_stats)


def get_aw_stats(keywords):
    from aw_reporting.models import dict_norm_base_stats, dict_add_calculated_stats

    stats = get_keywords_aw_stats(keywords)
    top_bottom_stats = get_keywords_aw_top_bottom_stats(keywords)

    for keyword in keywords:
        item_stats = stats.get(keyword)
        if item_stats:
            dict_norm_base_stats(item_stats)
            dict_add_calculated_stats(item_stats)
            del item_stats['keyword']

            item_top_bottom_stats = top_bottom_stats.get(keyword, {})
            item_stats.update(**item_top_bottom_stats)
    return [stats.get(keyword) for keyword in keywords]
