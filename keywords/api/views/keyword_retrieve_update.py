import logging

from es_components.constants import Sections
from es_components.managers import KeywordManager
from keywords.api.views import add_aw_stats
from keywords.api.views import add_views_history_chart
from utils.api.research import ESRetrieveAdapter
from utils.api.research import ESRetrieveApiView
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class KeywordRetrieveUpdateApiView(ESRetrieveApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.keyword_details"),
            OnlyAdminUserCanCreateUpdateDelete
        ),
    )

    def get_object(self):
        keyword = self.kwargs.get("pk")
        logging.info("keyword id {}".format(keyword))
        sections = (Sections.MAIN, Sections.STATS,)

        return ESRetrieveAdapter(KeywordManager(sections)) \
            .id(keyword).fields().extra_fields_func((add_aw_stats, add_views_history_chart,)) \
            .get_data()
