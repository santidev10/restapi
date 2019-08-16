import logging

from rest_framework.generics import RetrieveAPIView

from es_components.constants import Sections
from es_components.managers import KeywordManager
from keywords.api.serializers.keyword_with_views_history import KeywordWithViewsHistorySerializer
from utils.api.research import ESRetrieveAdapter
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class KeywordRetrieveUpdateApiView(RetrieveAPIView):
    serializer_class = KeywordWithViewsHistorySerializer
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
            .id(keyword).fields().get_data()
