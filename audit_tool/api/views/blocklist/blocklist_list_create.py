from distutils.util import strtobool

from rest_framework.exceptions import ValidationError
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response

from .filter_backend import BlocklistESFilterBackend
from audit_tool.utils.get_blocklist_serializer_context import get_context
from audit_tool.api.serializers.blocklist_serializer import BlocklistSerializer
from audit_tool.models import BlacklistItem
from audit_tool.models import get_hash_name
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from userprofile.constants import StaticPermissions
from utils.api_paginator import CustomPageNumberPaginator
from utils.db.functions import safe_bulk_create
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.views import validate_max_page
from utils.datetime import now_in_default_tz


class BlocklistPaginator(CustomPageNumberPaginator):
    page_size = 20

    def _get_response_data(self, data):
        response_data = super()._get_response_data(data)
        try:
            response_data["max_page"] = int(min((10000 / self.page_size), response_data["max_page"]))
        except (TypeError, ZeroDivisionError):
            pass
        return response_data

    def get_page_size(self, request):
        self.page_size = super().get_page_size(request)
        return self.page_size


class BlocklistListCreateAPIView(ListCreateAPIView):
    serializer_class = BlocklistSerializer
    filter_backends = [BlocklistESFilterBackend]
    pagination_class = BlocklistPaginator
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.BLOCKLIST_MANAGER),)
    DEFAULT_PAGE_SIZE = 25

    def get_queryset(self) -> ESQuerysetAdapter:
        """ Validate query params and instantiate queryset """
        self._validate()
        es_manager_class = self._get_es_manager(self.kwargs["data_type"])
        queryset = ESQuerysetAdapter(es_manager_class(), from_cache=False)
        return queryset

    def _validate(self):
        """ Validation logic for get request """
        page = int(self.request.query_params.get("page", 1))
        size = int(self.request.query_params.get("size", self.DEFAULT_PAGE_SIZE))
        validate_max_page(10000, size, page)

    def get_serializer_context(self) -> dict:
        """
        Retrieve blacklist_data context for creation serializer
        blacklist_data is BlacklistItem table data that is retrieved for the current page
        """
        item_ids = [item.main.id for item in self.paginator.page.object_list]
        context = get_context(item_ids)
        return context

    def create(self, request, *args, **kwargs):
        """
        Handles creating or updating new BlacklistItem objects
        counter_key is either the "blocked_count" or "unblocked_count" field on the BlacklistItem table and is
            determined by whether we should block or unblock the count
            e.g. if blocking, counter_key = "blocked_count"
        """
        try:
            should_block = bool(strtobool(request.query_params.get("block", "")))
        except ValueError:
            raise ValidationError("Please provide the query parameter 'block' as 'true' or 'false'.")
        data_type = kwargs["data_type"]
        # Determine which field BlacklistItem counter field we need to increment
        counter_key = "blocked_count" if should_block else "unblocked_count"
        item_ids = self._map_urls(request.data.get("item_urls", []), data_type=data_type)
        to_update, to_create = self._prepare_items(item_ids, data_type, counter_key, should_block)
        safe_bulk_create(BlacklistItem, to_create)
        BlacklistItem.objects.bulk_update(to_update, fields=["blocked_count", "unblocked_count",
                                                             "updated_at", "processed_by_user_id", "blocklist"])
        self._update_docs(item_ids, should_block, data_type)
        return Response()

    def _prepare_items(self, item_ids: list, data_type: str, counter_key: str, should_block: bool):
        """
        Prepare BlacklistItem objects for creation or updating
            Only update values if blocklist value changes
        :param item_ids: list
        :param data_type: video | channel. Will be used to set the item_type field on BlacklistItem
            0 = video, 1 = channel
        :param counter_key: blocked_count | unblocked_count
        :return: tuple of items to create and update
        """
        now = now_in_default_tz()
        exists = BlacklistItem.objects.filter(item_id__in=item_ids)
        exists_ids = set(exists.values_list("item_id", flat=True))

        to_update = []
        for item in exists:
            # Only update blocklist item if blocklist value is changing
            if item.blocklist != should_block:
                item.blocklist = not item.blocklist
                item.processed_by_user_id = self.request.user.id
                item.updated_at = now
                value = getattr(item, counter_key) + 1
                setattr(item, counter_key, value)
                to_update.append(item)
                
        item_type = 1 if data_type == "channel" else 0
        to_create = []
        if should_block:
            to_create = [
                BlacklistItem(
                    item_type=item_type, item_id=item_id, item_id_hash=get_hash_name(item_id), updated_at=now,
                    processed_by_user_id=self.request.user.id, **{counter_key: 1}
                ) for item_id in item_ids if item_id not in exists_ids
            ]
        return to_update, to_create

    def _map_urls(self, urls: list, data_type: str) -> list:
        """
        Map urls into ids
        :param urls: list of channel or video ids
        :param data_type: video or channel
        :return: new list of item ids
        """
        separator = "/channel/" if data_type == "channel" else "?v="
        mapped_ids = []
        if isinstance(urls, str):
            urls = [urls]
        for url in urls:
            mapped = url
            if "youtube" in url:
                mapped = url.split(separator)[-1].strip("/")
            if self._validate_url(mapped, data_type):
                mapped_ids.append(mapped)
        return mapped_ids

    def _validate_url(self, url, data_type):
        url = str(url)
        valid = True
        if data_type == "channel" and len(url) != 24:
            valid = False
        elif data_type == "video" and len(url) != 11:
            valid = False
        return valid

    def _update_docs(self, item_ids: list, should_block: bool, data_type: str) -> None:
        """
        Set brand_safety.rescore values to True for rescoring after changing blocklist values
        Sets channel videos blocklist = True if blocklisting
        :param item_ids: list of Youtube channel or video ids
        :param should_block: bool whether to blocklist or not
        :return: None
        """
        upsert_sections = (Sections.BRAND_SAFETY, Sections.CUSTOM_PROPERTIES)
        es_manager = self._get_es_manager(data_type)(upsert_sections=upsert_sections)
        # Only set overall_score to 0 if adding to blocklist
        overall_score = {"overall_score": 0} if should_block else {}
        bs_data = {
            "rescore": True if should_block is False else False,
            **overall_score
        }
        docs = [es_manager.model(item_id, brand_safety=bs_data,
                                 custom_properties={"blocklist": should_block}) for item_id in item_ids]
        es_manager.upsert(docs, refresh=False)
        if data_type == "channel" and should_block is True:
            script = "ctx._source.brand_safety.overall_score = 0"
            video_manager = VideoManager(upsert_sections=upsert_sections)
            query = video_manager.ids_query(item_ids, id_field=f"{Sections.CHANNEL}.id") \
                    & QueryBuilder().build().must().exists().field(Sections.BRAND_SAFETY).get()
            video_manager.update(query).script(source=script, lang="painless")\
                .params(conflicts="proceed", wait_for_completion=False).execute()

    def _get_es_manager(self, doc_type: str):
        managers = dict(
            video=VideoManager,
            channel=ChannelManager,
        )
        return managers[doc_type]