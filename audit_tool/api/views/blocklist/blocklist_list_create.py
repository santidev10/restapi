from distutils.util import strtobool

from django.contrib.auth import get_user_model
from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response

from .filter_backend import BlocklistESFilterBackend
from audit_tool.api.serializers.blocklist_serializer import BlocklistSerializer
from audit_tool.models import BlacklistItem
from audit_tool.models import get_hash_name
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from utils.api_paginator import CustomPageNumberPaginator
from utils.db.functions import safe_bulk_create
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.views import validate_max_page
from utils.datetime import now_in_default_tz


class BlocklistPaginator(CustomPageNumberPaginator):
    page_size = 20


class BlocklistListCreateAPIView(ListCreateAPIView):
    serializer_class = BlocklistSerializer
    filter_backends = [BlocklistESFilterBackend]
    pagination_class = BlocklistPaginator
    permission_classes = (IsAdminUser,)
    DEFAULT_PAGE_SIZE = 25

    def get_queryset(self) -> ESQuerysetAdapter:
        """ Validate query params and instantiate queryset """
        self._validate()
        es_manager_class = self._get_es_manager(self.kwargs["data_type"])
        queryset = ESQuerysetAdapter(es_manager_class())
        return queryset

    def _validate(self):
        """ Validation logic for get request """
        page = self.request.query_params.get("page", 1)
        size = self.request.query_params.get("size", self.DEFAULT_PAGE_SIZE)
        validate_max_page(10000, size, page)

    def get_serializer_context(self) -> dict:
        """
        Retrieve blacklist_data context for creation serializer
        blacklist_data is BlacklistItem table data that is retrieved for the current page
        """
        item_ids = [item.main.id for item in self.paginator.page.object_list]
        blacklist_qs = BlacklistItem.objects.filter(item_id__in=item_ids)
        email_by_user_id = {
            user.id: user.email for user
            in get_user_model().objects.filter(id__in=list(blacklist_qs.values_list("processed_by_user_id", flat=True)))
        }
        blacklist_data = {}
        for item in blacklist_qs:
            try:
                setattr(item, "email", email_by_user_id[item.processed_by_user_id])
            except KeyError:
                pass
            finally:
                blacklist_data[item.item_id] = item
        context = {"blacklist_data": blacklist_data}
        return context

    def create(self, request, *args, **kwargs):
        """
        Handles creating or updating new BlacklistItem objects
        counter_key is either the "blocked_count" or "unblocked_count" field on the BlacklistItem table and is
            determined by whether we should block or unblock the count
            e.g. if blocking, counter_key = "blocked_count"
        """
        # Determine which field counter we need to increment
        counter_key = "blocked_count" if strtobool(request.query_params.get("block", "")) is 1 else "unblocked_count"
        item_ids = self._map_urls(request.data.get("item_urls", []), data_type=kwargs["data_type"])
        to_update, to_create = self._prepare_items(item_ids, kwargs["data_type"], counter_key)
        safe_bulk_create(BlacklistItem, to_create)
        BlacklistItem.objects.bulk_update(to_update, fields=["blocked_count", "unblocked_count"])
        if counter_key == "blocked_count":
            self._update_blocklist(item_ids, True)
        else:
            # Rescore items if unblocking
            self._update_rescore(item_ids)
        return Response()

    def _prepare_items(self, item_ids: list, data_type: str, counter_key: str):
        """
        Prepare BlacklistItem objects for creation or updating
        :param item_ids: list
        :param data_type: video | channel. Will be used to set the item_type field on BlacklistItem
            0 = video, 1 = channel
        :param counter_key: blocked_count | unblocked_count
        :return: tuple of items to create and update
        """
        now = now_in_default_tz()
        data_type = 1 if data_type == "channel" else 0
        to_update = BlacklistItem.objects.filter(item_id__in=item_ids)
        exists_ids = set(to_update.values_list("item_id", flat=True))
        for item in to_update:
            item.processed_by_user_id = self.request.user.id
            item.updated_at = now
            value = getattr(item, counter_key) + 1
            setattr(item, counter_key, value)
        to_create = [
            BlacklistItem(
                item_type=data_type, item_id=item_id, item_id_hash=get_hash_name(item_id), updated_at=now,
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
            mapped_ids.append(mapped)
        return mapped_ids

    def _update_rescore(self, item_ids: list) -> None:
        """
        Set brand_safety.rescore values to True for unblocked items
        :param item_ids: list of Youtube channel or video ids
        :return: None
        """
        es_manager_class = self._get_es_manager(self.kwargs["data_type"])
        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value(item_ids).get()
        es_manager_class(upsert_sections=[Sections.BRAND_SAFETY]).update_rescore(query, rescore=True)

    def _update_blocklist(self, item_ids, block):
        es_manager = self._get_es_manager(self.kwargs["data_type"])(upsert_sections=[Sections.CUSTOM_PROPERTIES])
        es_manager.update(es_manager.ids_query(item_ids))\
            .script(source=f"ctx._source.custom_properties.blocklist={str(block).lower()}")\
            .execute()

    def _get_es_manager(self, doc_type):
        managers = dict(
            video=VideoManager,
            channel=ChannelManager,
        )
        return managers[doc_type]