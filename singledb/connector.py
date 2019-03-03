"""
Single database API connector module
"""
import json
from urllib.parse import urlencode, quote

import requests
from django.conf import settings
from django.http import Http404
from rest_framework.status import HTTP_404_NOT_FOUND

from singledb.settings import DEFAULT_VIDEO_DETAILS_FIELDS, \
    DEFAULT_VIDEO_LIST_FIELDS, DEFAULT_CHANNEL_LIST_FIELDS, \
    DEFAULT_CHANNEL_DETAILS_FIELDS, DEFAULT_KEYWORD_DETAILS_FIELDS, \
    DEFAULT_KEYWORD_LIST_FIELDS
from singledb.settings import DEFAULT_VIDEO_DETAILS_SOURCES, \
    DEFAULT_VIDEO_LIST_SOURCES, DEFAULT_CHANNEL_LIST_SOURCES, \
    DEFAULT_CHANNEL_DETAILS_SOURCES, DEFAULT_KEYWORD_DETAILS_SOURCES, \
    DEFAULT_KEYWORD_LIST_SOURCES


class SingleDatabaseApiConnectorException(Exception):
    """
    Exception class for single database api connector
    """

    def __init__(self, *args, **kwargs):
        sdb_response = kwargs.pop("sdb_response", None)
        super(SingleDatabaseApiConnectorException, self) \
            .__init__(*args, **kwargs)
        self.sdb_response = sdb_response


class SingleDatabaseApiConnector(object):
    """
    Connector class for IQ api
    """
    single_database_api_url = settings.SINGLE_DATABASE_API_URL
    index_info = False
    actual_index = None

    def execute_get_call(self, *args, **kwargs):
        return self.execute_call(requests.get, *args, **kwargs)

    def execute_put_call(self, *args, **kwargs):
        return self.execute_call(requests.put, *args, **kwargs)

    def execute_post_call(self, *args, **kwargs):
        return self.execute_call(requests.post, *args, **kwargs)

    def execute_delete_call(self, *args, **kwargs):
        return self.execute_call(requests.delete, *args, **kwargs)

    def execute_call(self, method, endpoint, query_params, data=None):
        """
        Make GET call to api
        """
        if hasattr(query_params, "_mutable"):
            query_params._mutable = True
        if self.index_info:
            query_params["index_info"] = 1
        else:
            query_params.pop("index_info", None)
        if self.actual_index:
            query_params["actual_index"] = self.actual_index
        else:
            query_params.pop("actual_index", None)
        # prepare header
        headers = {"Content-Type": "application/json"}
        # prepare query params
        params = "?{}".format(urlencode(query_params, doseq=True))
        # build url
        url = "{}{}{}".format(self.single_database_api_url, endpoint, params)
        # execute call
        try:
            if data is None:
                self.response = method(url, headers=headers, verify=False)
            else:
                self.response = method(url, headers=headers, verify=False,
                                       data=json.dumps(data))
        except Exception as e:
            raise SingleDatabaseApiConnectorException(
                "Unable to reach API. Original exception: {}".format(e))
        else:
            if self.response.status_code == HTTP_404_NOT_FOUND:
                raise Http404(self.response.text)
            if self.response.status_code > 300:
                raise SingleDatabaseApiConnectorException(
                    "Error during iq api call: {}".format(self.response.text),
                    sdb_response=self.response
                )
        try:
            response_data = self.response.json()
        except Exception as e:
            raise SingleDatabaseApiConnectorException(
                "Unable to parse api response: {}\n{}" \
                    .format(e, self.response.text))
        return response_data

    def get_channel(self, query_params, pk):
        """
        Obtain channel
        :param query_params: dict
        """
        endpoint = "channels/" + pk
        self.set_fields_query_param(
            query_params, DEFAULT_CHANNEL_DETAILS_FIELDS)
        self.set_sources_query_param(
            query_params, DEFAULT_CHANNEL_DETAILS_SOURCES)
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def put_channel(self, query_params, pk, data):
        """
        Update channel
        :param query_params: dict
        """
        endpoint = "channels/" + pk + "/"
        self.set_fields_query_param(query_params,
                                    DEFAULT_CHANNEL_DETAILS_FIELDS)
        self.set_sources_query_param(query_params,
                                     DEFAULT_CHANNEL_DETAILS_SOURCES)
        response_data = self.execute_put_call(endpoint, query_params, data)
        return response_data

    def get_channel_list(self, query_params, ignore_sources=False):
        """
        Obtain channel list
        :param query_params: dict
        :param ignore_sources: bool
        """
        endpoint = "channels/"
        self.set_fields_query_param(query_params, DEFAULT_CHANNEL_LIST_FIELDS)
        if not ignore_sources:
            self.set_sources_query_param(query_params, DEFAULT_CHANNEL_LIST_SOURCES)
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def _get_items_list_full(self, endpoint, sort_filed, filters, fields, batch_size):
        """
        Obtain full list by batches.
        :param fields: list of fields to retrieve
        :param filters: dict
        :param batch_size:
        :returns generator for channels by sized batches
        """
        fields = (fields or []) + [sort_filed]
        params = {
            "sort": sort_filed,
            "size": batch_size,
            "fields": ",".join(fields),
            **filters
        }
        last_id = None
        has_more = True
        count = 0
        while has_more:
            params[sort_filed + "__range"] = "{},".format(last_id or "")
            start_from = 0 if last_id is None else 1
            response_data = self.execute_get_call(endpoint, params)
            items = response_data.get("items")[start_from:]
            count += len(items)
            if len(items) > 0:
                yield items
                last_id = items[-1][sort_filed]
            else:
                has_more = False

    def get_channel_list_full(self, filters, fields=None, batch_size=5000):
        """
        Obtain full channel list by batches.
        :param fields: list of fields to retrieve
        :param filters: dict
        :param batch_size:
        :returns generator for channels by sized batches
        """
        return self._get_items_list_full("channels/", "channel_id", filters, fields, batch_size)

    def get_top_channel_keywords(self, query_params):
        """
        Get top keywords for popular channels
        :param query_params: dict
        """
        endpoint = "channels/top_keywords/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def delete_channels(self, query_params, data):
        """
        Delete channels
        :param query_params: dict
        """
        endpoint = "channel_set/"
        response_data = self.execute_delete_call(endpoint, query_params, data)
        return response_data

    def delete_channel_test(self, pk: str):
        """
        Delete channel
        :param pk: str Channel ID
        """
        endpoint = "channels_test/" + pk
        response_data = self.execute_delete_call(endpoint, query_params=dict())
        return response_data

    def get_video(self, query_params, pk):
        """
        Obtain video
        :param query_params: dict
        """
        endpoint = "videos/" + pk
        self.set_fields_query_param(
            query_params, DEFAULT_VIDEO_DETAILS_FIELDS)
        self.set_sources_query_param(
            query_params, DEFAULT_VIDEO_DETAILS_SOURCES)
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def put_video(self, query_params, pk, data):
        """
        Update video
        :param query_params: dict
        """
        endpoint = "videos/" + pk + "/"
        response_data = self.execute_put_call(endpoint, query_params, data)
        return response_data

    def get_video_list(self, query_params, ignore_sources=False):
        """
        Obtain video list
        :param query_params: dict
        :param ignore_sources: bool
        """
        endpoint = "videos/"
        self.set_fields_query_param(
            query_params, DEFAULT_VIDEO_LIST_FIELDS)
        if not ignore_sources:
            self.set_sources_query_param(
                query_params, DEFAULT_VIDEO_LIST_SOURCES)
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def get_video_list_full(self, filters, fields=None, batch_size=5000):
        """
        Obtain full video list by batches.
        :param fields: list of fields to retrieve
        :param filters: dict
        :param batch_size:
        :returns generator for videos by sized batches
        """
        return self._get_items_list_full("videos/", "video_id", filters, fields, batch_size)

    def delete_videos(self, query_params, data):
        """
        Delete videos
        :param query_params: dict
        """
        endpoint = "video_set/"
        response_data = self.execute_delete_call(endpoint, query_params, data)
        return response_data

    def store_ids(self, ids):
        """
        Wrap requested ids into hash
        """
        endpoint = "cached_objects/"
        response_data = self.execute_post_call(endpoint, {}, data=ids)
        return response_data['hash']

    @staticmethod
    def set_fields_query_param(query_params, default_fields):
        """
        Add fields query param to query params if absent
        """
        if "fields" not in query_params:
            query_params._mutable = True
            query_params["fields"] = ",".join(default_fields)
        return query_params

    @staticmethod
    def set_sources_query_param(query_params, default_sources):
        """
        Add sources query param to query params if absent
        """
        if "sources" not in query_params:
            try:
                query_params._mutable = True
            except AttributeError:
                pass
            query_params["sources"] = ",".join(default_sources)
        return query_params

    def get_highlights_channels(self, query_params):
        endpoint = "channels/"
        max_page = query_params.pop("max_page")
        self.set_fields_query_param(query_params, DEFAULT_CHANNEL_LIST_FIELDS)
        response_data = self.execute_get_call(endpoint, query_params)
        response_max_page = response_data.get("max_page", None)
        if response_max_page:
            response_data["max_page"] = max_page if response_max_page > max_page else response_max_page
        return response_data

    def get_highlights_videos(self, query_params):
        endpoint = "videos/"
        max_page = query_params.pop("max_page")
        self.set_fields_query_param(query_params, DEFAULT_VIDEO_LIST_FIELDS)
        response_data = self.execute_get_call(endpoint, query_params)
        response_max_page = response_data.get("max_page", None)
        if response_max_page:
            response_data["max_page"] = max_page if response_max_page > max_page else response_max_page
        return response_data

    def get_highlights_keywords(self, query_params):
        endpoint = "keywords/"
        max_page = query_params.pop("max_page")
        self.set_fields_query_param(query_params, DEFAULT_KEYWORD_LIST_FIELDS)
        response_data = self.execute_get_call(endpoint, query_params)
        response_max_page = response_data.get("max_page", None)
        if response_max_page:
            response_data["max_page"] = max_page if response_max_page > max_page else response_max_page
        return response_data

    def get_channels_base_info(self, ids):
        fields = ("channel_id", "title", "thumbnail_image_url")
        ids_hash = self.store_ids(ids)
        query_params = dict(fields=",".join(fields), size=len(ids),
                            ids_hash=ids_hash, sources=DEFAULT_CHANNEL_LIST_SOURCES)
        response_data = self.get_channel_list(query_params)
        items = response_data["items"]
        for i in items:
            i["id"] = i["channel_id"]
            del i["channel_id"]
        return items

    def get_videos_base_info(self, ids):
        fields = ("video_id", "title", "thumbnail_image_url", "duration")
        ids_hash = self.store_ids(ids)
        query_params = dict(fields=",".join(fields), size=len(ids),
                            ids_hash=ids_hash, sources=DEFAULT_VIDEO_LIST_SOURCES)
        response_data = self.get_video_list(query_params)
        items = response_data["items"]
        for i in items:
            i["id"] = i["video_id"]
            del i["video_id"]
        return items

    def auth_channel(self, data):
        """
        Authenticate channel
        :param query_params: dict
        """
        endpoint = "channels/authentication/"
        response_data = self.execute_post_call(endpoint, {}, data)
        return response_data

    def post_channels(self, channels_ids):
        """
        Create channels
        :param channels_ids: list of ids
        """
        endpoint = "channels/"
        response_data = self.execute_post_call(
            endpoint, {}, {"channels_ids": channels_ids})
        return response_data

    def get_keyword(self, query_params, pk):
        """
        Obtain keywords
        :param query_params: dict
        :param pk: str
        """
        endpoint = "keywords/" + quote(pk) + "/"
        self.set_fields_query_param(
            query_params, DEFAULT_KEYWORD_DETAILS_FIELDS)
        self.set_sources_query_param(
            query_params, DEFAULT_KEYWORD_DETAILS_SOURCES)
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def get_keyword_list(self, query_params):
        """
        Obtain keywords list
        :param query_params: dict
        """
        endpoint = "keywords/"
        self.set_fields_query_param(
            query_params, DEFAULT_KEYWORD_LIST_FIELDS)
        self.set_sources_query_param(
            query_params, DEFAULT_KEYWORD_LIST_SOURCES)
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def unauthorize_channel(self, channel_id):
        """
        Remove access token for the channel
        """
        endpoint = "channels/" + channel_id + "/unauthorize"
        return self.execute_put_call(endpoint, {})

    def get_bad_words_list(self, query_params):
        endpoint = "bad_words/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def get_bad_words_categories_list(self, query_params):
        endpoint = "bad_words_categories/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def get_bad_words_history_list(self, query_params):
        endpoint = "bad_words_history/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def post_bad_word(self, query_params, data):
        endpoint = "bad_words/"
        response_data = self.execute_post_call(endpoint, {}, data)
        return response_data

    def put_bad_word(self, query_params, pk, data):
        endpoint = "bad_words/" + pk + "/"
        response_data = self.execute_put_call(endpoint, query_params, data)
        return response_data

    def get_bad_word(self, query_params, pk):
        endpoint = "bad_words/" + pk + "/"
        response_data = self.execute_get_call(endpoint, query_params)
        return response_data

    def delete_bad_word(self, query_params, pk, data):
        endpoint = "bad_words/" + pk + "/"
        response_data = self.execute_delete_call(endpoint, query_params, data)
        return response_data
